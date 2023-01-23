//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::ConstantFolder;
use common_expression::Expr;
use common_expression::Scalar;
use common_expression::TableSchemaRef;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use opendal::Operator;
use storages_common_index::BloomIndex;
use storages_common_index::FilterEvalResult;
use storages_common_table_meta::meta::Location;

use crate::io::BloomFilterReader;

#[async_trait::async_trait]
pub trait FuseBloomPruner {
    // returns ture, if target should NOT be pruned (false positive allowed)
    async fn should_keep(&self, index_location: &Option<Location>, index_length: u64) -> bool;
}

pub struct FuseBloomPrunerCreator {
    ctx: Arc<dyn TableContext>,

    /// indices that should be loaded from filter block
    index_columns: Vec<String>,

    /// the expression that would be evaluate
    filter_expression: Expr<String>,

    /// pre calculated digest for constant Scalar
    scalar_map: HashMap<Scalar, u64>,

    /// the data accessor
    dal: Operator,

    /// the schema of data being indexed
    data_schema: TableSchemaRef,
}

impl FuseBloomPrunerCreator {
    pub fn create(
        ctx: &Arc<dyn TableContext>,
        filter_exprs: Option<&[Expr<String>]>,
        schema: &TableSchemaRef,
        dal: Operator,
    ) -> Result<Option<Arc<dyn FuseBloomPruner + Send + Sync>>> {
        if let Some(exprs) = filter_exprs {
            if exprs.is_empty() {
                return Ok(None);
            }

            // Check if there were applicable filter conditions.
            let expr: Expr<String> = exprs
                .iter()
                .cloned()
                .reduce(|lhs, rhs| {
                    check_function(None, "and", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS).unwrap()
                })
                .unwrap();

            let (optimized_expr, _) =
                ConstantFolder::fold(&expr, ctx.try_get_function_context()?, &BUILTIN_FUNCTIONS);
            let point_query_cols = BloomIndex::find_eq_columns(&optimized_expr)?;

            tracing::debug!(
                "Bloom filter expr {:?}, optimized {:?}, point_query_cols: {:?}",
                expr.sql_display(),
                optimized_expr.sql_display(),
                point_query_cols
            );

            if !point_query_cols.is_empty() {
                // convert to filter column names
                let mut filter_block_cols = vec![];
                let mut scalar_map = HashMap::<Scalar, u64>::new();
                let func_ctx = ctx.try_get_function_context()?;
                for (col_name, scalar, ty) in point_query_cols.iter() {
                    filter_block_cols.push(BloomIndex::build_filter_column_name(col_name));
                    if !scalar_map.contains_key(scalar) {
                        let digest = BloomIndex::calculate_scalar_digest(func_ctx, scalar, ty)?;
                        scalar_map.insert(scalar.clone(), digest);
                    }
                }

                let creator = FuseBloomPrunerCreator {
                    ctx: ctx.clone(),
                    index_columns: filter_block_cols,
                    filter_expression: optimized_expr,
                    scalar_map,
                    dal,
                    data_schema: schema.clone(),
                };
                return Ok(Some(Arc::new(creator)));
            }
        }
        Ok(None)
    }

    // Check a location file is hit or not by bloom filter.
    pub async fn apply(&self, index_location: &Location, index_length: u64) -> Result<bool> {
        // load the relevant index columns
        let maybe_filter = index_location
            .read_filter(
                self.ctx.clone(),
                self.dal.clone(),
                &self.index_columns,
                index_length,
            )
            .await;

        match maybe_filter {
            Ok(filter) => Ok(BloomIndex::from_filter_block(
                self.ctx.try_get_function_context()?,
                self.data_schema.clone(),
                filter.filter_schema,
                filter.filter_block,
                index_location.1,
            )?
            .apply(self.filter_expression.clone(), &self.scalar_map)?
                != FilterEvalResult::MustFalse),
            Err(e) if e.code() == ErrorCode::DEPRECATED_INDEX_FORMAT => {
                // In case that the index is no longer supported, just return ture to indicate
                // that the block being pruned should be kept. (Although the caller of this method
                // "FilterPruner::should_keep",  will ignore any exceptions returned)
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl FuseBloomPruner for FuseBloomPrunerCreator {
    async fn should_keep(&self, index_location: &Option<Location>, index_length: u64) -> bool {
        if let Some(loc) = index_location {
            // load filter, and try pruning according to filter expression
            match self.apply(loc, index_length).await {
                Ok(v) => v,
                Err(e) => {
                    // swallow exceptions intentionally, corrupted index should not prevent execution
                    tracing::warn!("failed to apply bloom pruner, returning ture. {}", e);
                    true
                }
            }
        } else {
            true
        }
    }
}
