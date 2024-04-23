// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use chrono::Utc;
use databend_common_exception::Result;
use databend_common_meta_api::ShareApi;
use databend_common_meta_app::share::share_name_ident::ShareNameIdent;
use databend_common_meta_app::share::AddShareAccountsReq;
use databend_common_meta_app::share::RemoveShareAccountsReq;
use databend_common_storages_share::save_share_spec;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::share::AlterShareTenantsPlan;

pub struct AlterShareTenantsInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterShareTenantsPlan,
}

impl AlterShareTenantsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterShareTenantsPlan) -> Result<Self> {
        Ok(AlterShareTenantsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterShareTenantsInterpreter {
    fn name(&self) -> &str {
        "AlterShareTenantsInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        if self.plan.is_add {
            let req = AddShareAccountsReq {
                share_name: ShareNameIdent::new(&tenant, &self.plan.share),
                if_exists: self.plan.if_exists,
                accounts: self.plan.accounts.clone(),
                share_on: Utc::now(),
            };
            let resp = meta_api.add_share_tenants(req).await?;

            save_share_spec(
                self.ctx.get_tenant().tenant_name(),
                self.ctx.get_data_operator()?.operator(),
                resp.spec_vec,
                None,
            )
            .await?;
        } else {
            let req = RemoveShareAccountsReq {
                share_name: ShareNameIdent::new(&tenant, &self.plan.share),
                if_exists: self.plan.if_exists,
                accounts: self.plan.accounts.clone(),
            };
            let resp = meta_api.remove_share_tenants(req).await?;

            save_share_spec(
                self.ctx.get_tenant().tenant_name(),
                self.ctx.get_data_operator()?.operator(),
                resp.spec_vec,
                None,
            )
            .await?;
        };

        Ok(PipelineBuildResult::create())
    }
}
