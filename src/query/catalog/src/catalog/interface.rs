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

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReply;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReply;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReply;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReply;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GcDroppedTableResp;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableByIdReq;
use databend_common_meta_app::schema::UndropTableReply;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReply;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateVirtualColumnReply;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MetaId;
use dyn_clone::DynClone;

use crate::database::Database;
use crate::table::Table;
use crate::table_args::TableArgs;
use crate::table_function::TableFunction;

#[derive(Default, Clone)]
pub struct StorageDescription {
    pub engine_name: String,
    pub comment: String,
    pub support_cluster_key: bool,
}

pub trait CatalogCreator: Send + Sync + Debug {
    fn try_create(&self, info: &CatalogInfo) -> Result<Arc<dyn Catalog>>;
}

#[async_trait::async_trait]
pub trait Catalog: DynClone + Send + Sync + Debug {
    /// Catalog itself

    // Get the name of the catalog.
    fn name(&self) -> String;
    // Get the info of the catalog.
    fn info(&self) -> CatalogInfo;

    /// Database.

    // Get the database by name.
    async fn get_database(&self, tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>>;

    // Get all the databases.
    async fn list_databases(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>>;

    // Operation with database.
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply>;

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply>;

    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply>;

    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply>;

    async fn drop_index(&self, req: DropIndexReq) -> Result<DropIndexReply>;

    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply>;

    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply>;

    async fn list_indexes(&self, req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>>;

    async fn list_index_ids_by_table_id(&self, req: ListIndexesByIdReq) -> Result<Vec<u64>>;

    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>>;

    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply>;

    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply>;

    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply>;

    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>>;

    #[async_backtrace::framed]
    async fn exists_database(&self, tenant: &Tenant, db_name: &str) -> Result<bool> {
        match self.get_database(tenant, db_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UNKNOWN_DATABASE {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply>;

    /// Table.

    // Build a `Arc<dyn Table>` from `TableInfo`.
    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>>;

    // Get the table meta by meta id.
    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)>;

    // Get the table name by meta id.
    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<String>;

    // List the tables name by meta ids.
    async fn mget_table_names_by_ids(
        &self,
        tenant: &Tenant,
        table_ids: &[MetaId],
    ) -> databend_common_exception::Result<Vec<Option<String>>>;

    // Mget the db name by meta id.
    async fn get_db_name_by_id(&self, db_ids: MetaId) -> databend_common_exception::Result<String>;

    // Mget the dbs name by meta ids.
    async fn mget_database_names_by_ids(
        &self,
        tenant: &Tenant,
        db_ids: &[MetaId],
    ) -> databend_common_exception::Result<Vec<Option<String>>>;

    // Get one table by db and table name.
    async fn get_table(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>>;

    async fn list_tables(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>>;
    async fn list_tables_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>>;

    async fn get_drop_table_infos(
        &self,
        _req: ListDroppedTableReq,
    ) -> Result<(Vec<Arc<dyn Table>>, Vec<DroppedId>)> {
        Err(ErrorCode::Unimplemented(
            "'get_drop_table_infos' not implemented",
        ))
    }

    async fn gc_drop_tables(&self, _req: GcDroppedTableReq) -> Result<GcDroppedTableResp> {
        Err(ErrorCode::Unimplemented("'gc_drop_tables' not implemented"))
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply>;

    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply>;

    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply>;

    async fn undrop_table_by_id(&self, _req: UndropTableByIdReq) -> Result<UndropTableReply> {
        unimplemented!("TODO")
    }

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply>;

    // Check a db.table is exists or not.
    #[async_backtrace::framed]
    async fn exists_table(&self, tenant: &Tenant, db_name: &str, table_name: &str) -> Result<bool> {
        match self.get_table(tenant, db_name, table_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UNKNOWN_TABLE {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn upsert_table_option(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply>;

    async fn update_table_meta(
        &self,
        table_info: &TableInfo,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply>;

    async fn update_multi_table_meta(&self, _req: UpdateMultiTableMetaReq) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "'update_multi_table_meta' not implemented",
        ))
    }

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply>;

    async fn create_table_index(&self, req: CreateTableIndexReq) -> Result<CreateTableIndexReply>;

    async fn drop_table_index(&self, req: DropTableIndexReq) -> Result<DropTableIndexReply>;

    async fn get_table_copied_file_info(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply>;

    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply>;

    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>>;

    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply>;

    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()>;

    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()>;

    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>>;

    /// Table function

    // Get function by name.
    fn get_table_function(
        &self,
        _func_name: &str,
        _tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        Err(ErrorCode::Unimplemented(
            "'get_table_function' not implemented",
        ))
    }

    fn exists_table_function(&self, _func_name: &str) -> bool {
        false
    }

    // List all table functions' names.
    fn list_table_functions(&self) -> Vec<String> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any;

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }

    fn get_stream_source_table(&self, _stream_desc: &str) -> Result<Option<Arc<dyn Table>>> {
        Err(ErrorCode::Unimplemented(
            "'get_stream_source_table' not implemented",
        ))
    }

    fn cache_stream_source_table(&self, _stream: TableInfo, _source: TableInfo) {
        unimplemented!()
    }

    async fn create_sequence(&self, req: CreateSequenceReq) -> Result<CreateSequenceReply>;
    async fn get_sequence(&self, req: GetSequenceReq) -> Result<GetSequenceReply>;

    async fn get_sequence_next_value(
        &self,
        req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply>;

    async fn drop_sequence(&self, req: DropSequenceReq) -> Result<DropSequenceReply>;
}
