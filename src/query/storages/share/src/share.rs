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

use std::collections::BTreeMap;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::Result;
use databend_common_meta_app::share::ShareDatabaseSpec;
use databend_common_meta_app::share::ShareSpec;
use databend_common_meta_app::share::ShareTableInfoMap;
use databend_common_meta_app::share::ShareTableSpec;
use opendal::Operator;

const SHARE_CONFIG_PREFIX: &str = "_share_config";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareSpecVec {
    share_specs: BTreeMap<String, ext::ShareSpecExt>,
}

pub fn get_share_spec_location(tenant: &str) -> String {
    format!("{}/{}/share_specs.json", SHARE_CONFIG_PREFIX, tenant,)
}

pub fn share_table_info_location(tenant: &str, share_name: &str) -> String {
    format!(
        "{}/{}/{}_table_info.json",
        SHARE_CONFIG_PREFIX, tenant, share_name
    )
}

#[async_backtrace::framed]
pub async fn save_share_table_info(
    tenant: &str,
    operator: Operator,
    share_table_info: Vec<ShareTableInfoMap>,
) -> Result<()> {
    for (share_name, share_table_info) in share_table_info {
        let share_name = share_name.clone();
        let location = share_table_info_location(tenant, &share_name);
        match share_table_info {
            Some(table_info_map) => {
                operator
                    .write(&location, serde_json::to_vec(&table_info_map)?)
                    .await?;
            }
            None => {
                operator.delete(&location).await?;
            }
        }
    }

    Ok(())
}

#[async_backtrace::framed]
pub async fn save_share_spec(
    tenant: &str,
    operator: Operator,
    spec_vec: Option<Vec<ShareSpec>>,
    share_table_info: Option<Vec<ShareTableInfoMap>>,
) -> Result<()> {
    if let Some(share_spec) = spec_vec {
        let location = get_share_spec_location(tenant);
        let mut share_spec_vec = ShareSpecVec::default();
        for spec in share_spec {
            let share_name = spec.name.clone();
            let share_spec_ext = ext::ShareSpecExt::from_share_spec(spec, &operator);
            share_spec_vec
                .share_specs
                .insert(share_name, share_spec_ext);
        }
        operator
            .write(&location, serde_json::to_vec(&share_spec_vec)?)
            .await?;
    }

    // save share table info
    if let Some(share_table_info) = share_table_info {
        save_share_table_info(tenant, operator, share_table_info).await?
    }

    Ok(())
}

mod ext {
    use databend_common_meta_app::share::ShareGrantObjectPrivilege;
    use databend_storages_common_table_meta::table::database_storage_prefix;
    use databend_storages_common_table_meta::table::table_storage_prefix;
    use enumflags2::BitFlags;

    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
    struct WithLocation<T> {
        location: String,
        #[serde(flatten)]
        t: T,
    }

    /// An extended form of [ShareSpec], which decorates [ShareDatabaseSpec] and [ShareTableSpec]
    /// with location
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
    pub(super) struct ShareSpecExt {
        name: String,
        share_id: u64,
        version: u64,
        database: Option<WithLocation<ShareDatabaseSpec>>,
        tables: Vec<WithLocation<ShareTableSpec>>,
        tenants: Vec<String>,
        db_privileges: Option<BitFlags<ShareGrantObjectPrivilege>>,
        comment: Option<String>,
        share_on: Option<DateTime<Utc>>,
    }

    impl ShareSpecExt {
        pub fn from_share_spec(spec: ShareSpec, operator: &Operator) -> Self {
            Self {
                name: spec.name,
                share_id: spec.share_id,
                version: spec.version,
                database: spec.database.map(|db_spec| WithLocation {
                    location: shared_database_prefix(operator, db_spec.id),
                    t: db_spec,
                }),
                tables: spec
                    .tables
                    .into_iter()
                    .map(|tbl_spec| WithLocation {
                        location: shared_table_prefix(
                            operator,
                            tbl_spec.database_id,
                            tbl_spec.table_id,
                        ),
                        t: tbl_spec,
                    })
                    .collect(),
                tenants: spec.tenants,
                db_privileges: spec.db_privileges,
                comment: spec.comment.clone(),
                share_on: spec.share_on,
            }
        }
    }

    /// Returns prefix path which covers all the data of give table.
    /// something like "query-storage-bd5efc6/tnc7yee14/501248/501263/", where
    ///   - "/query-storage-bd5efc6/tnc7yee14/" is the storage prefix
    ///   - "501248/" is the database id suffixed with '/'
    ///   - "501263/" is the table id  suffixed with '/'
    fn shared_table_prefix(operator: &Operator, database_id: u64, table_id: u64) -> String {
        let operator_meta_data = operator.info();
        let storage_prefix = operator_meta_data.root();
        let table_storage_prefix = table_storage_prefix(database_id, table_id);
        // storage_prefix has suffix character '/'
        format!("{}{}/", storage_prefix, table_storage_prefix)
    }

    /// Returns prefix path which covers all the data of give database.
    /// something like "query-storage-bd5efc6/tnc7yee14/501248/", where
    ///   - "/query-storage-bd5efc6/tnc7yee14/" is the storage prefix
    ///   - "501248/" is the database id suffixed with '/'
    fn shared_database_prefix(operator: &Operator, database_id: u64) -> String {
        let operator_meta_data = operator.info();
        let storage_prefix = operator_meta_data.root();
        let database_storage_prefix = database_storage_prefix(database_id);
        // storage_prefix has suffix character '/'
        format!("{}{}/", storage_prefix, database_storage_prefix)
    }

    #[cfg(test)]
    mod tests {

        use opendal::services::Fs;

        use super::*;

        #[test]
        fn test_serialize_share_spec_ext() -> Result<()> {
            let share_spec = ShareSpec {
                name: "test_share_name".to_string(),
                version: 1,
                share_id: 1,
                database: Some(ShareDatabaseSpec {
                    name: "share_database".to_string(),
                    id: 1,
                }),
                tables: vec![ShareTableSpec {
                    name: "share_table".to_string(),
                    database_id: 1,
                    table_id: 1,
                    presigned_url_timeout: "100s".to_string(),
                }],
                tenants: vec!["test_tenant".to_owned()],
                comment: None,
                share_on: None,
                db_privileges: None,
            };
            let tmp_dir = tempfile::tempdir()?;
            let test_root = tmp_dir.path().join("test_cluster_id/test_tenant_id");
            let test_root_str = test_root.to_str().unwrap();
            let operator = {
                let mut builder = Fs::default();
                builder.root(test_root_str);
                Operator::new(builder)?.finish()
            };

            let share_spec_ext = ShareSpecExt::from_share_spec(share_spec, &operator);
            let spec_json_value = serde_json::to_value(share_spec_ext).unwrap();

            use serde_json::json;
            use serde_json::Value::Null;

            let expected = json!({
              "name": "test_share_name",
              "share_id": 1,
              "version": 1,
              "database": {
                "location": format!("{}/1/", test_root_str),
                "name": "share_database",
                "id": 1
              },
              "tables": [
                {
                  "location": format!("{}/1/1/", test_root_str),
                  "name": "share_table",
                  "database_id": 1,
                  "table_id": 1,
                  "presigned_url_timeout": "100s"
                }
              ],
              "tenants": [
                "test_tenant"
              ],
              "db_privileges": Null,
              "comment": Null,
              "share_on": Null
            });

            assert_eq!(expected, spec_json_value);
            Ok(())
        }
    }
}
