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

use std::fmt::Display;
use std::ops::Deref;

use chrono::DateTime;
use chrono::Utc;

use crate::schema::CatalogNameIdent;
use crate::storage::StorageParams;
use crate::tenant::Tenant;
use crate::KeyWithTenant;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum CatalogType {
    Default = 1,
    Hive = 2,
    Iceberg = 3,
}

impl Display for CatalogType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogType::Default => write!(f, "DEFAULT"),
            CatalogType::Hive => write!(f, "HIVE"),
            CatalogType::Iceberg => write!(f, "ICEBERG"),
        }
    }
}

/// different options for creating catalogs
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum CatalogOption {
    /// The default catalog.
    ///
    /// It's not allowed to create a new default catalog.
    Default,
    // Catalog option for hive.
    Hive(HiveCatalogOption),
    // Catalog option for Iceberg.
    Iceberg(IcebergCatalogOption),
}

impl CatalogOption {
    pub fn catalog_type(&self) -> CatalogType {
        match self {
            CatalogOption::Default => CatalogType::Default,
            CatalogOption::Hive(_) => CatalogType::Hive,
            CatalogOption::Iceberg(_) => CatalogType::Iceberg,
        }
    }
}

/// Option for creating a iceberg catalog
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HiveCatalogOption {
    pub address: String,
    pub storage_params: Option<Box<StorageParams>>,
}

/// Option for creating a iceberg catalog
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IcebergCatalogOption {
    pub storage_params: Box<StorageParams>,
}

/// Same as `CatalogNameIdent`, but with `serde` support,
/// and can be used a s part of a value.
// #[derive(Clone, Debug, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CatalogName {
    pub tenant: String,
    pub catalog_name: String,
}

impl From<CatalogNameIdent> for CatalogName {
    fn from(ident: CatalogNameIdent) -> Self {
        CatalogName {
            tenant: ident.tenant_name().to_string(),
            catalog_name: ident.name().to_string(),
        }
    }
}

impl Display for CatalogName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.tenant, self.catalog_name)
    }
}

// serde is required by `DataSourcePlan.catalog_info`
// serde is required by `CommitSink.catalog_info`
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CatalogInfo {
    pub id: catalog_info::CatalogId,
    pub name_ident: CatalogName,
    pub meta: CatalogMeta,
}

/// Private types for `CatalogInfo`.
mod catalog_info {

    /// Same as [`crate::schema::CatalogId`], except with serde support, and can be used in a value,
    /// while CatalogId is only used for Key.
    ///
    /// This type is sealed in a private mod so that it is pub for use but can not be created directly.
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
    pub struct CatalogId {
        pub catalog_id: u64,
    }

    impl From<crate::schema::CatalogId> for CatalogId {
        fn from(value: crate::schema::CatalogId) -> Self {
            Self {
                catalog_id: value.catalog_id,
            }
        }
    }
}

impl CatalogInfo {
    /// Get the catalog type via catalog info.
    pub fn catalog_type(&self) -> CatalogType {
        self.meta.catalog_option.catalog_type()
    }

    /// Get the catalog name via catalog info.
    pub fn catalog_name(&self) -> &str {
        &self.name_ident.catalog_name
    }

    /// Create a new default catalog info.
    pub fn new_default() -> CatalogInfo {
        Self {
            id: CatalogId { catalog_id: 0 }.into(),
            name_ident: CatalogName {
                // tenant for default catalog is not used.
                tenant: "".to_string(),
                catalog_name: "default".to_string(),
            },
            meta: CatalogMeta {
                catalog_option: CatalogOption::Default,
                created_on: Default::default(),
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CatalogMeta {
    pub catalog_option: CatalogOption,
    pub created_on: DateTime<Utc>,
}

// serde is required by `CatalogInfo.id`
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogId {
    pub catalog_id: u64,
}

impl CatalogId {
    pub fn new(catalog_id: u64) -> CatalogId {
        CatalogId { catalog_id }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogIdToName {
    pub catalog_id: u64,
}

impl Display for CatalogIdToName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.catalog_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateCatalogReq {
    pub if_not_exists: bool,
    pub name_ident: CatalogNameIdent,
    pub meta: CatalogMeta,
}

impl CreateCatalogReq {
    pub fn tenant(&self) -> &str {
        self.name_ident.tenant_name()
    }
    pub fn catalog_name(&self) -> &str {
        self.name_ident.name()
    }
}

impl Display for CreateCatalogReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_catalog(if_not_exists={}):{}/{}={:?}",
            self.if_not_exists,
            self.name_ident.tenant_name(),
            self.name_ident.name(),
            self.meta
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateCatalogReply {
    pub catalog_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropCatalogReq {
    pub if_exists: bool,
    pub name_ident: CatalogNameIdent,
}

impl Display for DropCatalogReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drop_catalog(if_exists={}):{}/{}",
            self.if_exists,
            self.name_ident.tenant_name(),
            self.name_ident.name()
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropCatalogReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetCatalogReq {
    pub inner: CatalogNameIdent,
}

impl Deref for GetCatalogReq {
    type Target = CatalogNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GetCatalogReq {
    pub fn new(ident: CatalogNameIdent) -> GetCatalogReq {
        GetCatalogReq { inner: ident }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListCatalogReq {
    pub tenant: Tenant,
}

impl ListCatalogReq {
    pub fn new(tenant: Tenant) -> ListCatalogReq {
        ListCatalogReq { tenant }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyBuilder;
    use databend_common_meta_kvapi::kvapi::KeyError;
    use databend_common_meta_kvapi::kvapi::KeyParser;

    use super::CatalogId;
    use super::CatalogIdToName;
    use crate::schema::CatalogMeta;
    use crate::schema::CatalogNameIdent;

    impl kvapi::KeyCodec for CatalogId {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.catalog_id)
        }

        fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError> {
            let catalog_id = parser.next_u64()?;

            Ok(Self { catalog_id })
        }
    }

    /// "__fd_catalog_by_id/<catalog_id>"
    impl kvapi::Key for CatalogId {
        const PREFIX: &'static str = "__fd_catalog_by_id";

        type ValueType = CatalogMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::KeyCodec for CatalogIdToName {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.catalog_id)
        }

        fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError> {
            let catalog_id = parser.next_u64()?;

            Ok(Self { catalog_id })
        }
    }

    /// "__fd_catalog_id_to_name/<catalog_id> -> CatalogNameIdent"
    impl kvapi::Key for CatalogIdToName {
        const PREFIX: &'static str = "__fd_catalog_id_to_name";

        type ValueType = CatalogNameIdent;

        fn parent(&self) -> Option<String> {
            Some(CatalogId::new(self.catalog_id).to_string_key())
        }
    }

    impl kvapi::Value for CatalogMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for CatalogNameIdent {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
