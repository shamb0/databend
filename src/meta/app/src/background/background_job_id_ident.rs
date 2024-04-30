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

use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

pub type BackgroundJobIdIdent = TIdent<Resource, u64>;
pub type BackgroundJobIdIdentRaw = TIdentRaw<Resource, u64>;

pub use kvapi_impl::Resource;

impl BackgroundJobIdIdent {
    pub fn job_id(&self) -> u64 {
        *self.name()
    }
}

impl BackgroundJobIdIdentRaw {
    pub fn job_id(&self) -> u64 {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::background::BackgroundJobInfo;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_background_job_by_id";
        const TYPE: &'static str = "BackgroundJobIdIdent";
        const HAS_TENANT: bool = false;
        type ValueType = BackgroundJobInfo;
    }

    impl kvapi::Value for BackgroundJobInfo {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::BackgroundJobIdIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_background_job_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = BackgroundJobIdIdent::new(tenant, 3);

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_background_job_by_id/3");

        assert_eq!(ident, BackgroundJobIdIdent::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_background_job_id_ident_with_key_space() {
        // TODO(xp): implement this test
        // let tenant = Tenant::new_literal("test");
        // let ident = BackgroundJobIdIdent::new(tenant, 3);
        //
        // let key = ident.to_string_key();
        // assert_eq!(key, "__fd_background_job_by_id/3");
        //
        // assert_eq!(ident, BackgroundJobIdIdent::from_str_key(&key).unwrap());
    }
}
