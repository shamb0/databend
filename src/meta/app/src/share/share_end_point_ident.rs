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

pub type ShareEndpointIdent = TIdent<Resource>;

pub type ShareEndpointIdentRaw = TIdentRaw<Resource>;

pub use kvapi_impl::Resource;

impl ShareEndpointIdent {
    pub fn share_endpoint_name(&self) -> &str {
        self.name()
    }
}

impl ShareEndpointIdentRaw {
    pub fn share_endpoint_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::share::ShareEndpointId;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_share_endpoint";
        const TYPE: &'static str = "ShareEndpointIdent";
        type ValueType = ShareEndpointId;
    }

    impl kvapi::Value for ShareEndpointId {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            [self.to_string_key()]
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::ShareEndpointIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = ShareEndpointIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_share_endpoint/test/test1");

        assert_eq!(ident, ShareEndpointIdent::from_str_key(&key).unwrap());
    }
}
