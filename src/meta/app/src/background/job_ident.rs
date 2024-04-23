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

/// Defines the meta-service key for background job.
pub type BackgroundJobIdent = TIdent<Resource>;

pub use kvapi_impl::Resource;

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::background::BackgroundJobId;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_background_job";
        const TYPE: &'static str = "BackgroundJobIdent";
        type ValueType = BackgroundJobId;
    }

    impl kvapi::Value for BackgroundJobId {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            [self.to_string_key()]
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    //     fn from(err: ExistError<Resource>) -> Self {
    //         ErrorCode::ConnectionAlreadyExists(err.to_string())
    //     }
    // }
    //
    // impl From<UnknownError<Resource>> for ErrorCode {
    //     fn from(err: UnknownError<Resource>) -> Self {
    //         // Special case: use customized message to keep backward compatibility.
    //         // TODO: consider using the default message in the future(`err.to_string()`)
    //         ErrorCode::UnknownConnection(format!("Connection '{}' does not exist.", err.name()))
    //             .add_message_back(err.ctx())
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::BackgroundJobIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_job_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = BackgroundJobIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_background_job/test/test1");

        assert_eq!(ident, BackgroundJobIdent::from_str_key(&key).unwrap());
    }
}
