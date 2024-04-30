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

use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;

use crate::tenant::Tenant;
use crate::tenant::ToTenant;
use crate::tenant_key::raw::TIdentRaw;
use crate::tenant_key::resource::TenantResource;
use crate::KeyWithTenant;

/// `[T]enant[Ident]` is a common meta-service key structure in form of `<PREFIX>/<TENANT>/<NAME>`.
pub struct TIdent<R, N = String> {
    tenant: Tenant,
    name: N,
    _p: std::marker::PhantomData<R>,
}

/// `TIdent` to be Debug does not require `R` to be Debug.
impl<R, N> Debug for TIdent<R, N>
where
    R: TenantResource,
    N: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        // If there is a specified type name for this alias, use it.
        // Otherwise use the default name
        let type_name = if R::TYPE.is_empty() {
            "TIdent"
        } else {
            R::TYPE
        };

        f.debug_struct(type_name)
            .field("tenant", &self.tenant)
            .field("name", &self.name)
            .finish()
    }
}

/// `TIdent` to be Clone does not require `R` to be Clone.
impl<R, N> Clone for TIdent<R, N>
where N: Clone
{
    fn clone(&self) -> Self {
        Self {
            tenant: self.tenant.clone(),
            name: self.name.clone(),
            _p: Default::default(),
        }
    }
}

/// `TIdent` to be PartialEq does not require `R` to be PartialEq.
impl<R, N> PartialEq for TIdent<R, N>
where N: PartialEq
{
    fn eq(&self, other: &Self) -> bool {
        self.tenant == other.tenant && self.name == other.name
    }
}

impl<R, N> Eq for TIdent<R, N> where N: PartialEq {}

impl<R, N> Hash for TIdent<R, N>
where N: Hash
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.tenant, state);
        Hash::hash(&self.name, state);
        Hash::hash(&self._p, state)
    }
}

impl<R> TIdent<R, String> {
    pub fn new(tenant: impl ToTenant, name: impl ToString) -> Self {
        Self::new_generic(tenant, name.to_string())
    }
}

impl<R> TIdent<R, u64> {
    pub fn new(tenant: impl ToTenant, name: u64) -> Self {
        Self::new_generic(tenant, name)
    }
}

impl<R> TIdent<R, ()> {
    pub fn new(tenant: impl ToTenant) -> Self {
        Self::new_generic(tenant, ())
    }
}

impl<R, N> TIdent<R, N> {
    pub fn new_generic(tenant: impl ToTenant, name: N) -> Self {
        Self {
            tenant: tenant.to_tenant(),
            name,
            _p: Default::default(),
        }
    }

    /// Create a new instance from TIdent of different resource definition.
    pub fn new_from<T>(other: TIdent<T, N>) -> Self {
        Self::new_generic(other.tenant, other.name)
    }

    pub fn unpack(self) -> (Tenant, N) {
        (self.tenant, self.name)
    }

    pub fn name(&self) -> &N {
        &self.name
    }

    /// Create a display-able instance.
    pub fn display(&self) -> impl fmt::Display + '_
    where N: fmt::Display {
        format!("'{}'/'{}'", self.tenant.tenant_name(), self.name)
    }
}

impl<R, N> TIdent<R, N>
where
    R: TenantResource,
    N: Clone + Debug,
{
    /// Convert to the corresponding Raw key that can be stored as value,
    /// getting rid of the embedded per-tenant config.
    pub fn to_raw(&self) -> TIdentRaw<R, N> {
        TIdentRaw::new_generic(self.tenant_name(), self.name().clone())
    }
}

mod kvapi_key_impl {
    use std::fmt::Debug;

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyCodec;
    use databend_common_meta_kvapi::kvapi::KeyError;
    use databend_common_meta_types::NonEmptyString;

    use crate::tenant::Tenant;
    use crate::tenant_key::ident::TIdent;
    use crate::tenant_key::resource::TenantResource;
    use crate::KeyWithTenant;

    impl<R, N> kvapi::KeyCodec for TIdent<R, N>
    where
        R: TenantResource,
        N: KeyCodec,
    {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            let b = if R::HAS_TENANT {
                b.push_str(self.tenant_name())
            } else {
                b
            };
            self.name.encode_key(b)
        }

        fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, KeyError> {
            let tenant_name = if R::HAS_TENANT {
                p.next_nonempty()?
            } else {
                NonEmptyString::new("dummy").unwrap()
            };

            let name = N::decode_key(p)?;

            Ok(TIdent::<R, N>::new_generic(
                Tenant::new_nonempty(tenant_name),
                name,
            ))
        }
    }

    impl<R, N> kvapi::Key for TIdent<R, N>
    where
        R: TenantResource,
        N: KeyCodec,
        N: Debug,
    {
        const PREFIX: &'static str = R::PREFIX;
        type ValueType = R::ValueType;

        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }
    }

    impl<R, N> KeyWithTenant for TIdent<R, N>
    where R: TenantResource
    {
        fn tenant(&self) -> &Tenant {
            &self.tenant
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use databend_common_meta_kvapi::kvapi::Key;

    use crate::tenant::Tenant;
    use crate::tenant_key::ident::TIdent;
    use crate::tenant_key::resource::TenantResource;

    #[test]
    fn test_tenant_ident() {
        struct Foo;

        impl TenantResource for Foo {
            const PREFIX: &'static str = "foo";
            const HAS_TENANT: bool = true;
            type ValueType = Infallible;
        }

        let tenant = Tenant::new_literal("test");
        let ident = TIdent::<Foo>::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "foo/test/test1");

        assert_eq!(ident, TIdent::<Foo>::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_tenant_ident_u64() {
        struct Foo;

        impl TenantResource for Foo {
            const PREFIX: &'static str = "foo";
            const HAS_TENANT: bool = true;
            type ValueType = Infallible;
        }

        let tenant = Tenant::new_literal("test");
        let ident = TIdent::<Foo, u64>::new(tenant, 3);

        let key = ident.to_string_key();
        assert_eq!(key, "foo/test/3");

        assert_eq!(ident, TIdent::<Foo, u64>::from_str_key(&key).unwrap());
    }
}
