#[macro_use] extern crate serde_derive;
#[macro_use] extern crate anyhow;
#[macro_use] extern crate pin_utils;

pub mod chars;
pub mod pack;
pub mod pool;
pub mod utils;
pub mod path;

#[cfg(test)]
mod test;

// CR alee: TODO stuff into feature architect-schemars
use schemars::gen::SchemaGenerator;
use schemars::schema::*;
use schemars::JsonSchema;
use path::Path;
use chars::Chars;
use pool::{Poolable, Pooled};

macro_rules! forward_impl {
    (($($impl:tt)+) => $target:ty) => {
        impl $($impl)+ {
            fn is_referenceable() -> bool {
                <$target>::is_referenceable()
            }

            fn schema_name() -> String {
                <$target>::schema_name()
            }

            fn json_schema(gen: &mut SchemaGenerator) -> Schema {
                <$target>::json_schema(gen)
            }

            fn _schemars_private_non_optional_json_schema(gen: &mut SchemaGenerator) -> Schema {
                <$target>::_schemars_private_non_optional_json_schema(gen)
            }

            fn _schemars_private_is_option() -> bool {
                <$target>::_schemars_private_is_option()
            }
        }
    };
    ($ty:ty => $target:ty) => {
        forward_impl!((JsonSchema for $ty) => $target);
    };
}

forward_impl!((JsonSchema for Path) => String);
forward_impl!((JsonSchema for Chars) => String);
forward_impl!((<T> JsonSchema for Pooled<T> where T: Poolable + Sync + Send + JsonSchema + 'static) => T);