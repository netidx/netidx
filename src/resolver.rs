pub use crate::resolver_single::{Auth, ResolverError};
use crate::resolver_single::{
    OrReferral, ResolverRead as SingleRead, ResolverWrite as SingleWrite,
};
use std::{collections::BTreeMap};

pub struct ResolverReadInner {
}
