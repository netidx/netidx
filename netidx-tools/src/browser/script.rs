#[macro_use]
extern crate serde_derive;

use gluon::{
    base::types::{AppVec, ArcType, Type},
    import, new_vm,
    vm::{
        self,
        api::{
            self,
            generic::{A, L, R},
            ActiveThread, FunctionRef, Getable, Hole, OpaqueRef, OpaqueValue, Pushable,
            UserdataValue, ValueRef, VmType, IO,
        },
        ExternModule, Variants,
    },
    Result, RootedThread, Thread, ThreadExt,
};

