use gluon::{
    base::types::{AppVec, ArcType, Type},
    import, new_vm,
    vm::{
        self,
        impl_getable_simple,
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
use netidx::subscriber::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct GluVal(pub(super) Value);

impl VmType for GluVal {
    type Type = Self;

    fn make_type(thread: &Thread) -> ArcType {
        thread
            .find_type_info("netidx.GluVal")
            .expect("couldn't find type")
            .clone()
            .into_type()
    }
}

impl<'vm, 'value> api::Pushable<'vm> for GluVal {
    fn push(self, context: &mut ActiveThread<'vm>) -> vm::Result<()> {
        api::ser::Ser(self).push(context)
    }
}

impl<'vm, 'value> api::Getable<'vm, 'value> for GluVal {
    impl_getable_simple!();

    fn from_value(thread: &'vm Thread, value: vm::Variants<'value>) -> Self {
        api::de::De::from_value(thread, value).0
    }
}

pub(super) fn load_builtins(thread: &Thread) -> Result<()> {
    thread.load_script("netidx", &api::typ::make_source::<GluVal>(&thread)?)
}

pub(super) fn new() -> RootedThread {
    let vm = new_vm();
    load_builtins(&*vm).expect("failed to load built in netidx type");
    vm
}
