use super::super::BSCtx;
use glib::{prelude::*, subclass::prelude::*};
use gtk::{self, prelude::*};
use sourceview4::{
    prelude::*, subclass::prelude::*, CompletionItem, CompletionProposal,
    CompletionProvider, CompletionActivation, CompletionContext, CompletionInfo
};

glib::wrapper! {
    pub(crate) struct BScriptCompletionProvider(ObjectSubclass<imp::BScriptCompletionProvider>)
        @implements CompletionProvider;
}

impl BScriptCompletionProvider {
    pub(crate) fn new() -> Self {
        glib::Object::new(&[]).expect("failed to create BScriptCompletionProvider")
    }
}

mod imp {
    use super::*;

    #[derive(Default)]
    pub(super) struct BScriptCompletionProvider;

    #[glib::object_subclass]
    impl ObjectSubclass for BScriptCompletionProvider {
        const NAME: &'static str = "BScriptCompletionProvider";

        type Type = super::BScriptCompletionProvider;

        type ParentType = glib::Object;

        type Interfaces = (CompletionProvider,);
    }

    impl ObjectImpl for BScriptCompletionProvider {}

    impl CompletionProviderImpl for BScriptCompletionProvider {
        fn activate_proposal(
            &self,
            _proposal: &impl IsA<CompletionProposal>,
            _iter: &gtk::TextIter,
        ) -> bool {
            dbg!("activate_proposal");
            false
        }

        fn activation(&self) -> CompletionActivation {
            dbg!("activation");
            CompletionActivation::USER_REQUESTED
        }

        fn gicon(&self) -> Option<gio::Icon> {
            dbg!("gicon");
            None
        }

        fn icon(&self) -> Option<gdk_pixbuf::Pixbuf> {
            dbg!("icon");
            None
        }

        fn icon_name(&self) -> Option<glib::GString> {
            dbg!("icon_name");
            None
        }

        fn info_widget(
            &self,
            _proposal: &impl IsA<CompletionProposal>,
        ) -> Option<gtk::Widget> {
            dbg!("info_widget");
            None
        }

        fn interactive_delay(&self) -> i32 {
            dbg!("interactive_delay");
            100
        }

        fn name(&self) -> Option<glib::GString> {
            dbg!("name");
            Some("bscript".into())
        }

        fn priority(&self) -> i32 {
            dbg!("priority");
            1
        }

        fn start_iter(
            &self,
            _context: &impl IsA<CompletionContext>,
            _proposal: &impl IsA<CompletionProposal>,
        ) -> Option<gtk::TextIter> {
            dbg!("start_iter");
            None
        }

        fn match_(&self, _context: &impl IsA<CompletionContext>) -> bool {
            dbg!("match_");
            true
        }

        fn populate(&self, provider: &impl IsA<CompletionProvider>, context: &impl IsA<CompletionContext>) {
            dbg!("populate");
            let p = CompletionItem::builder().text("foobarbaz").label("foobarbaz").build();
            context.add_proposals(provider, &[p.upcast()], true)
        }

        fn update_info(
            &self,
            _proposal: &impl IsA<CompletionProposal>,
            _info: &impl IsA<CompletionInfo>,
        ) {
            dbg!("update_info");
        }
    }
}
