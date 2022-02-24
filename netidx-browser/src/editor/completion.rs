use super::Scope;
use glib::{prelude::*, subclass::prelude::*};
use netidx::path::Path;
use radix_trie::TrieCommon;
use sourceview4_sc::{
    prelude::*, subclass::prelude::*, CompletionActivation, CompletionContext,
    CompletionInfo, CompletionItem, CompletionProposal, CompletionProvider,
};
use std::{default::Default, rc::Rc};

glib::wrapper! {
    pub(crate) struct BScriptCompletionProvider(ObjectSubclass<imp::BScriptCompletionProvider>)
        @implements CompletionProvider;
}

impl BScriptCompletionProvider {
    pub(crate) fn new() -> Self {
        glib::Object::new(&[]).expect("failed to create BScriptCompletionProvider")
    }
}

pub(crate) mod imp {
    use std::cell::RefCell;

    use crate::BSCtx;

    use super::*;

    struct BScriptCompletionProviderInner {
        ctx: BSCtx,
        scope: Scope,
    }

    pub(crate) struct BScriptCompletionProvider(
        Rc<RefCell<Option<BScriptCompletionProviderInner>>>,
    );

    impl BScriptCompletionProvider {
        pub(crate) fn init(&self, ctx: BSCtx, scope: Scope) {
            *self.0.borrow_mut() = Some(BScriptCompletionProviderInner { ctx, scope });
        }
    }

    impl Default for BScriptCompletionProvider {
        fn default() -> Self {
            BScriptCompletionProvider(Rc::new(RefCell::new(None)))
        }
    }

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
            false
        }

        fn activation(&self) -> CompletionActivation {
            CompletionActivation::USER_REQUESTED
        }

        fn gicon(&self) -> Option<gio::Icon> {
            None
        }

        fn icon(&self) -> Option<gdk_pixbuf::Pixbuf> {
            None
        }

        fn icon_name(&self) -> Option<glib::GString> {
            None
        }

        fn info_widget(
            &self,
            _proposal: &impl IsA<CompletionProposal>,
        ) -> Option<gtk::Widget> {
            None
        }

        fn interactive_delay(&self) -> i32 {
            100
        }

        fn name(&self) -> Option<glib::GString> {
            Some("bscript".into())
        }

        fn priority(&self) -> i32 {
            1
        }

        fn start_iter(
            &self,
            _context: &impl IsA<CompletionContext>,
            _proposal: &impl IsA<CompletionProposal>,
        ) -> Option<gtk::TextIter> {
            None
        }

        fn match_(&self, _context: &impl IsA<CompletionContext>) -> bool {
            true
        }

        fn populate(
            &self,
            provider: &impl IsA<CompletionProvider>,
            context: &impl IsA<CompletionContext>,
        ) {
            macro_rules! get {
                ($e:expr) => {
                    match $e {
                        None => return,
                        Some(e) => e,
                    }
                };
            }
            let inner = self.0.borrow();
            let inner = get!(&*inner);
            let ctx = inner.ctx.borrow();
            let word = {
                let mut iter = get!(context.iter());
                let fin = iter.clone();
                let coff = iter.line_offset();
                let mut start = iter.clone();
                start.backward_chars(coff);
                let mut i = 0;
                iter.backward_find_char(
                    |c| {
                        let r = i >= coff
                            || c.is_ascii_whitespace()
                            || (c != '_' && c.is_ascii_punctuation());
                        i += 1;
                        r
                    },
                    Some(&start),
                );
                let wc = iter.char().unwrap_or('a');
                if (wc.is_ascii_punctuation() || wc.is_ascii_whitespace())
                    && iter.offset() < fin.offset()
                {
                    iter.forward_char();
                }
                iter.text(&fin)
            };
            let word = word.as_ref().map(|s| &**s).unwrap_or("");
            let fn_candidates = ctx
                .user
                .fns
                .get_raw_descendant(word)
                .into_iter()
                .map(|st| st.iter())
                .flatten()
                .map(|(c, ())| {
                    let l = format!("fn {}(..)", c);
                    CompletionItem::builder().text(c).label(&l).build().upcast()
                });
            let scope = inner.scope.borrow();
            let var_candidates = ctx
                .user
                .vars
                .get_raw_descendant(word)
                .into_iter()
                .map(|st| st.iter())
                .flatten()
                .filter(|(_, scopes)| {
                    scopes.get(&**scope).is_some()
                        || scopes.get_ancestor(&**scope).is_some()
                        || scopes
                            .get_raw_descendant(&**scope)
                            .into_iter()
                            .map(|st| st.iter())
                            .flatten()
                            .any(|(s, ())| {
                                let s = s.trim_start_matches(&**scope);
                                Path::parts(s).all(|p| p.starts_with("do"))
                            })
                })
                .map(|(c, _)| {
                    let l = format!("var {}", c);
                    CompletionItem::builder().text(c).label(&l).build().upcast()
                });
            let candidates = fn_candidates.chain(var_candidates).collect::<Vec<_>>();
            context.add_proposals(provider, &*candidates, true);
        }

        fn update_info(
            &self,
            _proposal: &impl IsA<CompletionProposal>,
            _info: &impl IsA<CompletionInfo>,
        ) {
        }
    }
}
