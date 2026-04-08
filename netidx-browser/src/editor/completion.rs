// Syntax highlighting (embedded .lang file) and context-sensitive
// code completion using the graphix compiler Env.

use arcstr::ArcStr;
use glib::subclass::prelude::*;
use graphix_compiler::{
    env::Env,
    expr::{ModPath, print::PrettyDisplay},
    typ::Type,
};
use gtk::prelude::*;
use netidx::path::Path;
use sourceview4::prelude::*;
use sourceview4::subclass::prelude::*;
use std::{
    cell::RefCell,
    rc::Rc,
};

// ---- Syntax highlighting ----

const GRAPHIX_LANG: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<language id="graphix" name="Graphix" version="2.0" _section="Source">
  <metadata>
    <property name="mimetypes">text/x-graphix</property>
    <property name="globs">*.gx</property>
    <property name="line-comment-start">//</property>
  </metadata>

  <styles>
    <style id="comment"       name="Comment"        map-to="def:comment"/>
    <style id="doc-comment"   name="Doc Comment"    map-to="def:doc-comment"/>
    <style id="string"        name="String"         map-to="def:string"/>
    <style id="escape"        name="Escape"         map-to="def:special-char"/>
    <style id="interpolation" name="Interpolation"  map-to="def:special-char"/>
    <style id="keyword"       name="Keyword"        map-to="def:keyword"/>
    <style id="boolean"       name="Boolean"        map-to="def:boolean"/>
    <style id="null"          name="Null"           map-to="def:special-constant"/>
    <style id="type"          name="Type"           map-to="def:type"/>
    <style id="type-var"      name="Type Variable"  map-to="def:type"/>
    <style id="number"        name="Number"         map-to="def:number"/>
    <style id="operator"      name="Operator"       map-to="def:operator"/>
    <style id="labeled-param" name="Labeled Param"  map-to="def:preprocessor"/>
    <style id="variant"       name="Variant"        map-to="def:constant"/>
    <style id="builtin"       name="Builtin"        map-to="def:builtin"/>
  </styles>

  <definitions>
    <define-regex id="ident">[a-zA-Z_][a-zA-Z0-9_]*</define-regex>

    <context id="doc-comment" style-ref="doc-comment" end-at-line-end="true" class="comment">
      <start>///</start>
    </context>

    <context id="line-comment" style-ref="comment" end-at-line-end="true" class="comment">
      <start>//</start>
    </context>

    <context id="escape" style-ref="escape">
      <match>\\([nrt0"\\&lt;&gt;\[\]]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\})</match>
    </context>

    <context id="interpolation" style-ref="interpolation">
      <start>\[</start>
      <end>\]</end>
      <include>
        <context ref="graphix"/>
      </include>
    </context>

    <context id="string" style-ref="string" end-at-line-end="false" class="string">
      <start>"</start>
      <end>"</end>
      <include>
        <context ref="escape"/>
        <context ref="interpolation"/>
      </include>
    </context>

    <context id="raw-string" style-ref="string" end-at-line-end="false" class="string">
      <start>r'</start>
      <end>'</end>
    </context>

    <context id="keywords" style-ref="keyword">
      <keyword>let</keyword>
      <keyword>rec</keyword>
      <keyword>mod</keyword>
      <keyword>use</keyword>
      <keyword>type</keyword>
      <keyword>fn</keyword>
      <keyword>cast</keyword>
      <keyword>select</keyword>
      <keyword>if</keyword>
      <keyword>try</keyword>
      <keyword>catch</keyword>
      <keyword>any</keyword>
      <keyword>with</keyword>
      <keyword>where</keyword>
      <keyword>throws</keyword>
      <keyword>as</keyword>
    </context>

    <context id="boolean" style-ref="boolean">
      <keyword>true</keyword>
      <keyword>false</keyword>
    </context>

    <context id="null" style-ref="null">
      <keyword>null</keyword>
      <keyword>ok</keyword>
    </context>

    <context id="types" style-ref="type">
      <keyword>bool</keyword>
      <keyword>string</keyword>
      <keyword>bytes</keyword>
      <keyword>i8</keyword>
      <keyword>u8</keyword>
      <keyword>i16</keyword>
      <keyword>u16</keyword>
      <keyword>i32</keyword>
      <keyword>u32</keyword>
      <keyword>v32</keyword>
      <keyword>z32</keyword>
      <keyword>i64</keyword>
      <keyword>u64</keyword>
      <keyword>v64</keyword>
      <keyword>z64</keyword>
      <keyword>f32</keyword>
      <keyword>f64</keyword>
      <keyword>decimal</keyword>
      <keyword>datetime</keyword>
      <keyword>duration</keyword>
      <keyword>Any</keyword>
      <keyword>Array</keyword>
      <keyword>Map</keyword>
    </context>

    <context id="type-variable" style-ref="type-var">
      <match>'[a-z][a-zA-Z0-9_]*</match>
    </context>

    <context id="variant" style-ref="variant">
      <match>`[A-Z][a-zA-Z0-9_]*</match>
    </context>

    <context id="labeled-param" style-ref="labeled-param">
      <match>#[a-zA-Z_][a-zA-Z0-9_]*</match>
    </context>

    <context id="builtin" style-ref="builtin">
      <match>\$[a-zA-Z_][a-zA-Z0-9_]*</match>
    </context>

    <context id="hex-number" style-ref="number">
      <match>\b0[xX][0-9a-fA-F_]+\b</match>
    </context>

    <context id="binary-number" style-ref="number">
      <match>\b0[bB][01_]+\b</match>
    </context>

    <context id="octal-number" style-ref="number">
      <match>\b0[oO][0-7_]+\b</match>
    </context>

    <context id="float-number" style-ref="number">
      <match>\b[0-9][0-9_]*\.[0-9][0-9_]*([eE][+-]?[0-9][0-9_]*)?\b</match>
    </context>

    <context id="integer" style-ref="number">
      <match>\b[0-9][0-9_]*\b</match>
    </context>

    <context id="operators" style-ref="operator">
      <match>&lt;-|=&gt;|-&gt;|::|==|!=|&lt;=|&gt;=|&amp;&amp;|\|\||[+\-*/%~?&amp;@!&lt;&gt;]</match>
    </context>

    <context id="graphix" class="no-spell-check">
      <include>
        <context ref="doc-comment"/>
        <context ref="line-comment"/>
        <context ref="string"/>
        <context ref="raw-string"/>
        <context ref="hex-number"/>
        <context ref="binary-number"/>
        <context ref="octal-number"/>
        <context ref="float-number"/>
        <context ref="integer"/>
        <context ref="boolean"/>
        <context ref="null"/>
        <context ref="keywords"/>
        <context ref="types"/>
        <context ref="type-variable"/>
        <context ref="variant"/>
        <context ref="labeled-param"/>
        <context ref="builtin"/>
        <context ref="operators"/>
      </include>
    </context>
  </definitions>
</language>
"##;

pub(super) fn setup_language(buf: &sourceview4::Buffer) {
    let lang_dir = dirs::cache_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
        .join("netidx-browser");
    let lang_path = lang_dir.join("graphix.lang");
    let _ = std::fs::create_dir_all(&lang_dir);
    let _ = std::fs::write(&lang_path, GRAPHIX_LANG);
    // Use the default manager so system lang definitions (def.lang etc.)
    // remain available for map-to="def:*" style references.
    if let Some(lm) = sourceview4::LanguageManager::default() {
        let mut paths: Vec<String> = lm.search_path()
            .iter().map(|p| p.to_string()).collect();
        let our_dir = lang_dir.to_str().unwrap_or("/tmp").to_string();
        if !paths.iter().any(|p| p == &our_dir) {
            paths.push(our_dir);
            let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
            lm.set_search_path(&refs);
        }
        if let Some(lang) = lm.language("graphix") {
            buf.set_language(Some(&lang));
        }
    }
}

// ---- Completion provider ----

mod imp {
    use super::*;

    #[derive(Default)]
    pub struct GxCompleteInner {
        pub(super) env: RefCell<Option<Rc<RefCell<Option<Env>>>>>,
        pub(super) info_label: once_cell::unsync::OnceCell<gtk::Label>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for GxCompleteInner {
        const NAME: &'static str = "GxComplete";
        type Type = super::GxComplete;
        type Interfaces = (sourceview4::CompletionProvider,);
    }

    impl ObjectImpl for GxCompleteInner {}

    impl CompletionProviderImpl for GxCompleteInner {
        fn name(&self) -> Option<glib::GString> {
            Some("Graphix".into())
        }

        fn activation(&self) -> sourceview4::CompletionActivation {
            sourceview4::CompletionActivation::INTERACTIVE
                | sourceview4::CompletionActivation::USER_REQUESTED
        }

        fn match_(&self, _context: &sourceview4::CompletionContext) -> bool {
            true
        }

        fn populate(&self, context: &sourceview4::CompletionContext) {
            let proposals = self.build_proposals(context);
            let obj = self.obj();
            let provider_ref: &sourceview4::CompletionProvider = obj.upcast_ref();
            context.add_proposals(provider_ref, &proposals, true);
        }

        fn info_widget(
            &self,
            _proposal: &sourceview4::CompletionProposal,
        ) -> Option<gtk::Widget> {
            let label = self.info_label.get_or_init(|| {
                let l = gtk::Label::new(None);
                l.set_xalign(0.0);
                l.set_margin(4);
                l.set_selectable(true);
                l.show();
                l
            });
            Some(label.clone().upcast())
        }

        fn update_info(
            &self,
            proposal: &sourceview4::CompletionProposal,
            _info: &sourceview4::CompletionInfo,
        ) {
            use sourceview4::prelude::CompletionProposalExt;
            if let Some(label) = self.info_label.get() {
                let text = proposal.info().unwrap_or_default();
                label.set_text(&text);
            }
        }

        fn priority(&self) -> i32 {
            1
        }
    }

    impl GxCompleteInner {
        fn build_proposals(
            &self,
            context: &sourceview4::CompletionContext,
        ) -> Vec<sourceview4::CompletionProposal> {
            let cursor = match context.iter() {
                Some(it) => it,
                None => return vec![],
            };
            let buf = match cursor.buffer() {
                Some(b) => b,
                None => return vec![],
            };
            // Current line text for word prefix extraction
            let mut line_start = cursor;
            line_start.set_line_offset(0);
            let line_text = match buf.text(&line_start, &cursor, false) {
                Some(t) => t.to_string(),
                None => return vec![],
            };
            // Multi-line lookback for finding enclosing function call
            let lookback_text = {
                let start = buf.start_iter();
                buf.text(&start, &cursor, false)
                    .map(|t| t.to_string())
                    .unwrap_or_default()
            };
            let env_rc = self.env.borrow();
            let env_rc = match env_rc.as_ref() {
                Some(rc) => rc,
                None => return vec![],
            };
            let env_ref = env_rc.borrow();
            let env = match env_ref.as_ref() {
                Some(e) => e,
                None => return vec![],
            };
            match classify_prefix(&line_text, &lookback_text) {
                Prefix::Bind(prefix) => {
                    self.complete_bind(env, &prefix)
                }
                Prefix::Variant { prefix, context } => {
                    self.complete_variant(env, &prefix, context.as_ref())
                }
                Prefix::ArgLabel { function, arg } => {
                    self.complete_arg_label(env, &function, &arg)
                }
                Prefix::StructField { prefix, context } => {
                    self.complete_struct_field(env, &prefix, &context)
                }
                Prefix::None => vec![],
            }
        }

        fn complete_bind(
            &self,
            env: &Env,
            prefix: &str,
        ) -> Vec<sourceview4::CompletionProposal> {
            let part = ModPath::from_iter(prefix.split("::"));
            let scope = ModPath::root();
            let mut proposals = Vec::new();
            // Module path basename: what the user typed after the last ::
            for m in env.lookup_matching_modules(&scope, &part) {
                let label = format!("{}", m);
                let text = format!("{}::", Path::basename(&m.0).unwrap_or(&label));
                let item = sourceview4::CompletionItem::builder()
                    .label(&label)
                    .text(&text)
                    .info("module")
                    .build();
                proposals.push(item.upcast());
            }
            for (value, id) in env.lookup_matching(&scope, &part) {
                let info = match env.by_id.get(&id) {
                    None => String::new(),
                    Some(b) => {
                        let mut desc = match &b.typ {
                            Type::Fn(ft) => {
                                let ft = ft.replace_auto_constrained();
                                ft.to_string_pretty(60).trim_end().to_string()
                            }
                            t => t.to_string_pretty(60).trim_end().to_string(),
                        };
                        if let Some(doc) = &b.doc {
                            desc.push('\n');
                            desc.push_str(doc);
                        }
                        desc
                    }
                };
                let display_name = match Path::dirname(&part.0) {
                    None => String::from(value.as_str()),
                    Some(dir) => {
                        let path = Path::from(ArcStr::from(dir)).append(&*value);
                        format!("{}", ModPath(path))
                    }
                };
                let insert_text = String::from(value.as_str());
                let mut builder = sourceview4::CompletionItem::builder()
                    .label(&display_name)
                    .text(&insert_text);
                if !info.is_empty() {
                    builder = builder.info(&info);
                }
                proposals.push(builder.build().upcast());
            }
            proposals
        }

        fn complete_variant(
            &self,
            env: &Env,
            prefix: &str,
            context: Option<&ArgContext>,
        ) -> Vec<sourceview4::CompletionProposal> {
            // If we have context, try to filter to just the expected type
            if let Some(ctx) = context {
                if let Some(label) = &ctx.arg_label {
                    if let Some(typ) = resolve_arg_type(env, &ctx.function, label) {
                        return self.variants_from_type(&typ, prefix);
                    }
                }
            }
            // Fallback: all variants from all typedefs
            let mut proposals = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for (_scope, defs) in env.typedefs.into_iter() {
                for (_name, td) in defs.into_iter() {
                    Self::collect_variants(&td.typ, prefix, &mut seen, &mut proposals);
                }
            }
            proposals
        }

        fn variants_from_type(
            &self,
            typ: &Type,
            prefix: &str,
        ) -> Vec<sourceview4::CompletionProposal> {
            let mut proposals = Vec::new();
            let mut seen = std::collections::HashSet::new();
            Self::collect_variants(typ, prefix, &mut seen, &mut proposals);
            proposals
        }

        fn collect_variants(
            typ: &Type,
            prefix: &str,
            seen: &mut std::collections::HashSet<String>,
            proposals: &mut Vec<sourceview4::CompletionProposal>,
        ) {
            match typ {
                Type::Set(variants) => {
                    for v in variants.iter() {
                        if let Type::Variant(tag, args) = v {
                            if tag.starts_with(prefix) && seen.insert(tag.to_string()) {
                                let display = if args.is_empty() {
                                    tag.to_string()
                                } else {
                                    let arg_types: Vec<String> = args.iter()
                                        .map(|t| format!("{}", t))
                                        .collect();
                                    format!("{}({})", tag, arg_types.join(", "))
                                };
                                let item = sourceview4::CompletionItem::builder()
                                    .label(&format!("`{}", display))
                                    .text(tag.as_str())
                                    .build();
                                proposals.push(item.upcast());
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        fn complete_arg_label(
            &self,
            env: &Env,
            function: &str,
            arg_prefix: &str,
        ) -> Vec<sourceview4::CompletionProposal> {
            let func_path = ModPath::from_iter(function.split("::"));
            let scope = ModPath::root();
            let mut proposals = Vec::new();
            if let Some((_, bind)) = env.lookup_bind(&scope, &func_path) {
                if let Type::Fn(ft) = &bind.typ {
                    for arg in ft.args.iter() {
                        if let Some((lbl, _)) = &arg.label {
                            if lbl.starts_with(arg_prefix) {
                                let text = format!("{}: ", lbl);
                                let info = arg.typ.to_string_pretty(60)
                                    .trim_end().to_string();
                                let item = sourceview4::CompletionItem::builder()
                                    .label(lbl.as_str())
                                    .text(&text)
                                    .info(&info)
                                    .build();
                                proposals.push(item.upcast());
                            }
                        }
                    }
                }
            }
            proposals
        }

        fn complete_struct_field(
            &self,
            env: &Env,
            prefix: &str,
            context: &ArgContext,
        ) -> Vec<sourceview4::CompletionProposal> {
            let mut proposals = Vec::new();
            let label = match &context.arg_label {
                Some(l) => l,
                None => return proposals,
            };
            let typ = match resolve_arg_type(env, &context.function, label) {
                Some(t) => t,
                None => return proposals,
            };
            if let Type::Struct(fields) = &typ {
                for (name, field_typ) in fields.iter() {
                    if name.starts_with(prefix) {
                        let text = format!("{}: ", name);
                        let info = field_typ.to_string_pretty(60)
                            .trim_end().to_string();
                        let item = sourceview4::CompletionItem::builder()
                            .label(name.as_str())
                            .text(&text)
                            .info(&info)
                            .build();
                        proposals.push(item.upcast());
                    }
                }
            }
            proposals
        }
    }
}

glib::wrapper! {
    pub struct GxComplete(ObjectSubclass<imp::GxCompleteInner>)
        @implements sourceview4::CompletionProvider;
}

impl GxComplete {
    pub(super) fn new(env: Rc<RefCell<Option<Env>>>) -> Self {
        let obj: Self = glib::Object::new();
        obj.imp().env.replace(Some(env));
        obj
    }
}

// ---- Prefix classification and context detection ----

enum Prefix {
    Bind(String),
    Variant { prefix: String, context: Option<ArgContext> },
    ArgLabel { function: String, arg: String },
    StructField { prefix: String, context: ArgContext },
    None,
}

struct ArgContext {
    function: String,
    arg_label: Option<String>,
    in_struct: bool,
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == ':'
}

/// Walk backward from end of line text to classify what we're completing.
fn classify_prefix(line: &str, lookback: &str) -> Prefix {
    let s = line.trim_end();
    if s.is_empty() {
        return Prefix::None;
    }
    let mut word_start = s.len();
    for (i, c) in s.char_indices().rev() {
        if c == '#' || c == '`' {
            word_start = i;
            break;
        }
        if !is_ident_char(c) {
            word_start = i + c.len_utf8();
            break;
        }
        if i == 0 {
            word_start = 0;
        }
    }
    let word = &s[word_start..];
    if word.is_empty() {
        return Prefix::None;
    }
    let before_word = lookback.len().saturating_sub(word.len());
    if let Some(tag) = word.strip_prefix('`') {
        let context = find_arg_context(&lookback[..before_word]);
        return Prefix::Variant { prefix: tag.to_string(), context };
    }
    if let Some(arg) = word.strip_prefix('#') {
        let context = find_arg_context(&lookback[..before_word]);
        if let Some(ref ctx) = context {
            return Prefix::ArgLabel {
                function: ctx.function.clone(),
                arg: arg.to_string(),
            };
        }
    }
    if word.len() < 2 {
        return Prefix::None;
    }
    let context = find_arg_context(&lookback[..before_word]);
    if let Some(ctx) = context {
        if ctx.in_struct {
            return Prefix::StructField { prefix: word.to_string(), context: ctx };
        }
    }
    Prefix::Bind(word.to_string())
}

/// Walk backward through text to find the enclosing function call context.
/// Returns the function name, the current labeled arg (if any), and whether
/// we're inside a struct literal.
fn find_arg_context(before: &str) -> Option<ArgContext> {
    let s = before.trim_end();
    let mut depth = 0i32;
    let mut arg_label: Option<String> = None;
    let mut in_struct = false;
    for (i, c) in s.char_indices().rev() {
        match c {
            ')' | ']' | '}' => depth += 1,
            '(' => {
                if depth == 0 {
                    let before_paren = s[..i].trim_end();
                    let func_start = before_paren.rfind(|c: char| {
                        c.is_whitespace() || c == '(' || c == '{' || c == '[' || c == ',' || c == ';'
                    }).map(|p| p + 1).unwrap_or(0);
                    let name = &before_paren[func_start..];
                    if !name.is_empty() {
                        return Some(ArgContext {
                            function: name.to_string(),
                            arg_label,
                            in_struct,
                        });
                    }
                    return None;
                }
                depth -= 1;
            }
            '{' => {
                if depth == 0 {
                    // Entering a struct literal context
                    in_struct = true;
                    depth -= 1;
                } else if depth > 0 {
                    depth -= 1;
                }
            }
            '[' => {
                if depth > 0 { depth -= 1; }
            }
            '#' if depth == 0 && arg_label.is_none() => {
                // Extract the label name (text from # to the next :)
                let rest = &s[i + 1..];
                if let Some(colon) = rest.find(':') {
                    let label = rest[..colon].trim();
                    if !label.is_empty() && label.chars().all(|c| c.is_alphanumeric() || c == '_') {
                        arg_label = Some(label.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// Resolve the expected type for a labeled argument, unwrapping ByRef,
/// nullable Set, and Ref layers.
fn resolve_arg_type(env: &Env, function: &str, arg_label: &str) -> Option<Type> {
    let func_path = ModPath::from_iter(function.split("::"));
    let (_, bind) = env.lookup_bind(&ModPath::root(), &func_path)?;
    let ft = match &bind.typ {
        Type::Fn(ft) => ft.replace_auto_constrained(),
        _ => return None,
    };
    let arg = ft.args.iter().find(|a| {
        a.label.as_ref().map(|(n, _)| n.as_str()) == Some(arg_label)
    })?;
    Some(unwrap_type(&arg.typ, env))
}

fn unwrap_type(typ: &Type, env: &Env) -> Type {
    match typ {
        Type::ByRef(inner) => unwrap_type(inner, env),
        Type::Set(variants) if super::expr_util::is_nullable_set(variants) => {
            unwrap_type(super::expr_util::non_null_type(variants), env)
        }
        Type::Ref { .. } => {
            match typ.lookup_ref(env) {
                Ok(resolved) => resolved,
                Err(_) => typ.clone(),
            }
        }
        _ => typ.clone(),
    }
}
