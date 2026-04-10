//! Context-sensitive code completion for graphix source.
//!
//! Classifies the text before the cursor to determine what kind of
//! completion to offer (binding, argument label, variant, struct field),
//! then queries the graphix Env for matching items.

use graphix_compiler::{
    env::Env,
    expr::ModPath,
    typ::Type,
};

/// A single completion item to display.
#[derive(Debug, Clone)]
pub(crate) struct CompletionItem {
    /// The text to insert
    pub label: String,
    /// Type signature or description
    pub detail: String,
}

/// Visible state of the completion popup.
pub(crate) struct CompletionState {
    /// Whether the completion popup is visible
    pub active: bool,
    /// Current completion items
    pub items: Vec<CompletionItem>,
    /// Currently selected/highlighted index
    pub selected: usize,
    /// The byte range in the source text to replace when accepting
    pub replace_start: usize,
    pub replace_end: usize,
}

impl CompletionState {
    pub fn new() -> Self {
        Self {
            active: false,
            items: Vec::new(),
            selected: 0,
            replace_start: 0,
            replace_end: 0,
        }
    }

    pub fn dismiss(&mut self) {
        self.active = false;
        self.items.clear();
        self.selected = 0;
    }

    pub fn select_up(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
        }
    }

    pub fn select_down(&mut self) {
        if !self.items.is_empty() && self.selected + 1 < self.items.len() {
            self.selected += 1;
        }
    }
}

/// What kind of completion context we're in.
#[derive(Debug)]
enum Context {
    /// Completing a binding name or module path
    Bind { prefix: String },
    /// Completing a labeled argument (#name)
    ArgLabel { function: String, prefix: String },
}

/// Compute the cursor's byte offset in the full text from line+column.
pub(crate) fn cursor_byte_offset(text: &str, line: usize, column: usize) -> usize {
    let mut offset = 0;
    for (i, l) in text.lines().enumerate() {
        if i == line {
            return offset + column.min(l.len());
        }
        offset += l.len() + 1; // +1 for newline
    }
    text.len()
}

/// Classify the text up to the cursor to determine completion context.
fn classify(text: &str, cursor_offset: usize) -> Option<Context> {
    let before = &text[..cursor_offset];

    // Scan backward to find the token being typed
    let mut end = before.len();
    let chars: Vec<char> = before.chars().collect();
    let len = chars.len();
    if len == 0 {
        return Some(Context::Bind { prefix: String::new() });
    }

    // Check if we're typing a labeled argument (#name)
    // Scan backward to find '#'
    let mut i = len;
    while i > 0 {
        i -= 1;
        let c = chars[i];
        if c == '#' {
            // Found label start — extract the prefix after #
            let prefix: String = chars[i + 1..].iter().collect();
            // Now find the enclosing function name by scanning back past '('
            let fn_name = find_enclosing_function(&chars[..i]);
            if let Some(func) = fn_name {
                return Some(Context::ArgLabel { function: func, prefix });
            }
            break;
        }
        if !c.is_alphanumeric() && c != '_' {
            break;
        }
    }

    // Otherwise, it's a binding/module path completion
    // Walk backward to find the start of the identifier/path
    let mut start = len;
    while start > 0 {
        let c = chars[start - 1];
        if c.is_alphanumeric() || c == '_' || c == ':' {
            start -= 1;
        } else {
            break;
        }
    }
    let prefix: String = chars[start..].iter().collect();
    Some(Context::Bind { prefix })
}

/// Scan backward from position to find the function name before '('.
fn find_enclosing_function(chars: &[char]) -> Option<String> {
    let len = chars.len();
    if len == 0 {
        return None;
    }

    // Skip whitespace before '#'
    let mut i = len;
    while i > 0 && chars[i - 1].is_whitespace() {
        i -= 1;
    }

    // We might be after a comma or '(' — skip back past commas and args
    // Simple approach: find the nearest '(' scanning backward, skip past it,
    // then read the function name
    let mut depth = 0i32;
    while i > 0 {
        i -= 1;
        match chars[i] {
            ')' => depth += 1,
            '(' => {
                if depth == 0 {
                    // Found our opening paren — function name is before it
                    let mut end = i;
                    while end > 0 && chars[end - 1].is_whitespace() {
                        end -= 1;
                    }
                    let mut start = end;
                    while start > 0 && (chars[start - 1].is_alphanumeric()
                        || chars[start - 1] == '_'
                        || chars[start - 1] == ':')
                    {
                        start -= 1;
                    }
                    if start < end {
                        return Some(chars[start..end].iter().collect());
                    }
                    return None;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Query the Env for completions matching the given context.
pub(crate) fn complete(
    env: &Env,
    text: &str,
    line: usize,
    column: usize,
) -> CompletionState {
    let cursor_offset = cursor_byte_offset(text, line, column);
    let ctx = match classify(text, cursor_offset) {
        Some(c) => c,
        None => return CompletionState::new(),
    };

    let mut items = Vec::new();
    let scope = ModPath::root();

    match &ctx {
        Context::Bind { prefix } => {
            let part = ModPath::from_iter(prefix.split("::"));

            // Matching modules
            for m in env.lookup_matching_modules(&scope, &part) {
                items.push(CompletionItem {
                    label: format!("{m}"),
                    detail: "module".into(),
                });
            }

            // Matching bindings
            for (name, bind_id) in env.lookup_matching(&scope, &part) {
                let detail = match env.by_id.get(&bind_id) {
                    Some(b) => {
                        let mut s = format!("{}", b.typ);
                        if let Some(doc) = &b.doc {
                            s.push_str("  ");
                            s.push_str(doc);
                        }
                        s
                    }
                    None => String::new(),
                };
                items.push(CompletionItem {
                    label: name.to_string(),
                    detail,
                });
            }

            // Compute the replacement range (the prefix text)
            let replace_start = cursor_offset - prefix.len();
            let replace_end = cursor_offset;

            let mut state = CompletionState::new();
            state.active = !items.is_empty();
            state.items = items;
            state.replace_start = replace_start;
            state.replace_end = replace_end;
            state
        }
        Context::ArgLabel { function, prefix } => {
            let func_path = ModPath::from_iter(function.split("::"));
            if let Some((_, bind)) = env.lookup_bind(&scope, &func_path) {
                if let Type::Fn(ft) = &bind.typ {
                    for arg in ft.args.iter() {
                        if let Some((lbl, _)) = &arg.label {
                            if lbl.starts_with(prefix.as_str()) {
                                items.push(CompletionItem {
                                    label: lbl.to_string(),
                                    detail: format!("{}", arg.typ),
                                });
                            }
                        }
                    }
                }
            }

            // Replace range: the #prefix text
            let replace_start = cursor_offset - prefix.len();
            let replace_end = cursor_offset;

            let mut state = CompletionState::new();
            state.active = !items.is_empty();
            state.items = items;
            state.replace_start = replace_start;
            state.replace_end = replace_end;
            state
        }
    }
}
