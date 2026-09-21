use anyhow::Result;
use arcstr::ArcStr;
use graphix_compiler::expr::Source;
use graphix_package::MainThreadHandle;
use graphix_rt::NoExt;
use graphix_shell::{Mode, ShellBuilder};

/// Run the Graphix TUI program `src`, built into this binary, until it exits.
pub(crate) async fn run_tui(shell: ShellBuilder<NoExt>, src: ArcStr) -> Result<()> {
    let (mt, _) = MainThreadHandle::new();
    shell.mode(Mode::Script(Source::Internal(src))).no_init(true).build()?.run(mt).await
}
