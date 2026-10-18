use anyhow::Result;
use arcstr::ArcStr;
use graphix_compiler::expr::Load;
use graphix_package::MainThreadHandle;
use graphix_rt::NoExt;
use graphix_shell::{Mode, ShellBuilder};

/// Run the Graphix TUI program `src`, built into this binary, until it
/// exits; a code it exits with is the process's.
pub(crate) async fn run_tui(shell: ShellBuilder<NoExt>, src: ArcStr) -> Result<()> {
    let (mt, _) = MainThreadHandle::new();
    let shell = shell.mode(Mode::Script(Load::Text(src))).no_init(true).build()?;
    if let Some(code) = shell.run(mt).await? {
        std::process::exit(code)
    }
    Ok(())
}
