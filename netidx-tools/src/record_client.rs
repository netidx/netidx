use anyhow::Result;
use netidx::path::Path;
use netidx_tools_core::ClientParams;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub(crate) struct OneshotParams {
    #[structopt(name = "base", help = "the base path of the recorder instance")]
    base: Path,
    #[structopt(name = "start", help = "the time to start the recording at")]
    start: Option<String>,
    #[structopt(name = "end", help = "the time to end the recording at")]
    end: Option<String>,
    #[structopt(name = "filter", help = "glob pattern(s) to include")]
    filter: Vec<String>,
}

#[derive(StructOpt, Debug)]
pub(crate) enum Cmd {
    #[structopt(name = "oneshot", about = "get a oneshot recording")]
    Oneshot {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: OneshotParams,
    },
}

pub(super) async fn run(cmd: Cmd) -> Result<()> {
    unimplemented!()
}
