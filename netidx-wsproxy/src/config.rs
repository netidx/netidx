use serde_derive::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Debug, Serialize, Deserialize, StructOpt)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[structopt(long = "bind", help = "the address to bind to")]
    pub bind: String,
    #[serde(default)]
    #[structopt(long = "cert", help = "path to the tls certificate")]
    pub cert: Option<String>,
    #[serde(default)]
    #[structopt(long = "key", help = "path to the private key")]
    pub key: Option<String>,
}
