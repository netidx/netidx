use serde_derive::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Debug, Serialize, Deserialize, StructOpt)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[structopt(long = "listen", help = "the websocket address/port to listen on")]
    pub listen: String,
    #[serde(default)]
    #[structopt(long = "wstimeout", help = "the websocket timeout, unit: sec. example: 3.0")]
    pub wstimeout: Option<f64>,
    #[serde(default)]
    #[structopt(long = "cert", help = "path to the tls certificate")]
    pub cert: Option<String>,
    #[serde(default)]
    #[structopt(long = "key", help = "path to the private key")]
    pub key: Option<String>,
}
