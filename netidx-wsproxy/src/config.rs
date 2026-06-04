use clap::Args;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Args)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// the websocket address/port to listen on
    #[arg(long)]
    pub listen: String,
    /// path to the tls certificate
    #[serde(default)]
    #[arg(long)]
    pub cert: Option<String>,
    /// path to the private key
    #[serde(default)]
    #[arg(long)]
    pub key: Option<String>,
}
