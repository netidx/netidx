use std::{fmt, str::FromStr};

use anyhow::bail;
use clap::Args;
use serde_derive::{Deserialize, Serialize};

pub const DEFAULT_DISCONNECT_PFACTOR: DisconnectPfactor =
    DisconnectPfactor::Threshold(200.);

fn default_disconnect_pfactor() -> DisconnectPfactor {
    DEFAULT_DISCONNECT_PFACTOR
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DisconnectPfactor {
    Never,
    Threshold(f32),
}

impl FromStr for DisconnectPfactor {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "never" => Ok(Self::Never),
            s => {
                let n = usize::from_str(s)?;
                if n < 5 {
                    bail!("disconnect_pfactor may not be less than 5")
                }
                Ok(Self::Threshold(n as f32))
            }
        }
    }
}

impl fmt::Display for DisconnectPfactor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Never => f.write_str("never"),
            Self::Threshold(t) => write!(f, "{t}"),
        }
    }
}

impl DisconnectPfactor {
    pub fn disconnect(&self, pfactor: f32) -> bool {
        match self {
            Self::Never => false,
            Self::Threshold(t) => pfactor >= *t,
        }
    }
}

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
    /// disconnect clients whose queue depth is this many times larger than the
    /// mean queue depth of the other clients
    ///
    /// Lower values disconnect pathological clients sooner. Higher values allow
    /// more per-client queue growth before disconnecting. Specify "never" to never
    /// disconnect
    #[serde(default = "default_disconnect_pfactor")]
    #[arg(long, default_value_t = DEFAULT_DISCONNECT_PFACTOR)]
    pub disconnect_pfactor: DisconnectPfactor,
}
