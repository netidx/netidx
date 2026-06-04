use clap::Args;
use netidx::{config::Config, resolver_client::DesiredAuth};

#[derive(Args, Debug, Clone)]
pub struct ClientParams {
    /// path to the client config
    #[arg(short, long)]
    pub config: Option<String>,
    /// auth mechanism
    #[arg(short, long)]
    pub auth: Option<DesiredAuth>,
    /// kerberos upn, only if auth = krb5
    #[arg(long)]
    pub upn: Option<String>,
    /// kerberos spn, only if auth = krb5
    #[arg(long)]
    pub spn: Option<String>,
    /// the tls identity to publish as, default_identity if omitted
    #[arg(long)]
    pub identity: Option<String>,
}

impl ClientParams {
    pub fn load(&self) -> (Config, DesiredAuth) {
        let cfg = match &self.config {
            None => Config::load_default_or_local_only()
                .expect("failed to load default netidx config"),
            Some(path) => Config::load(path).expect("failed to load netidx config"),
        };
        let auth = match self.auth.clone().unwrap_or_else(|| cfg.default_auth()) {
            auth @ (DesiredAuth::Anonymous | DesiredAuth::Local) => auth,
            DesiredAuth::Krb5 { .. } => {
                DesiredAuth::Krb5 { upn: self.upn.clone(), spn: self.spn.clone() }
            }
            DesiredAuth::Tls { .. } => {
                DesiredAuth::Tls { identity: self.identity.clone() }
            }
        };
        match &auth {
            DesiredAuth::Krb5 { .. } => (),
            DesiredAuth::Anonymous | DesiredAuth::Local | DesiredAuth::Tls { .. } => {
                if self.upn.is_some() || self.spn.is_some() {
                    panic!("upn/spn may only be specified for krb5 auth")
                }
            }
        }
        match &auth {
            DesiredAuth::Tls { .. } => (),
            DesiredAuth::Anonymous | DesiredAuth::Local | DesiredAuth::Krb5 { .. } => {
                if self.identity.is_some() {
                    panic!("identity may only be specified for tls auth")
                }
            }
        }
        (cfg, auth)
    }
}
