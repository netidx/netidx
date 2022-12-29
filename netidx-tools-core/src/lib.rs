use netidx::{config::Config, resolver_client::DesiredAuth};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct ClientParams {
    #[structopt(short = "c", long = "config", help = "path to the client config")]
    pub config: Option<String>,
    #[structopt(short = "a", long = "auth", help = "auth mechanism")]
    pub auth: Option<DesiredAuth>,
    #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
    pub upn: Option<String>,
    #[structopt(long = "spn", help = "kerberos spn, only if auth = krb5")]
    pub spn: Option<String>,
    #[structopt(
        long = "identity",
        help = "the tls identity to publish as, default_identity if omitted"
    )]
    pub identity: Option<String>,
}

impl ClientParams {
    pub fn load(self) -> (Config, DesiredAuth) {
        let cfg = match self.config {
            None => Config::load_default().expect("failed to load default netidx config"),
            Some(path) => Config::load(path).expect("failed to load netidx config"),
        };
        let auth = match self.auth.unwrap_or_else(|| cfg.default_auth()) {
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
