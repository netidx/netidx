use netidx::{config::Config, resolver_client::DesiredAuth};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct ClientParams {
    #[structopt(short = "c", long = "config", help = "path to the client config")]
    pub config: Option<String>,
    #[structopt(
        short = "a",
        long = "auth",
        help = "auth mechanism",
        default_value = "krb5"
    )]
    pub auth: DesiredAuth,
    #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
    pub upn: Option<String>,
    #[structopt(long = "spn", help = "kerberos spn, only if auth = krb5")]
    pub spn: Option<String>,
    #[structopt(
        long = "certificate",
        help = "path to the tls certificate in pem format"
    )]
    pub certificate: Option<String>,
    #[structopt(
        long = "private-key",
        help = "path to the tls private key in pem format"
    )]
    pub private_key: Option<String>,
    #[structopt(
        long = "server-name",
        help = "the domain name in the tls certificate, only required for server"
    )]
    pub name: Option<String>,
}

impl ClientParams {
    pub fn load(self) -> (Config, DesiredAuth) {
        let cfg = match self.config {
            None => Config::load_default().expect("failed to load default netidx config"),
            Some(path) => Config::load(path).expect("failed to load netidx config"),
        };
        let auth = match self.auth {
            DesiredAuth::Anonymous | DesiredAuth::Local => match (self.upn, self.spn) {
                (None, None) => self.auth,
                (Some(_), _) | (_, Some(_)) => {
                    panic!("upn/spn may not be specified for local or anonymous auth")
                }
            },
            DesiredAuth::Krb5 { .. } => {
                DesiredAuth::Krb5 { upn: self.upn, spn: self.spn }
            }
            DesiredAuth::Tls { .. } => match (self.certificate, self.private_key) {
                (Some(certificate), Some(private_key)) => {
                    DesiredAuth::Tls { certificate, private_key, name: self.name }
                }
                (_, _) => {
                    panic!("certificate, and private-key are required arguments for tls")
                }
            },
        };
        (cfg, auth)
    }
}
