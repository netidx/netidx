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
        long = "root-certificates",
        help = "path to the tls root certificates in pem format"
    )]
    pub root_certificates: Option<String>,
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
            DesiredAuth::Tls { .. } => match (self.root_certificates, self.certificate, self.private_key) {
                (Some(root_certificates), Some(certificate), Some(private_key)) => {
                    DesiredAuth::Tls {root_certificates, certificate, private_key, name: self.name }
                }
                (_, _, _) => panic!("root-certificates, certificate, and private-key are required arguments for tls")
            }
        };
        (cfg, auth)
    }
}
