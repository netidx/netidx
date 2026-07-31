use crate::{
    ca::{Ca, CaParams, MIN_KEY_BITS, Subject},
    ca_store,
    config_lock::ConfigDirLock,
};
use std::{io::Cursor, path::Path, time::Duration};

pub(super) async fn signed_empty_crl(dir: &Path, name: &str) -> (String, Vec<u8>) {
    let ca = Ca::init(
        &CaParams {
            directory: dir.to_path_buf(),
            subject: Subject::cn(name),
            san: vec![],
            key_bits: MIN_KEY_BITS,
            validity: Duration::from_secs(30 * 86400),
        },
        None,
    )
    .unwrap();
    drop(ca);
    let key = std::fs::read(dir.join("private.key")).unwrap();
    let ca_pem = std::fs::read(dir.join("certificate.pem")).unwrap();
    let ca_der =
        rustls_pemfile::certs(&mut Cursor::new(ca_pem)).next().unwrap().unwrap().to_vec();
    let lock = ConfigDirLock::acquire_for_ca_dir(dir).await.unwrap();
    let mut ca = ca_store::CaDir::open(lock, dir).await.unwrap();
    ca.store.write_crl(&key).await.unwrap();
    let pem = std::fs::read_to_string(ca.store.crl_path()).unwrap();
    (pem, ca_der)
}
