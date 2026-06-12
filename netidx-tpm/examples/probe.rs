//! Does sealing work on this machine, and if not, why not?
//!
//!   cargo run -p netidx-tpm --example probe                # in-process round trip
//!   cargo run -p netidx-tpm --example probe seal <file>    # seal a probe to a file
//!   cargo run -p netidx-tpm --example probe unseal <file>  # unseal it (run in a
//!                                                          # fresh process / after
//!                                                          # a reboot to verify
//!                                                          # durability)
//!
//! Prints the full error chain from the platform underneath — the
//! fastest way to see what a TPM/Secure-Enclave problem actually is.

fn main() {
    let mut args = std::env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (Some("seal"), Some(path)) => match netidx_tpm::seal(b"probe") {
            Ok(blob) => {
                std::fs::write(&path, blob).expect("writing the blob");
                println!("sealed to {path}");
            }
            Err(e) => println!("seal failed: {e:#}"),
        },
        (Some("unseal"), Some(path)) => {
            let blob = std::fs::read(&path).expect("reading the blob");
            match netidx_tpm::unseal(&blob) {
                Ok(s) if &*s == b"probe" => println!("unseal: ok, payload verified"),
                Ok(_) => println!("unseal: WRONG PAYLOAD"),
                Err(e) => println!("unseal failed: {e:#}"),
            }
        }
        _ => {
            println!("available: {}", netidx_tpm::available());
            match netidx_tpm::seal(b"probe") {
                Ok(blob) => {
                    println!("seal: ok ({} byte blob)", blob.len());
                    match netidx_tpm::unseal(&blob) {
                        Ok(s) if &*s == b"probe" => {
                            println!("unseal: ok, round trip verified")
                        }
                        Ok(_) => println!("unseal: WRONG PAYLOAD"),
                        Err(e) => println!("unseal failed: {e:#}"),
                    }
                }
                Err(e) => println!("seal failed: {e:#}"),
            }
        }
    }
}
