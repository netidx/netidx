use anyhow::{anyhow, Result};
use std::process::Command;
use tokio::task;

// Unix group membership is a little complex, it can come from a
// lot of places, and it's not entirely standardized at the api
// level, it seems libc provides getgrouplist on most platforms,
// but unfortunatly Apple doesn't implement it. Luckily the 'id'
// command is specified in POSIX.
pub(crate) struct Mapper(String);

impl Mapper {
    pub(crate) fn new() -> Result<Mapper> {
        task::block_in_place(|| {
            let out = Command::new("sh").arg("-c").arg("which id").output()?;
            let buf = String::from_utf8_lossy(&out.stdout);
            let path =
                buf.lines().next().ok_or_else(|| anyhow!("can't find the id command"))?;
            Ok(Mapper(String::from(path)))
        })
    }

    pub(crate) fn groups(&mut self, user: &str) -> Result<Vec<String>> {
        task::block_in_place(|| {
            let out = Command::new(&self.0).arg(user).output()?;
            Mapper::parse_output(&String::from_utf8_lossy(&out.stdout))
        })
    }

    fn parse_output(out: &str) -> Result<Vec<String>> {
        let mut groups = Vec::new();
        match out.find("groups=") {
            None => Ok(Vec::new()),
            Some(i) => {
                let mut s = &out[i..];
                while let Some(i_op) = s.find('(') {
                    match s.find(')') {
                        None => {
                            return Err(anyhow!(
                                "invalid id command output, expected ')'"
                            ))
                        }
                        Some(i_cp) => {
                            groups.push(String::from(&s[i_op + 1..i_cp]));
                            s = &s[i_cp + 1..];
                        }
                    }
                }
                Ok(groups)
            }
        }
    }
}
