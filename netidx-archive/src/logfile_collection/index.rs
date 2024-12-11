use crate::config::Config;
use anyhow::Result;
use chrono::prelude::*;
use log::{debug, info, warn};
use std::{cmp::Ordering, path::PathBuf, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum File {
    Head,
    Historical(DateTime<Utc>),
}

impl PartialOrd for File {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (File::Head, File::Head) => Some(Ordering::Equal),
            (File::Head, File::Historical(_)) => Some(Ordering::Greater),
            (File::Historical(_), File::Head) => Some(Ordering::Less),
            (File::Historical(d0), File::Historical(d1)) => d0.partial_cmp(d1),
        }
    }
}

impl Ord for File {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl File {
    fn read(config: &Config, shard: &str) -> Result<Vec<File>> {
        let mut files = vec![];
        {
            let path = config.archive_directory.join(shard);
            std::fs::create_dir_all(&path)?;
            for dir in std::fs::read_dir(&path)? {
                let dir = dir?;
                let typ = dir.file_type()?;
                if typ.is_file() || typ.is_symlink() {
                    let name = dir.file_name();
                    // 'I' stands in for : on windows because windows ... is windows
                    // Why did I pick I. Maybe I stands for I hate windows. Or maybe I just wanted
                    // to overload I as many times as I could so that I can express what a joke
                    // I think windows is.
                    // These are all very good hypothesis, but in fact I just like the way I looks.
                    let name = name.to_string_lossy().replace("I", ":");
                    if name == "current" {
                        files.push(File::Head);
                    } else if let Ok(ts) = name.parse::<DateTime<Utc>>() {
                        files.push(File::Historical(ts));
                    }
                }
            }
        }
        debug!("would run list, cmd config {:?}", &config.archive_cmds);
        if let Some(cmds) = &config.archive_cmds {
            use std::process::Command;
            info!("running list command");
            let args = cmds.list.1.iter().cloned().map(|s| s.replace("{shard}", shard));
            match Command::new(&cmds.list.0).args(args).output() {
                Err(e) => warn!("failed to run list command {}", e),
                Ok(o) if !o.status.success() => warn!("list command failed {:?}", o),
                Ok(output) => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    if stderr.len() > 0 {
                        warn!("list stderr {}", stderr);
                    }
                    info!("list succeeded with {}", &stdout);
                    for name in stdout.split("\n") {
                        match name.parse::<DateTime<Utc>>() {
                            Err(e) => {
                                warn!("failed to parse list ts \'{}\', {}", name, e)
                            }
                            Ok(ts) => files.push(File::Historical(ts)),
                        }
                    }
                }
            };
        }
        files.sort();
        files.dedup();
        Ok(files)
    }

    pub(super) fn path(&self, base: &PathBuf, shard: &str) -> PathBuf {
        match self {
            File::Head => base.join(shard).join("current"),
            File::Historical(h) => base.join(shard).join(h.to_rfc3339()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveIndex(Arc<Vec<File>>);

impl ArchiveIndex {
    pub fn new(config: &Config, shard: &str) -> Result<Self> {
        Ok(Self(Arc::new(File::read(&config, shard)?)))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn first(&self) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            *self.0.first().unwrap()
        }
    }

    pub fn last(&self) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            *self.0.last().unwrap()
        }
    }

    pub fn find(&self, ts: DateTime<Utc>) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            match self.0.binary_search(&File::Historical(ts)) {
                Err(i) => self.0[i],
                Ok(i) => {
                    if i + 1 < self.0.len() {
                        self.0[i + 1]
                    } else {
                        File::Head
                    }
                }
            }
        }
    }

    pub fn next(&self, cur: File) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            match self.0.binary_search(&cur) {
                Err(i) | Ok(i) => {
                    if i + 1 < self.0.len() {
                        self.0[i + 1]
                    } else {
                        self.0[i]
                    }
                }
            }
        }
    }

    pub fn prev(&self, cur: File) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            match self.0.binary_search(&cur) {
                Err(i) | Ok(i) => {
                    if i > 0 {
                        self.0[i - 1]
                    } else {
                        self.0[i]
                    }
                }
            }
        }
    }
}
