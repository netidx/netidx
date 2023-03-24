use anyhow::Result;
use chrono::prelude::*;
use std::{cmp::Ordering, path::PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum File {
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
    async fn read<P: AsRef<std::path::Path>>(archive: P) -> Result<Vec<File>> {
        let mut files = vec![];
        let mut reader = tokio::fs::read_dir(archive).await?;
        while let Some(dir) = reader.next_entry().await? {
            let typ = dir.file_type().await?;
            if typ.is_file() {
                let name = dir.file_name();
                let name = name.to_string_lossy();
                if name == "current" {
                    files.push(File::Head);
                } else if let Ok(ts) = name.parse::<DateTime<Utc>>() {
                    files.push(File::Historical(ts));
                }
            }
        }
        files.sort();
        Ok(files)
    }

    pub(super) fn path(&self, base: &PathBuf) -> PathBuf {
        match self {
            File::Head => base.join("current"),
            File::Historical(h) => base.join(h.to_rfc3339()),
        }
    }
}

pub(super) struct LogfileCollection(Vec<File>);

impl LogfileCollection {
    pub(super) async fn new<P: AsRef<std::path::Path>>(archive_path: P) -> Result<Self> {
        Ok(Self(File::read(archive_path).await?))
    }

    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    pub(super) fn first(&self) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            *self.0.first().unwrap()
        }
    }

    pub(super) fn last(&self) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            *self.0.last().unwrap()
        }
    }

    pub(super) fn find(&self, ts: DateTime<Utc>) -> File {
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

    pub(super) fn next(&self, cur: File) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            match self.0.binary_search(&cur) {
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

    pub(super) fn prev(&self, cur: File) -> File {
        if self.0.len() == 0 {
            File::Head
        } else {
            match self.0.binary_search(&cur) {
                Err(i) => {
                    if i > 0 {
                        self.0[i - 1]
                    } else {
                        self.0[i]
                    }
                }
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
}
