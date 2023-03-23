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

pub(super) struct LogfileCollection {
    files: Vec<File>,
    current: Option<usize>,
}

impl LogfileCollection {
    pub(super) async fn new<P: AsRef<std::path::Path>>(archive_path: P) -> Result<Self> {
        Ok(Self { files: File::read(archive_path).await?, current: None })
    }

    pub(super) fn set_current(&mut self, file: Option<File>) {
        match file {
            None => self.current = None,
            Some(file) => {
                let i = self.files.iter().enumerate().find_map(|(i, f)| if file == *f { Some(i) } else { None });
                self.current = i;
            }
        }
    }

    pub(super) fn first(&self) -> File {
        if self.files.len() == 0 {
            File::Head
        } else {
            *self.files.first().unwrap()
        }
    }

    pub(super) fn last(&self) -> File {
        if self.files.len() == 0 {
            File::Head
        } else {
            *self.files.last().unwrap()
        }
    }

    pub(super) fn find(&self, ts: DateTime<Utc>) -> File {
        if self.files.len() == 0 {
            File::Head
        } else {
            match self.files.binary_search(&File::Historical(ts)) {
                Err(i) => self.files[i],
                Ok(i) => {
                    if i + 1 < self.files.len() {
                        self.files[i + 1]
                    } else {
                        File::Head
                    }
                }
            }
        }
    }

    pub(super) fn next(&self, cur: File) -> File {
        if self.files.len() == 0 {
            File::Head
        } else {
            match self.files.binary_search(&cur) {
                Err(i) => self.files[i],
                Ok(i) => {
                    if i + 1 < self.files.len() {
                        self.files[i + 1]
                    } else {
                        File::Head
                    }
                }
            }
        }
    }

    pub(super) fn prev(&self, cur: File) -> File {
        if self.files.len() == 0 {
            File::Head
        } else {
            match self.files.binary_search(&cur) {
                Err(i) => {
                    if i > 0 {
                        self.files[i - 1]
                    } else {
                        self.files[i]
                    }
                }
                Ok(i) => {
                    if i + 1 < self.files.len() {
                        self.files[i + 1]
                    } else {
                        File::Head
                    }
                }
            }
        }
    }
}
