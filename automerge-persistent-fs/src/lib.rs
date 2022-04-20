use std::{
    fs,
    os::unix::prelude::OsStrExt,
    path::{Path, PathBuf},
};

use automerge::ActorId;
use automerge_persistent::{Persister, StoredSizes};
use hex::FromHexError;

#[derive(Debug)]
pub struct FsPersister {
    changes_path: PathBuf,
    doc_path: PathBuf,
    sync_path: PathBuf,
    sizes: StoredSizes,
}

/// Possible errors from persisting.
#[derive(Debug, thiserror::Error)]
pub enum FsPersisterError {
    /// Internal errors from sled.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Hex(#[from] FromHexError),
}

const CHANGES_DIR: &str = "changes";
const DOC_FILE: &str = "doc";
const SYNC_DIR: &str = "sync";

impl FsPersister {
    pub fn new<R: AsRef<Path>, P: AsRef<Path>>(
        root: R,
        prefix: P,
    ) -> Result<Self, FsPersisterError> {
        let root_path = root.as_ref().join(&prefix);
        fs::create_dir_all(&root_path)?;

        let changes_path = root_path.join(CHANGES_DIR);
        if fs::metadata(&changes_path).is_err() {
            fs::create_dir(&changes_path)?;
        }

        let doc_path = root_path.join(DOC_FILE);

        let sync_path = root_path.join(SYNC_DIR);
        if fs::metadata(&sync_path).is_err() {
            fs::create_dir(&sync_path)?;
        }

        let mut s = Self {
            changes_path,
            doc_path,
            sync_path,
            sizes: StoredSizes::default(),
        };

        s.sizes.changes = s.get_changes()?.iter().map(Vec::len).sum();
        s.sizes.document = s.get_document()?.unwrap_or_default().len();
        s.sizes.sync_states = s
            .get_peer_ids()?
            .iter()
            .map(|id| s.get_sync_state(id).map(|o| o.unwrap_or_default().len()))
            .collect::<Result<Vec<usize>, _>>()?
            .iter()
            .sum();

        Ok(s)
    }

    fn make_changes_path(&self, actor_id: &ActorId, seq: u64) -> PathBuf {
        self.changes_path
            .join(format!("{}-{}", actor_id.to_hex_string(), seq))
    }

    fn make_peer_path(&self, peer_id: &[u8]) -> PathBuf {
        self.sync_path.join(hex::encode(peer_id))
    }
}

impl Persister for FsPersister {
    type Error = FsPersisterError;

    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        fs::read_dir(&self.changes_path)?
            .filter_map(|entry| {
                if let Ok((Ok(file_type), path)) =
                    entry.map(|entry| (entry.file_type(), entry.path()))
                {
                    if file_type.is_file() {
                        Some(fs::read(path).map_err(FsPersisterError::from))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    fn insert_changes(&mut self, changes: Vec<(ActorId, u64, Vec<u8>)>) -> Result<(), Self::Error> {
        for (a, s, c) in changes {
            fs::write(self.make_changes_path(&a, s), &c)?;
            self.sizes.changes += c.len();
        }
        Ok(())
    }

    fn remove_changes(&mut self, changes: Vec<(&ActorId, u64)>) -> Result<(), Self::Error> {
        for (a, s) in changes {
            let path = self.make_changes_path(a, s);
            if let Ok(meta) = fs::metadata(&path) {
                if meta.is_file() {
                    fs::remove_file(&path)?;
                    self.sizes.changes -= meta.len() as usize;
                }
            }
        }
        Ok(())
    }

    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        if fs::metadata(&self.doc_path).is_ok() {
            return Ok(fs::read(&self.doc_path).map(|v| if v.is_empty() { None } else { Some(v) })?);
        }
        Ok(None)
    }

    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        fs::write(&self.doc_path, &data)?;
        self.sizes.document = data.len();
        Ok(())
    }

    fn get_sync_state(&self, peer_id: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let path = self.make_peer_path(peer_id);
        if fs::metadata(&path).is_ok() {
            return Ok(fs::read(&path).map(|v| if v.is_empty() { None } else { Some(v) })?);
        }
        Ok(None)
    }

    fn set_sync_state(&mut self, peer_id: Vec<u8>, sync_state: Vec<u8>) -> Result<(), Self::Error> {
        fs::write(self.make_peer_path(&peer_id), &sync_state)?;
        self.sizes.sync_states += sync_state.len();
        Ok(())
    }

    fn remove_sync_states(&mut self, peer_ids: &[&[u8]]) -> Result<(), Self::Error> {
        for peer_id in peer_ids {
            let path = self.make_peer_path(peer_id);
            if let Ok(meta) = fs::metadata(&path) {
                if meta.is_file() {
                    fs::remove_file(&path)?;
                    self.sizes.sync_states -= meta.len() as usize;
                }
            }
        }
        Ok(())
    }

    fn get_peer_ids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        fs::read_dir(&self.sync_path)?
            .filter_map(|entry| {
                if let Ok((Ok(file_type), path)) =
                    entry.map(|entry| (entry.file_type(), entry.path()))
                {
                    if file_type.is_file() {
                        Some(
                            hex::decode(path.file_name().unwrap().as_bytes())
                                .map_err(FsPersisterError::from),
                        )
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    fn sizes(&self) -> StoredSizes {
        self.sizes.clone()
    }

    fn flush(&mut self) -> Result<usize, Self::Error> {
        Ok(0)
    }
}
