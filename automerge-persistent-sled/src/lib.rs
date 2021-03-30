use automerge_protocol::ActorId;

/// The key to use to store the document in the document tree
const DOCUMENT_KEY: &[u8] = b"document";

/// The persister that stores changes and documents in sled trees.
pub struct SledPersister {
    // TODO: should we just store a single tree and use a changes/ prefix
    changes_tree: sled::Tree,
    document_tree: sled::Tree,
}

impl SledPersister {
    pub fn new(changes_tree: sled::Tree, document_tree: sled::Tree) -> Self {
        Self {
            changes_tree,
            document_tree,
        }
    }
}

impl automerge_persistent::Persister for SledPersister {
    type Error = sled::Error;

    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.changes_tree
            .iter()
            .values()
            .map(|v| v.map(|v| v.to_vec()))
            .collect()
    }

    fn insert_change(
        &mut self,
        actor_id: ActorId,
        seq: u64,
        change: Vec<u8>,
    ) -> Result<(), Self::Error> {
        let key = make_key(&actor_id, seq);
        self.changes_tree.insert(key, change)?;
        Ok(())
    }

    fn remove_change(&mut self, actor_id: &ActorId, seq: u64) -> Result<(), Self::Error> {
        let key = make_key(actor_id, seq);
        self.changes_tree.remove(key)?;
        Ok(())
    }

    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.document_tree.get(DOCUMENT_KEY)?.map(|v| v.to_vec()))
    }

    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.document_tree.insert(DOCUMENT_KEY, data)?;
        Ok(())
    }
}

/// Make a key from the actor_id and sequence_number.
///
/// Converts the actor_id to bytes and appends the sequence_number in big endian form.
fn make_key(actor_id: &ActorId, seq: u64) -> Vec<u8> {
    let mut key = actor_id.to_bytes();
    key.extend(&seq.to_be_bytes());
    key
}
