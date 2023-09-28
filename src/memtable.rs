use crossbeam_skiplist::SkipMap;

// Write the MemTable
// Then the WAL
// Then SSTable, which can read through multiple serialized MemTables
// with filters
#[derive(Debug)]
pub struct MemTable {
    pub entries: SkipMap<Vec<u8>, MemTableEntry>,
}

impl PartialEq for MemTable {
    fn eq(&self, other: &Self) -> bool {
        if self.entries.len() != other.entries.len() {
            return false;
        }
        for entry in self.entries.iter() {
            let (key, val) = (entry.key(), entry.value());
            if !other.entries.contains_key(key) || other.entries.get(key).unwrap().value() != val {
                return false;
            }
        }
        true
    }
}

// if value is none, it is deleted?
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MemTableEntry {
    pub key: Vec<u8>,           // +24 24
    pub value: Option<Vec<u8>>, // +24 48
    pub timestamp: u128,        // +16 64
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: SkipMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get_value_by_key(&self, key: &[u8]) -> Option<MemTableEntry> {
        self.entries.get(key).map(|e| e.value().clone())
    }

    pub fn insert(&mut self, entry: MemTableEntry) {
        self.entries.insert(entry.key.clone(), entry);
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
        };

        match self.get_value_by_key(key) {
            Some(entry) => self.insert(entry.clone()),
            None => self.insert(entry),
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        if self.get_value_by_key(key).is_some() {
            self.entries.remove(key);
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
        self.get_value_by_key(key)
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(memtable: &mut MemTable, key: &[u8], value: &[u8], timestamp: u128) {
        memtable.set(key, value, timestamp);
    }

    #[test]
    fn it_works() {
        let mut memtable = MemTable::new();
        let key = &[1];
        let value = &[2];
        let timestamp = 1000;
        insert(&mut memtable, key, value, timestamp);
        assert_eq!(
            memtable.get(&[1]),
            Some(MemTableEntry {
                key: key.to_vec(),
                value: Some(value.to_vec()),
                timestamp
            })
        );
        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn insert_and_delete() {
        let mut memtable = MemTable::new();
        let key = &[1];
        let value = &[2];
        let timestamp = 1000;
        insert(&mut memtable, key, value, timestamp);
        memtable.delete(key);
        assert_eq!(memtable.get(&[1]), None);
        assert_eq!(memtable.len(), 0);
    }
}
