use crate::{
    memtable::{MemTable, MemTableEntry},
    wal::WAL,
};
use std::{
    fs::{read_dir, remove_file},
    io::{self, Write},
    path::{Path, PathBuf},
};

pub struct SSTable {
    memtable: MemTable,
    wal: WAL,
}

/// Gets the set of files with an extension for a given directory.
fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for file in read_dir(dir).unwrap() {
        let path = file.unwrap().path();
        if path.extension().unwrap() == ext {
            files.push(path);
        }
    }

    files
}

impl SSTable {
    pub fn new(dir: &Path) -> Self {
        let mut wal_files = files_with_ext(dir, "wal");
        wal_files.sort();

        let mut new_mem_table = MemTable::new();
        let mut new_wal = WAL::new(dir).unwrap();
        for wal_file in wal_files.iter() {
            if let Ok(wal) = WAL::from_path(wal_file) {
                for entry in wal.into_iter() {
                    if entry.value.is_none() {
                        new_mem_table.delete(entry.key.as_slice());
                        new_wal
                            .delete(entry.key.as_slice(), entry.timestamp)
                            .unwrap();
                    } else {
                        new_mem_table.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                            entry.timestamp,
                        );
                        new_wal
                            .set(
                                entry.key.as_slice(),
                                entry.value.unwrap().as_slice(),
                                entry.timestamp,
                            )
                            .unwrap();
                    }
                }
            }
        }
        new_wal.flush().unwrap();
        wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

        Self {
            wal: new_wal,
            memtable: new_mem_table,
        }
    }

    pub fn len(&self) -> usize {
        self.memtable.len()
    }

    pub fn is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        self.memtable.set(key, value, timestamp);
        self.wal
            .set(key, value, timestamp)
            .expect("Adding to WAL failed");
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        self.memtable.delete(key);
        self.wal
            .delete(key, timestamp)
            .expect("deleting entry failed");
    }

    pub fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
        self.memtable.get(key)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.wal.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_to_sstable() {
        let path = Path::new("/home/takashi/toylsm/test");
        let mut sstable = SSTable::new(path);
        let key = &[1];
        let val = &[2];
        let key_2 = &[3];
        let timestamp = 1000;
        sstable.set(key, val, timestamp);
        sstable.set(key_2, val, timestamp + 1);
        sstable.flush().unwrap();

        let mut expected_memtable = MemTable::new();
        expected_memtable.set(key, val, timestamp);
        expected_memtable.set(key_2, val, timestamp + 1);
        assert_eq!(sstable.memtable, expected_memtable);
        assert_eq!(sstable.len(), 2);
    }
}
