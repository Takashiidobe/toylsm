use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub struct WALEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
}

pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    pub fn new(path: PathBuf) -> io::Result<WALIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WALIterator { reader })
    }
}

impl Iterator for WALIterator {
    type Item = WALEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let mut key_len_buffer = [0; 8];
        if self.reader.read_exact(&mut key_len_buffer).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(key_len_buffer);

        let mut val_len_buffer = [0; 8];
        if self.reader.read_exact(&mut val_len_buffer).is_err() {
            return None;
        }
        let value_len = usize::from_le_bytes(val_len_buffer);

        let mut key = vec![0; key_len];
        if self.reader.read_exact(&mut key).is_err() {
            return None;
        }

        let mut value_buf = vec![0; value_len];
        if self.reader.read_exact(&mut value_buf).is_err() {
            return None;
        }
        let value = Some(value_buf);

        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        Some(WALEntry {
            key,
            value,
            timestamp,
        })
    }
}

#[derive(Debug)]
pub struct WAL {
    path: PathBuf,
    pub file: BufWriter<File>,
}

impl IntoIterator for WAL {
    type Item = WALEntry;

    type IntoIter = WALIterator;

    fn into_iter(self) -> Self::IntoIter {
        WALIterator::new(self.path).unwrap()
    }
}

impl WAL {
    pub fn new(dir: &Path) -> io::Result<WAL> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WAL { path, file })
    }

    pub fn from_path(path: &Path) -> io::Result<WAL> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let file = BufWriter::new(file);

        Ok(WAL {
            path: path.to_owned(),
            file,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
