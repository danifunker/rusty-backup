//! Copy a file between two filesystems without holding it in memory.
//!
//! The source's `write_file_to` runs on a scoped thread and feeds a bounded
//! channel of chunks; the consumer (usually the target's `create_file`) reads
//! the other end. Peak memory is a few chunks, whatever the file's size. A
//! producer error reaches the consumer as an `io::Error` rather than a short
//! read, so a failed source never yields a silently truncated copy.

use std::io::{self, Read, Write};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

const CHUNK: usize = 1 << 20;
const DEPTH: usize = 4;

type Msg = Result<Vec<u8>, String>;

struct ChanWriter {
    tx: SyncSender<Msg>,
    buf: Vec<u8>,
}

impl Write for ChanWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let room = CHUNK - self.buf.len();
        let n = data.len().min(room);
        self.buf.extend_from_slice(&data[..n]);
        if self.buf.len() == CHUNK {
            self.flush()?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK));
        self.tx
            .send(Ok(chunk))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "consumer stopped reading"))
    }
}

struct ChanReader {
    rx: Receiver<Msg>,
    cur: Vec<u8>,
    pos: usize,
}

impl Read for ChanReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.pos == self.cur.len() {
            match self.rx.recv() {
                Ok(Ok(chunk)) => {
                    self.cur = chunk;
                    self.pos = 0;
                }
                Ok(Err(e)) => return Err(crate::compat::io_other(e)),
                Err(_) => return Ok(0),
            }
        }
        let n = out.len().min(self.cur.len() - self.pos);
        out[..n].copy_from_slice(&self.cur[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Run `consume` over a reader that streams `entry`'s bytes out of `source`.
pub fn with_file_reader<S, T>(
    source: &mut S,
    entry: &FileEntry,
    consume: impl FnOnce(&mut dyn Read) -> Result<T, FilesystemError>,
) -> Result<T, FilesystemError>
where
    S: Filesystem + Send + ?Sized,
{
    let (tx, rx) = sync_channel::<Msg>(DEPTH);
    std::thread::scope(|scope| {
        let producer = scope.spawn(move || {
            let mut w = ChanWriter {
                tx: tx.clone(),
                buf: Vec::with_capacity(CHUNK),
            };
            let r = source.write_file_to(entry, &mut w).and_then(|n| {
                w.flush()?;
                Ok(n)
            });
            if let Err(e) = &r {
                let _ = tx.send(Err(e.to_string()));
            }
            r
        });
        let mut reader = ChanReader {
            rx,
            cur: Vec::new(),
            pos: 0,
        };
        let out = consume(&mut reader);
        // Unblock a producer still waiting on a full channel if the consumer stopped early.
        drop(reader);
        let produced = producer
            .join()
            .map_err(|_| FilesystemError::Io(crate::compat::io_other("source reader panicked")))?;
        match (out, produced) {
            (Ok(v), Ok(_)) => Ok(v),
            (Err(e), _) => Err(e),
            (Ok(_), Err(e)) => Err(e),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One file at `/big`, served by the default chunked `write_file_to`.
    struct OneFile(Vec<u8>);

    impl Filesystem for OneFile {
        fn root(&mut self) -> Result<FileEntry, FilesystemError> {
            Ok(FileEntry::root())
        }
        fn list_directory(&mut self, _: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
            Ok(vec![self.entry()])
        }
        fn read_file(&mut self, _: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
            Ok(self.0[..self.0.len().min(max)].to_vec())
        }
        fn write_file_to(
            &mut self,
            _: &FileEntry,
            w: &mut dyn Write,
        ) -> Result<u64, FilesystemError> {
            for c in self.0.chunks(64 * 1024) {
                w.write_all(c)?;
            }
            Ok(self.0.len() as u64)
        }
        fn volume_label(&self) -> Option<&str> {
            None
        }
        fn fs_type(&self) -> &str {
            "test"
        }
        fn total_size(&self) -> u64 {
            0
        }
        fn used_size(&self) -> u64 {
            0
        }
    }

    impl OneFile {
        fn entry(&self) -> FileEntry {
            FileEntry::new_file("big".into(), "/big".into(), self.0.len() as u64, 1)
        }
    }

    #[test]
    fn streams_a_file_larger_than_the_channel() {
        let body: Vec<u8> = (0..(CHUNK * DEPTH * 3 + 17))
            .map(|i| (i % 251) as u8)
            .collect();
        let mut fs = OneFile(body.clone());
        let entry = fs.entry();
        let got = with_file_reader(&mut fs, &entry, |r| {
            let mut v = Vec::new();
            r.read_to_end(&mut v)?;
            Ok(v)
        })
        .unwrap();
        assert_eq!(got, body);
    }

    #[test]
    fn a_consumer_that_stops_early_does_not_hang() {
        let mut fs = OneFile(vec![7u8; CHUNK * DEPTH * 4]);
        let entry = fs.entry();
        let got = with_file_reader(&mut fs, &entry, |r| {
            let mut one = [0u8; 10];
            r.read_exact(&mut one)?;
            Ok(one[0])
        });
        assert!(got.is_err(), "the producer's broken pipe surfaces");
    }
}
