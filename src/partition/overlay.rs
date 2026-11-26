//! An in-memory write overlay: a resize rehearses its writes here before touching the disk.

use std::io::{Read, Seek, SeekFrom, Write};

const SECTOR: u64 = 512;

/// Reads fall through to the file; writes stay in memory, a sector at a time.
pub(crate) struct Overlay<'a, R: Read + Seek> {
    file: &'a mut R,
    pos: u64,
    sectors: std::collections::HashMap<u64, Vec<u8>>,
}

impl<'a, R: Read + Seek> Overlay<'a, R> {
    pub(crate) fn new(file: &'a mut R) -> Self {
        Overlay {
            file,
            pos: 0,
            sectors: std::collections::HashMap::new(),
        }
    }

    /// Sector `n` as the rehearsal sees it (past the file's end reads as zeros).
    pub(crate) fn sector(&mut self, n: u64) -> std::io::Result<Vec<u8>> {
        if let Some(s) = self.sectors.get(&n) {
            return Ok(s.clone());
        }
        let mut buf = vec![0u8; SECTOR as usize];
        self.file.seek(SeekFrom::Start(n * SECTOR))?;
        let mut got = 0;
        while got < buf.len() {
            match self.file.read(&mut buf[got..])? {
                0 => break,
                k => got += k,
            }
        }
        Ok(buf)
    }
}

impl<R: Read + Seek> Read for Overlay<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let (n, at) = (self.pos / SECTOR, (self.pos % SECTOR) as usize);
        let sector = self.sector(n)?;
        let k = out.len().min(sector.len() - at);
        out[..k].copy_from_slice(&sector[at..at + k]);
        self.pos += k as u64;
        Ok(k)
    }
}

impl<R: Read + Seek> Write for Overlay<'_, R> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let (n, at) = (self.pos / SECTOR, (self.pos % SECTOR) as usize);
        let mut sector = self.sector(n)?;
        let k = data.len().min(sector.len() - at);
        sector[at..at + k].copy_from_slice(&data[..k]);
        self.sectors.insert(n, sector);
        self.pos += k as u64;
        Ok(k)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<R: Read + Seek> Seek for Overlay<'_, R> {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        let here = self.file.stream_position()?;
        let end = self.file.seek(SeekFrom::End(0))?;
        self.file.seek(SeekFrom::Start(here))?;
        self.pos = match to {
            SeekFrom::Start(n) => n,
            SeekFrom::End(d) => (end as i64 + d).max(0) as u64,
            SeekFrom::Current(d) => (self.pos as i64 + d).max(0) as u64,
        };
        Ok(self.pos)
    }
}
