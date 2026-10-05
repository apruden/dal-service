//! Bounded, file-backed Raft snapshot encoding.
//!
//! A snapshot is a stream of length-delimited key/value records followed by a
//! record count and SHA-256 checksum. Only one record and OpenRaft's configured
//! wire chunk are resident in memory at a time.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use sha2::{Digest, Sha256};
#[cfg(test)]
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, ReadBuf};

use crate::error::{Error, Result};

const MAGIC: &[u8; 8] = b"DALSNP01";
const END: u32 = u32::MAX;
/// Any individual state record came from a Raft append bounded below 64 MiB.
/// Enforce the same order of magnitude before allocating while decoding.
const MAX_RECORD_COMPONENT_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECORD_BYTES: usize = 65 * 1024 * 1024;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// A temporary snapshot file deleted when OpenRaft releases it.
pub struct SnapshotFile {
    file: tokio::fs::File,
    path: PathBuf,
}

impl std::fmt::Debug for SnapshotFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotFile")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl SnapshotFile {
    fn create_std(dir: &Path) -> Result<(File, PathBuf)> {
        std::fs::create_dir_all(dir)?;
        for _ in 0..1024 {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = dir.join(format!("snapshot-{}-{sequence}.tmp", std::process::id()));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => return Ok((file, path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique snapshot file",
        )))
    }

    /// Build a complete file synchronously from a stable RocksDB snapshot, then
    /// rewind it for OpenRaft's asynchronous chunk reader.
    pub fn build(
        dir: &Path,
        encode: impl FnOnce(&mut BufWriter<&mut File>) -> Result<()>,
    ) -> Result<SnapshotFile> {
        let (mut file, path) = Self::create_std(dir)?;
        let result = (|| {
            {
                let mut writer = BufWriter::with_capacity(64 * 1024, &mut file);
                encode(&mut writer)?;
                writer.flush()?;
            }
            file.sync_all()?;
            file.seek(SeekFrom::Start(0))?;
            Ok(())
        })();
        if let Err(error) = result {
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        Ok(SnapshotFile {
            file: tokio::fs::File::from_std(file),
            path,
        })
    }

    /// Empty seekable destination used while receiving OpenRaft chunks.
    pub fn receiving(dir: &Path) -> Result<SnapshotFile> {
        let (file, path) = Self::create_std(dir)?;
        Ok(SnapshotFile {
            file: tokio::fs::File::from_std(file),
            path,
        })
    }

    pub async fn rewind(&mut self) -> Result<()> {
        self.seek(SeekFrom::Start(0)).await?;
        Ok(())
    }

    /// Finish Tokio's pending file writes before a blocking installer opens
    /// its own buffered reader for the complete snapshot.
    pub async fn prepare_for_blocking_read(&mut self) -> Result<()> {
        tokio::io::AsyncWriteExt::flush(&mut self.file).await?;
        Ok(())
    }

    pub fn decode_records_sync(
        &self,
        mut record: impl FnMut(Vec<u8>, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut reader = BufReader::with_capacity(64 * 1024, File::open(&self.path)?);
        let mut magic = [0u8; MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::codec("invalid snapshot magic"));
        }
        let mut hasher = Sha256::new();
        let mut count = 0u64;
        loop {
            let mut key_len_bytes = [0u8; 4];
            reader.read_exact(&mut key_len_bytes)?;
            let key_len = u32::from_le_bytes(key_len_bytes);
            if key_len == END {
                break;
            }
            let mut value_len_bytes = [0u8; 4];
            reader.read_exact(&mut value_len_bytes)?;
            let value_len = u32::from_le_bytes(value_len_bytes) as usize;
            let key_len = key_len as usize;
            validate_lengths(key_len, value_len)?;
            let mut key = vec![0u8; key_len];
            let mut value = vec![0u8; value_len];
            crate::perf::record_snapshot_record_bytes(key_len.saturating_add(value_len));
            reader.read_exact(&mut key)?;
            reader.read_exact(&mut value)?;
            hasher.update(key_len_bytes);
            hasher.update(value_len_bytes);
            hasher.update(&key);
            hasher.update(&value);
            record(key, value)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| Error::codec("snapshot record count exhausted"))?;
        }
        let mut expected_count = [0u8; 8];
        let mut expected_digest = [0u8; 32];
        reader.read_exact(&mut expected_count)?;
        reader.read_exact(&mut expected_digest)?;
        if u64::from_le_bytes(expected_count) != count {
            return Err(Error::codec("snapshot record count mismatch"));
        }
        if hasher.finalize().as_slice() != expected_digest {
            return Err(Error::codec("snapshot checksum mismatch"));
        }
        let mut trailing = [0u8; 1];
        if reader.read(&mut trailing)? != 0 {
            return Err(Error::codec("trailing bytes after snapshot footer"));
        }
        Ok(())
    }
}

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl AsyncWrite for SnapshotFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.file).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.file).poll_complete(cx)
    }
}

fn write_hashed(writer: &mut impl Write, hasher: &mut Sha256, bytes: &[u8]) -> Result<()> {
    writer.write_all(bytes)?;
    hasher.update(bytes);
    Ok(())
}

/// Encode a point-in-time sequence of state records without collecting it.
pub fn encode_records<I, K, V>(writer: &mut impl Write, records: I) -> Result<()>
where
    I: IntoIterator<Item = Result<(K, V)>>,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    writer.write_all(MAGIC)?;
    let mut hasher = Sha256::new();
    let mut count = 0u64;
    for record in records {
        let (key, value) = record?;
        let key = key.as_ref();
        let value = value.as_ref();
        validate_lengths(key.len(), value.len())?;
        crate::perf::record_snapshot_record_bytes(key.len().saturating_add(value.len()));
        let key_len = u32::try_from(key.len())
            .map_err(|_| Error::codec("snapshot key length exceeds u32"))?;
        let value_len = u32::try_from(value.len())
            .map_err(|_| Error::codec("snapshot value length exceeds u32"))?;
        write_hashed(writer, &mut hasher, &key_len.to_le_bytes())?;
        write_hashed(writer, &mut hasher, &value_len.to_le_bytes())?;
        write_hashed(writer, &mut hasher, key)?;
        write_hashed(writer, &mut hasher, value)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::codec("snapshot record count exhausted"))?;
    }
    writer.write_all(&END.to_le_bytes())?;
    writer.write_all(&count.to_le_bytes())?;
    writer.write_all(&hasher.finalize())?;
    Ok(())
}

fn validate_lengths(key_len: usize, value_len: usize) -> Result<()> {
    if key_len > MAX_RECORD_COMPONENT_BYTES || value_len > MAX_RECORD_COMPONENT_BYTES {
        return Err(Error::codec("snapshot record component exceeds limit"));
    }
    if key_len.saturating_add(value_len) > MAX_RECORD_BYTES {
        return Err(Error::codec("snapshot record exceeds combined limit"));
    }
    Ok(())
}

/// Decode and verify a snapshot, handing one bounded record at a time to the
/// staged state installer. The callback is never invoked after a checksum or
/// framing failure has been discovered for a preceding record.
#[cfg(test)]
pub async fn decode_records(
    reader: &mut SnapshotFile,
    mut record: impl FnMut(Vec<u8>, Vec<u8>) -> Result<()>,
) -> Result<()> {
    reader.rewind().await?;
    let mut reader = tokio::io::BufReader::with_capacity(64 * 1024, reader);
    let mut magic = [0u8; MAGIC.len()];
    reader.read_exact(&mut magic).await?;
    if &magic != MAGIC {
        return Err(Error::codec("invalid snapshot magic"));
    }

    let mut hasher = Sha256::new();
    let mut count = 0u64;
    loop {
        let mut key_len_bytes = [0u8; 4];
        reader.read_exact(&mut key_len_bytes).await?;
        let key_len = u32::from_le_bytes(key_len_bytes);
        if key_len == END {
            break;
        }
        let mut value_len_bytes = [0u8; 4];
        reader.read_exact(&mut value_len_bytes).await?;
        let value_len = u32::from_le_bytes(value_len_bytes) as usize;
        let key_len_usize = key_len as usize;
        validate_lengths(key_len_usize, value_len)?;
        let mut key = vec![0u8; key_len_usize];
        let mut value = vec![0u8; value_len];
        reader.read_exact(&mut key).await?;
        reader.read_exact(&mut value).await?;
        hasher.update(key_len_bytes);
        hasher.update(value_len_bytes);
        hasher.update(&key);
        hasher.update(&value);
        record(key, value)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::codec("snapshot record count exhausted"))?;
    }

    let mut expected_count = [0u8; 8];
    let mut expected_digest = [0u8; 32];
    reader.read_exact(&mut expected_count).await?;
    reader.read_exact(&mut expected_digest).await?;
    if u64::from_le_bytes(expected_count) != count {
        return Err(Error::codec("snapshot record count mismatch"));
    }
    if hasher.finalize().as_slice() != expected_digest {
        return Err(Error::codec("snapshot checksum mismatch"));
    }
    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing).await? != 0 {
        return Err(Error::codec("trailing bytes after snapshot footer"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn file_backed_stream_round_trips_records() {
        let dir = tempfile::tempdir().unwrap();
        let records = vec![
            Ok((b"a".to_vec(), b"one".to_vec())),
            Ok((b"b".to_vec(), b"two".to_vec())),
        ];
        let mut file =
            SnapshotFile::build(dir.path(), |writer| encode_records(writer, records)).unwrap();
        let mut decoded = Vec::new();
        decode_records(&mut file, |key, value| {
            decoded.push((key, value));
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            decoded,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec())
            ]
        );
        let mut blocking_decoded = Vec::new();
        file.decode_records_sync(|key, value| {
            blocking_decoded.push((key, value));
            Ok(())
        })
        .unwrap();
        assert_eq!(blocking_decoded, decoded);
    }

    #[tokio::test]
    async fn checksum_rejects_a_corrupted_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = SnapshotFile::build(dir.path(), |writer| {
            encode_records(writer, [Ok((b"a".to_vec(), b"one".to_vec()))])
        })
        .unwrap();
        // Header + key/value lengths + one-byte key points at the value.
        file.seek(SeekFrom::Start(8 + 4 + 4 + 1)).await.unwrap();
        file.write_all(b"X").await.unwrap();
        file.flush().await.unwrap();

        let error = decode_records(&mut file, |_, _| Ok(())).await.unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
        let error = file.decode_records_sync(|_, _| Ok(())).unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn buffered_install_reads_records_across_buffer_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let maximum = vec![b'v'; crate::types::MAX_VALUE_BYTES];
        let records: Vec<_> = (0..1000)
            .map(|i| Ok((format!("key-{i:04}").into_bytes(), vec![b'x'; 128])))
            .chain(std::iter::once(Ok((b"last".to_vec(), maximum.clone()))))
            .collect();
        let file =
            SnapshotFile::build(dir.path(), |writer| encode_records(writer, records)).unwrap();
        let mut count = 0;
        file.decode_records_sync(|key, value| {
            if key == b"last" {
                assert_eq!(value, maximum);
            }
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1001);
    }

    #[test]
    #[ignore = "release snapshot stream measurement; set DAL_BENCH_DIR to a durable filesystem"]
    fn maximum_value_snapshot_stream_measurement() {
        let records: usize = std::env::var("DAL_BENCH_SNAPSHOT_RECORDS")
            .ok()
            .map(|value| value.parse().expect("invalid snapshot record count"))
            .unwrap_or(512);
        assert!((1..=2048).contains(&records));
        let root = match std::env::var_os("DAL_BENCH_DIR") {
            Some(path) => tempfile::tempdir_in(path).unwrap(),
            None => tempfile::tempdir().unwrap(),
        };
        let value = vec![b'v'; crate::types::MAX_VALUE_BYTES];
        let began = std::time::Instant::now();
        let file = SnapshotFile::build(root.path(), |writer| {
            encode_records(
                writer,
                (0..records).map(|index| Ok((index.to_be_bytes(), value.as_slice()))),
            )
        })
        .unwrap();
        let build = began.elapsed();
        let began = std::time::Instant::now();
        let mut decoded = 0usize;
        file.decode_records_sync(|key, read| {
            assert_eq!(key, decoded.to_be_bytes());
            assert_eq!(read.len(), value.len());
            assert_eq!(read[0], b'v');
            decoded += 1;
            Ok(())
        })
        .unwrap();
        let decode = began.elapsed();
        assert_eq!(decoded, records);
        let mib = records as f64 * value.len() as f64 / (1024.0 * 1024.0);
        println!(
            "snapshot_stream root={} records={} value_bytes={} logical_mib={:.1} build_s={:.3} build_mib_per_s={:.1} decode_s={:.3} decode_mib_per_s={:.1} record_peak_bytes={}",
            root.path().display(),
            records,
            value.len(),
            mib,
            build.as_secs_f64(),
            mib / build.as_secs_f64(),
            decode.as_secs_f64(),
            mib / decode.as_secs_f64(),
            crate::perf::source_record_peaks().1,
        );
    }
}
