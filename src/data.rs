use crate::query::OpenDALQuery;
use crate::reader::OpenDALReader;
use crate::writer::OpenDALWriter;
use crate::{RemoteConfig, path_to_str};
use arbhx_core::{DataFull, DataRead, DataReadSeek, DataUsage, DataWrite, DataWriteSeek, FilterOptions, SizedQuery, VfsBackend, VfsFull, VfsReader, VfsWriter};
use async_trait::async_trait;
use chrono::{DateTime, Local, Utc};
use opendal::layers::{ConcurrentLimitLayer, LoggingLayer, ThrottleLayer};
use opendal::{Metadata, Operator};
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;

/// `OpenDALBackend` contains a wrapper around an [`Operator`] of the `OpenDAL` library.
#[derive(Clone, Debug)]
pub struct OpenDALBackend {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) operator: Operator,
    pub(crate) config: RemoteConfig,
}

impl OpenDALBackend {
    /// Creates a new [`OpenDALBackend`] with the specified config.
    ///
    /// # Arguments
    /// `config` - The [`RemoteConfig`] to use.
    ///
    /// # Errors
    /// If the OpenDAL system fails to initialize.
    pub fn new(name: &str, config: RemoteConfig) -> io::Result<Self> {
        let mut operator = Operator::via_iter(config.src.scheme(), config.src.clone().to_map())
            .map_err(|x| io::Error::from(x))?; // *** map to IO error to not expose opendal.
        if let Some(x) = config.bandwidth {
            operator = operator.layer(ThrottleLayer::new(x.bandwidth, x.burst));
        }
        if let Some(x) = config.max_threads {
            operator = operator.layer(ConcurrentLimitLayer::new(x as usize));
        }
        operator = operator.layer(LoggingLayer::default()); // *** finally, add some logging!
        Ok(Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            operator,
            config,
        })
    }

    /// Converts an [`Metadata`] into a valid [`crate::Metadata`] instance.
    ///
    /// # Arguments
    /// * `path` - The [`Path`] to represent.
    /// * `meta` - The [`Metadata`] to convert.
    ///
    /// # Returns
    /// A valid [`crate::Metadata`] for use with operations.
    pub(crate) fn meta(path: &Path, meta: &Metadata) -> arbhx_core::Metadata {
        arbhx_core::Metadata::default()
            .set_path(path)
            .set_is_dir(meta.is_dir())
            .set_mtime(
                meta.last_modified()
                    .map(SystemTime::from)
                    .map(DateTime::<Utc>::from),
            )
            .set_size(meta.content_length())
    }

    /// Converts an [`Metadata`] into a valid [`crate::Metadata`] instance.
    ///
    /// # Arguments
    /// * `path` - The OpenDAL path to represent.
    /// * `meta` - The [`Metadata`] to convert.
    ///
    /// # Returns
    /// A valid [`crate::Metadata`] for use with operations.
    pub(crate) fn meta_str(path: &str, meta: &Metadata) -> io::Result<arbhx_core::Metadata> {
        let path =
            PathBuf::from_str(path).map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
        Ok(Self::meta(&path, meta))
    }

    /// Converts an [`opendal::Entry`] into a valid [`crate::Metadata`] instance.
    pub(crate) fn meta_entry(entry: opendal::Entry) -> io::Result<arbhx_core::Metadata> {
        Self::meta_str(entry.path(), entry.metadata())
    }
}

#[async_trait]
impl VfsBackend for OpenDALBackend {
    fn id(&self) -> Uuid {
        self.id
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn realpath(&self, item: &Path) -> PathBuf {
        item.to_path_buf()
    }

    fn reader(self: Arc<Self>) -> Option<Arc<dyn VfsReader>> {
        Some(self.clone())
    }

    fn writer(self: Arc<Self>) -> Option<Arc<dyn VfsWriter>> {
        Some(self.clone())
    }

    fn full(self: Arc<Self>) -> Option<Arc<dyn VfsFull>> {
        Some(self.clone())
    }

    async fn get_usage(&self) -> io::Result<Option<DataUsage>> {
        Ok(None) // *** not supported here!
    }
}

#[async_trait]
impl VfsReader for OpenDALBackend {
    async fn open_read_start(&self, item: &Path) -> io::Result<Box<dyn DataRead>> {
        let ret = OpenDALReader::new(item.to_path_buf(), self.operator.clone()).await?;
        Ok(Box::new(ret))
    }

    async fn open_read_random(&self, item: &Path) -> io::Result<Option<Box<dyn DataReadSeek>>> {
        let ret = OpenDALReader::new(item.to_path_buf(), self.operator.clone()).await?;
        Ok(Some(Box::new(ret)))
    }

    async fn get_metadata(&self, item: &Path) -> io::Result<Option<arbhx_core::Metadata>> {
        let path = path_to_str(item, false);
        if !self.operator.exists(&path).await? {
            return Ok(None);
        }
        let meta = self.operator.stat(&path).await?;
        let x_meta = Self::meta_str(&path, &meta)?;
        Ok(Some(x_meta))
    }

    async fn read_dir(
        &self,
        item: &Path,
        opts: Option<FilterOptions>,
        recursive: bool,
        include_root: bool,
    ) -> io::Result<Arc<dyn SizedQuery>> {
        let path = path_to_str(&item, true);
        let ret = OpenDALQuery::new(self.operator.clone(), path, opts, recursive, include_root)?;
        Ok(Arc::new(ret))
    }
}

#[async_trait]
impl VfsFull for OpenDALBackend {
    async fn open_full_random(&self, _item: &Path) -> io::Result<Option<Box<dyn DataFull>>> {
        Ok(None)
    }
}

#[async_trait]
impl VfsWriter for OpenDALBackend {
    async fn remove_dir(&self, dirname: &Path) -> io::Result<()> {
        self.operator
            .remove_all(&path_to_str(dirname, true))
            .await?;
        Ok(())
    }

    async fn remove_file(&self, filename: &Path) -> io::Result<()> {
        self.operator.delete(&path_to_str(filename, false)).await?;
        Ok(())
    }

    async fn create_dir(&self, item: &Path) -> io::Result<()> {
        self.operator.create_dir(&path_to_str(item, true)).await?;
        Ok(())
    }

    async fn set_times(
        &self,
        _item: &Path,
        _mtime: DateTime<Local>,
        _atime: DateTime<Local>,
    ) -> io::Result<()> {
        Ok(())
    }

    async fn set_length(&self, item: &Path, size: u64) -> io::Result<()> {
        if size != 0 {
            Err(ErrorKind::Unsupported.into())
        } else {
            self.operator
                .write(&path_to_str(item, false), Vec::<u8>::new())
                .await?;
            Ok(())
        }
    }

    async fn move_to(&self, old: &Path, new: &Path) -> io::Result<()> {
        // Check to see if the current spot is a directory or file.
        let is_dir = self
            .get_metadata(old)
            .await?
            .map(|x| x.is_dir())
            .ok_or(io::Error::from(ErrorKind::NotFound))?;
        let src = path_to_str(old, is_dir);
        let dst = path_to_str(new, is_dir);
        self.operator.rename(&src, &dst).await?;
        Ok(())
    }

    async fn copy_to(&self, old: &Path, new: &Path) -> io::Result<()> {
        let is_dir = self
            .get_metadata(old)
            .await?
            .map(|x| x.is_dir())
            .ok_or(io::Error::from(ErrorKind::NotFound))?;
        let src = path_to_str(old, is_dir);
        let dst = path_to_str(new, is_dir);
        self.operator.copy(&src, &dst).await?;
        Ok(())
    }

    async fn open_write_append(
        &self,
        item: &Path,
        overwrite: bool,
    ) -> io::Result<Box<dyn DataWrite>> {
        let ret = OpenDALWriter::new(item.to_path_buf(), self.operator.clone(), overwrite).await?;
        Ok(Box::new(ret))
    }

    async fn open_write_random(&self, _item: &Path) -> io::Result<Option<Box<dyn DataWriteSeek>>> {
        Ok(None)
    }
}
