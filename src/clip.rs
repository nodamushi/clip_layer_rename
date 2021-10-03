use std::convert::TryInto;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::Path;
use std::u64;
use tempfile::tempdir;
use thiserror::Error;

#[derive(Debug)]
pub struct ClipLayer {
  pw_id: u64,
  main_id: u64,
  layer_name: String,
  layer_type: u64,
  layer_folder: u64,
  layer_next_index: u64,
  layer_first_child_index: u64,
}

#[derive(Error, Debug)]
pub enum ClipError {
  #[error("file open failed")]
  FileOpenError,
  #[error("tempolary directory create failed")]
  TmpDirError,
  #[error("file save failed")]
  FileSaveError,
  #[error("create directories failed")]
  CreateDirectoryError,
  #[error("file read failed")]
  FileReadError,
  #[error("SQLite data base operation error")]
  SQLError,
  #[error("Fail to analyze layer.May be unsupported version.")]
  UnknownFileStruct,
  #[error("unknonw file io error occured")]
  IOError,
  #[error("not a clip studio file format.")]
  NotClipFile,
}

const SQL_CHANK: &[u8; 8] = b"CHNKSQLi";
const SQL_CHANK_LEN: usize = SQL_CHANK.len();
const SQL_HEADER: &[u8; 16] = b"SQLite format 3\0";
const SQL_HEADER_LEN: usize = SQL_HEADER.len();
const SQL_HEADER_TOTAL_SIZE: usize = SQL_CHANK_LEN + 8 + SQL_HEADER_LEN;
const FOOT_CHANK_DATA: [u8; 16] = [
  0x43, 0x48, 0x4E, 0x4B, 0x46, 0x6F, 0x6F, 0x74, 0, 0, 0, 0, 0, 0, 0, 0,
];

/// Brief.
///
/// change layer name to the name of parent folder.
///
/// * `src`: input file
/// * `dst`: output file
/// * `root_layer_base_name`: top level layer name.
/// * `rename_layer`: A function that takes a layer name as an argument and decides whether to change the layer name.
pub fn create_layer_renamed_clip_file<P1: AsRef<Path>, P2: AsRef<Path>, F>(
  src: P1,
  dst: P2,
  root_layer_base_name: &str,
  rename_layer: F,
) -> Result<(), ClipError>
where
  F: Fn(&str) -> bool + Copy,
{
  let dir = match tempdir() {
    Ok(x) => x,
    Err(_) => return Err(ClipError::TmpDirError),
  };
  let dir_path = dir.path();

  let sql_pathbuf = dir_path.join("sql.sql");
  let sql_path = sql_pathbuf.as_path();
  let out_pathbuf = dir_path.join("out.clip");
  let out_path = out_pathbuf.as_path();

  let (sqlsize, index) = match find_sqlite(&src)? {
    Some(x) => x,
    None => return Err(ClipError::NotClipFile),
  };
  save_sql_only(&src, sql_path, sqlsize, index)?;
  rename_layers_in_sqlite(&sql_path, root_layer_base_name, rename_layer)?;
  concat_sql(&src, &sql_path, &out_path, index)?;

  let dst_path: &Path = dst.as_ref();
  if let Some(parent) = dst_path.parent() {
    if !parent.exists() {
      if let Err(_) = std::fs::create_dir_all(parent) {
        return Err(ClipError::CreateDirectoryError);
      }
    }
  }

  if let Err(_) = std::fs::rename(&out_path, &dst_path) {
    if let Err(_) = std::fs::copy(out_path, dst_path) {
      return Err(ClipError::FileSaveError);
    }
  }

  if let Err(_) = dir.close() {
    return Err(ClipError::IOError);
  }

  return Ok(());
}

const BUFFER_SIZE: usize = 1024;
const READ_BLOCK_SIZE: usize = SQL_HEADER_TOTAL_SIZE;
struct Buffer {
  io: BufReader<File>,
  pos: usize,
  bufidx: usize,
  bufsize: usize,
  eof: bool,
  buf: [u8; BUFFER_SIZE],
}

impl Buffer {
  fn next(&mut self) -> Result<Option<(usize, &[u8])>, ClipError> {
    if self.bufsize == 0 && !self.eof {
      self.bufsize = match self.io.read(&mut self.buf) {
        Ok(x) => x,
        Err(_) => return Err(ClipError::FileReadError),
      };
      self.eof = self.bufsize < READ_BLOCK_SIZE;
    }

    if self.bufidx + READ_BLOCK_SIZE > self.bufsize {
      if self.eof {
        return Ok(None);
      }
      //move
      let idx = self.bufidx;
      let rest = self.bufsize - idx;
      self.bufidx = 0;

      for i in 0..rest {
        self.buf[i] = self.buf[idx + i];
      }
      let read_size = match self.io.read(&mut self.buf[idx..]) {
        Ok(x) => x,
        Err(_) => return Err(ClipError::FileReadError),
      };
      self.bufsize = rest + read_size;
      self.eof = self.bufsize < READ_BLOCK_SIZE;
      if self.eof {
        return Ok(None);
      }
    }
    let idx = self.bufidx;
    let pos = self.pos;
    self.bufidx = idx + 1;
    self.pos += 1;
    return Ok(Some((pos, &self.buf[idx..idx + READ_BLOCK_SIZE])));
  }

  fn new(path: &Path) -> Result<Buffer, ClipError> {
    return Ok(Buffer {
      io: BufReader::new(match File::open(path) {
        Ok(x) => x,
        Err(_) => return Err(ClipError::FileOpenError),
      }),
      pos: 0,
      bufidx: 0,
      bufsize: 0,
      eof: false,
      buf: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
    });
  }
}

/// Brief.
///
/// find sqlite3 data size & start position
///
/// * `path`: clip file path
///
/// Return.
///
/// `(size, position)`
///
/// * `size`: sqlite3 data size
/// * `position` : sqlite3 data position in the file
fn find_sqlite<P: AsRef<Path>>(path: P) -> Result<Option<(u64, usize)>, ClipError> {
  let mut buf = Buffer::new(path.as_ref())?;
  'outer: loop {
    let (pos, data) = match buf.next()? {
      Some(x) => x,
      None => return Ok(None),
    };
    for i in 0..SQL_CHANK_LEN {
      if data[i] != SQL_CHANK[i] {
        continue 'outer;
      }
    }

    for i in 0..SQL_HEADER_LEN {
      if data[i + SQL_CHANK_LEN + 8] != SQL_HEADER[i] {
        continue 'outer;
      }
    }

    let sqlsize_buf: [u8; 8] = data[SQL_CHANK_LEN..SQL_CHANK_LEN + 8].try_into().unwrap();
    let sqlsize = u64::from_be_bytes(sqlsize_buf);
    return Ok(Some((sqlsize, pos + SQL_CHANK_LEN + 8)));
  }
}

/// Brief.
///
/// Write the sqlite3 data in the clip file to a file.
///
/// * `clip`: clip file path
/// * `splout`: output sqlite3 file path
/// * `size`: sqlite3 data size.
/// * `index`: sqlite3 data position in the clip file.
fn save_sql_only<P1: AsRef<Path>, P2: AsRef<Path>>(
  clip: P1,
  sqlout: P2,
  size: u64,
  index: usize,
) -> Result<(), ClipError> {
  let mut inf = BufReader::new(match File::open(&clip) {
    Ok(x) => x,
    Err(_) => return Err(ClipError::FileOpenError),
  });
  if let Err(_) = inf.seek(SeekFrom::Start(index as u64)) {
    return Err(ClipError::IOError);
  }

  let mut outf = BufWriter::new(match File::create(sqlout) {
    Ok(x) => x,
    Err(_) => return Err(ClipError::FileSaveError),
  });

  let mut buf: [u8; 1024] = unsafe { mem::MaybeUninit::zeroed().assume_init() };
  let mut writesize = size as usize;
  while writesize != 0 {
    let length = if writesize as usize > buf.len() {
      buf.len()
    } else {
      writesize
    };

    let read = match inf.read(&mut buf[0..length]) {
      Ok(x) => x,
      Err(_) => return Err(ClipError::FileReadError),
    };
    if let Err(_) = outf.write_all(&mut buf[0..read]) {
      return Err(ClipError::FileSaveError);
    }
    writesize -= read;
  }

  return Ok(());
}

/// Brief
///
/// Create a file that concatenates the metadata of the original file and the data of sqlite3.
///
/// * `srcclip`: the original clip file path
/// * `srcsql` : the sqlite3 file path
/// * `dstclip` : the output clip file pth
/// * `index` : the sqlite3 data position in the srclip file
fn concat_sql<P1: AsRef<Path>, P2: AsRef<Path>, P3: AsRef<Path>>(
  srcclip: P1,
  srcsql: P2,
  dstclip: P3,
  index: usize,
) -> Result<(), ClipError> {
  let mut outf = BufWriter::new(match File::create(dstclip) {
    Ok(x) => x,
    Err(_) => return Err(ClipError::FileSaveError),
  });
  let mut buf: [u8; 1024] = unsafe { mem::MaybeUninit::zeroed().assume_init() };
  {
    // Original meta data
    let mut inf = BufReader::new(match File::open(&srcclip) {
      Ok(x) => x,
      Err(_) => return Err(ClipError::FileOpenError),
    });
    let mut write_size: usize = index - 8;
    while write_size != 0 {
      let read_length = std::cmp::min(write_size, buf.len());
      let slice = &mut buf[0..read_length];

      if let Err(_) = inf.read_exact(slice) {
        return Err(ClipError::FileReadError);
      }

      if let Err(_) = outf.write_all(slice) {
        return Err(ClipError::FileSaveError);
      }
      write_size -= read_length;
    }
  }
  let mut sqlsize: u64 = 0;
  // write dummy size
  let bytes = sqlsize.to_be_bytes();
  if let Err(_) = outf.write_all(&bytes) {
    return Err(ClipError::FileSaveError);
  }

  {
    // SQLite
    let mut inf = BufReader::new(match File::open(&srcsql) {
      Ok(x) => x,
      Err(_) => return Err(ClipError::FileOpenError),
    });

    loop {
      let read_length = match inf.read(&mut buf) {
        Ok(x) => x,
        Err(_) => return Err(ClipError::FileReadError),
      };
      if read_length == 0 {
        break;
      }

      if let Err(_) = outf.write_all(&buf[0..read_length]) {
        return Err(ClipError::FileSaveError);
      }
      sqlsize += read_length as u64;
    }
  }

  if let Err(_) = outf.write_all(&FOOT_CHANK_DATA) {
    return Err(ClipError::FileSaveError);
  }

  if let Err(_) = outf.seek(SeekFrom::Start((index as u64) - 8)) {
    return Err(ClipError::FileSaveError);
  }

  let bytes = sqlsize.to_be_bytes();
  if let Err(_) = outf.write_all(&bytes) {
    return Err(ClipError::FileSaveError);
  }

  return Ok(());
}

/// Brieaf
///
/// Rename layers
///
/// * `sqlfile`: sqlite3 file path
/// * `root_layer_base_name` : top level layer base name
/// * `need_rename`: A function that takes a layer name as an argument and decides whether to change the layer name.
pub fn rename_layers_in_sqlite<P: AsRef<Path>, F>(
  sqlfile: P,
  root_layer_base_name: &str,
  need_rename: F,
) -> Result<(), ClipError>
where
  F: Fn(&str) -> bool + Copy,
{
  let conn = match rusqlite::Connection::open(sqlfile) {
    Ok(x) => x,
    Err(_) => return Err(ClipError::SQLError),
  };
  let mut v: Vec<Box<ClipLayer>> = Vec::new();
  let root_main_id = get_layers(&conn, &mut v)?;
  let root_index = match find_layer_index(&v, root_main_id) {
    Some(x) => x,
    None => panic!("FATAL: root layer not found"),
  };
  rename_layers_in_folder(
    &conn,
    &v,
    root_index,
    true,
    root_layer_base_name,
    need_rename,
  )?;
  return Ok(());
}

/// Brief
///
/// Get layer information from sqlite3 data base.
///
/// `v`: output. ClipLayer vector
///
/// Return.
///
/// root folder main_id
fn get_layers(conn: &rusqlite::Connection, v: &mut Vec<Box<ClipLayer>>) -> Result<u64, ClipError> {
  let mut stmt = match conn.prepare("SELECT _PW_ID, MainId, LayerName, LayerType, LayerFolder, LayerNextIndex, LayerFIrstChildIndex FROM Layer") {
    Ok(x) => x,
    Err(_) => return Err(ClipError::SQLError),
  };
  let layer_itr = match stmt.query_map([], |row| {
    Ok(ClipLayer {
      pw_id: row.get(0)?,
      main_id: row.get(1)?,
      layer_name: row.get(2)?,
      layer_type: row.get(3)?,
      layer_folder: row.get(4)?,
      layer_next_index: row.get(5)?,
      layer_first_child_index: row.get(6)?,
    })
  }) {
    Ok(x) => x,
    Err(_) => return Err(ClipError::SQLError),
  };
  let mut root_main_id: Option<u64> = None;
  for layer in layer_itr {
    let b = Box::new(layer.unwrap());
    if b.layer_type == 256 && b.layer_folder == 1 {
      root_main_id = Some(b.main_id);
    }
    v.push(b);
  }
  v.sort_by(|a, b| a.main_id.cmp(&b.main_id));
  match root_main_id {
    Some(x) => return Ok(x),
    None => return Err(ClipError::UnknownFileStruct),
  }
}

/// Brief
///
/// Finds the vector index of the same data as main_id.
///
/// * `v` : vector
/// * `main_id`: main_id of layer
///
/// Return.
///
/// index of `v`
fn find_layer_index(v: &Vec<Box<ClipLayer>>, main_id: u64) -> Option<usize> {
  return match v.binary_search_by_key(&main_id, |x| x.main_id) {
    Ok(x) => Some(x),
    Err(_) => None,
  };
}

/// Brief
///
/// Recursively rename layers in the folders.
///
/// * `conn`: sqlite3
/// * `v`: all layer information
/// * `index`: target folder index of `v`
/// * `root`: whether is the folder a top level folder?
/// * `root_layer_base_name` : top level layer base name
/// * `need_rename`: A function that takes a layer name as an argument and decides whether to change the layer name.
fn rename_layers_in_folder<F>(
  conn: &rusqlite::Connection,
  v: &Vec<Box<ClipLayer>>,
  index: usize,
  root: bool,
  root_layer_base_name: &str,
  need_rename: F,
) -> Result<(), ClipError>
where
  F: Fn(&str) -> bool + Copy,
{
  let f = &v[index];

  if f.layer_folder == 0 {
    return Err(ClipError::UnknownFileStruct);
  }

  let mut next = f.layer_first_child_index;
  let mut layer_number = 1;

  while next != 0 {
    let ci = match find_layer_index(v, next) {
      Some(x) => x,
      None => return Err(ClipError::UnknownFileStruct),
    };
    let c = &v[ci];
    next = c.layer_next_index;
    if c.layer_folder != 0 {
      rename_layers_in_folder(conn, v, ci, false, root_layer_base_name, need_rename)?;
    } else if (!root || root_layer_base_name.len() != 0) && need_rename(&c.layer_name) {
      let name = if root {
        format!("{} {}", root_layer_base_name, layer_number)
      } else {
        format!("{} {}", f.layer_name, layer_number)
      };
      layer_number += 1;
      rename_layer(conn, c.main_id, &name)?;
    }
  }

  return Ok(());
}

/// Brief
///
/// update layer name
///
/// * `conn` : sqlite3
/// * `main_id` : layer main_id
/// * `rename` : new layer name
fn rename_layer(conn: &rusqlite::Connection, main_id: u64, rename: &str) -> Result<(), ClipError> {
  if let Err(_) = conn.execute(
    "UPDATE Layer SET LayerName = $1 WHERE MainId = $2",
    rusqlite::params![rename, main_id],
  ) {
    return Err(ClipError::SQLError);
  }
  return Ok(());
}
