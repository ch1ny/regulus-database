use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use crate::storage::Row;
use crate::types::{TableSchema, DbResult, DbError};

/// WAL 操作类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    CreateTable { schema: TableSchema },
    DropTable { name: String },
    Insert { table: String, row_id: u64, row: Row },
    Update { table: String, row_id: u64, row: Row },
    Delete { table: String, row_id: u64 },
}

impl WalOperation {
    /// 转换为没有 row_id 包装的版本（用于序列化到 WAL）
    pub fn to_serializable(&self) -> SerializableWalOperation {
        match self {
            WalOperation::CreateTable { schema } => SerializableWalOperation::CreateTable { schema: schema.clone() },
            WalOperation::DropTable { name } => SerializableWalOperation::DropTable { name: name.clone() },
            WalOperation::Insert { table, row_id, row } => SerializableWalOperation::Insert {
                table: table.clone(),
                row_id: *row_id,
                row: row.0.clone(),
            },
            WalOperation::Update { table, row_id, row } => SerializableWalOperation::Update {
                table: table.clone(),
                row_id: *row_id,
                row: row.0.clone(),
            },
            WalOperation::Delete { table, row_id } => SerializableWalOperation::Delete {
                table: table.clone(),
                row_id: *row_id,
            },
        }
    }

    pub fn from_serializable(op: SerializableWalOperation) -> Self {
        match op {
            SerializableWalOperation::CreateTable { schema } => WalOperation::CreateTable { schema },
            SerializableWalOperation::DropTable { name } => WalOperation::DropTable { name },
            SerializableWalOperation::Insert { table, row_id, row } => WalOperation::Insert {
                table,
                row_id,
                row: Row(row),
            },
            SerializableWalOperation::Update { table, row_id, row } => WalOperation::Update {
                table,
                row_id,
                row: Row(row),
            },
            SerializableWalOperation::Delete { table, row_id } => WalOperation::Delete {
                table,
                row_id,
            },
        }
    }
}

/// 可序列化的 WAL 操作
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableWalOperation {
    CreateTable { schema: TableSchema },
    DropTable { name: String },
    Insert { table: String, row_id: u64, row: IndexMap<String, crate::types::DbValue> },
    Update { table: String, row_id: u64, row: IndexMap<String, crate::types::DbValue> },
    Delete { table: String, row_id: u64 },
}

use indexmap::IndexMap;

/// WAL 管理器
pub struct WalManager {
    file_path: PathBuf,
    file: Option<BufWriter<File>>,
    lsn: u64,  // Log Sequence Number
}

impl WalManager {
    /// 创建或打开 WAL 文件
    pub fn new(path: &Path) -> DbResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|e| DbError::IoError(e))?;

        Ok(WalManager {
            file_path: path.to_path_buf(),
            file: Some(BufWriter::new(file)),
            lsn: 0,
        })
    }

    /// 追加操作到 WAL
    pub fn append(&mut self, op: WalOperation) -> DbResult<u64> {
        let writer = self.file.as_mut().ok_or_else(|| {
            DbError::InternalError("WAL file not opened".to_string())
        })?;

        // 转换为可序列化的格式
        let serializable_op = op.to_serializable();

        // 序列化操作
        let data = bincode::serialize(&serializable_op)
            .map_err(|e| DbError::InternalError(format!("Serialization error: {}", e)))?;

        // 写入格式：[长度 4 字节][数据]
        let len = data.len() as u32;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&data)?;
        writer.flush()?;

        let current_lsn = self.lsn;
        self.lsn += 1;

        Ok(current_lsn)
    }

    /// 重放所有 WAL 操作
    pub fn replay<F>(&mut self, mut apply: F) -> DbResult<()>
    where
        F: FnMut(WalOperation) -> DbResult<()>,
    {
        // 关闭当前的写入器
        self.file = None;

        // 重新打开文件用于读取
        let file = File::open(&self.file_path)
            .map_err(|e| DbError::IoError(e))?;
        let mut reader = BufReader::new(file);

        // 读取并重放所有操作
        loop {
            // 读取长度
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(DbError::IoError(e)),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // 读取数据
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;

            // 反序列化为可序列化类型
            let serializable_op: SerializableWalOperation = bincode::deserialize(&data)
                .map_err(|e| DbError::InternalError(format!("Deserialization error: {}", e)))?;

            // 转换为 WalOperation 并应用
            let op = WalOperation::from_serializable(serializable_op);
            apply(op)?;
        }

        // 重新打开写入器
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.file_path)
            .map_err(|e| DbError::IoError(e))?;
        self.file = Some(BufWriter::new(file));

        Ok(())
    }

    /// 截断 WAL 文件
    pub fn truncate(&mut self) -> DbResult<()> {
        // 刷新缓冲区
        if let Some(writer) = &mut self.file {
            writer.flush()?;
        }

        // 关闭文件
        self.file = None;

        // 截断并重新创建
        let file = File::create(&self.file_path)
            .map_err(|e| DbError::IoError(e))?;

        // 重新打开写入器
        self.file = Some(BufWriter::new(file));
        self.lsn = 0;

        Ok(())
    }

    /// 获取 WAL 文件大小（字节数）
    pub fn size(&self) -> DbResult<u64> {
        let metadata = std::fs::metadata(&self.file_path)
            .map_err(|e| DbError::IoError(e))?;
        Ok(metadata.len())
    }
}
