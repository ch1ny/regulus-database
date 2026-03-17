pub mod wal;
pub mod snapshot;

use std::path::Path;
use crate::storage::{MemoryEngine, StorageEngine, RowId};
use crate::persistence::wal::{WalManager, WalOperation};
use crate::persistence::snapshot::SnapshotManager;
use crate::types::{DbResult, DbError};

/// 持久化管理器
pub struct PersistenceManager {
    wal: WalManager,
    snapshot: SnapshotManager,
    checkpoint_threshold: usize,  // WAL 文件大小阈值（字节）
    wal_size: u64,
}

impl PersistenceManager {
    pub fn new(base_path: &Path) -> DbResult<Self> {
        // 确保目录存在
        std::fs::create_dir_all(base_path)
            .map_err(|e| DbError::IoError(e))?;

        let wal_path = base_path.join("data.rdb.wal");
        let wal = WalManager::new(&wal_path)?;
        let snapshot = SnapshotManager::new(base_path);

        Ok(PersistenceManager {
            wal,
            snapshot,
            checkpoint_threshold: 10 * 1024 * 1024, // 10MB
            wal_size: 0,
        })
    }

    /// 恢复数据库
    pub fn restore(&mut self) -> DbResult<MemoryEngine> {
        // 1. 加载快照
        let mut engine = if let Some(e) = self.snapshot.load()? {
            e
        } else {
            MemoryEngine::new()
        };

        // 2. 重放 WAL
        self.wal.replay(|op| {
            Self::apply_operation(&mut engine, op)
        })?;

        // 3. 更新 WAL 大小
        self.wal_size = self.wal.size()?;

        Ok(engine)
    }

    /// 应用单个 WAL 操作
    fn apply_operation(engine: &mut MemoryEngine, op: WalOperation) -> DbResult<()> {
        match op {
            WalOperation::CreateTable { schema } => {
                engine.create_table(schema)?;
            }
            WalOperation::DropTable { name } => {
                engine.drop_table(&name)?;
            }
            WalOperation::Insert { table, row_id, row } => {
                // 直接插入到指定 row_id（用于恢复）
                engine.insert_restored(&table, RowId(row_id), row)?;
            }
            WalOperation::Update { table, row_id, row } => {
                engine.update(&table, RowId(row_id), row)?;
            }
            WalOperation::Delete { table, row_id } => {
                engine.delete(&table, RowId(row_id))?;
            }
        }
        Ok(())
    }

    /// 记录操作到 WAL
    pub fn log_operation(&mut self, op: WalOperation) -> DbResult<u64> {
        let lsn = self.wal.append(op)?;
        self.wal_size = self.wal.size()?;
        Ok(lsn)
    }

    /// 检查是否需要 checkpoint
    pub fn needs_checkpoint(&self) -> bool {
        self.wal_size > self.checkpoint_threshold as u64
    }

    /// 执行 checkpoint
    pub fn checkpoint(&mut self, engine: &MemoryEngine) -> DbResult<()> {
        // 1. 保存快照
        self.snapshot.save(engine)?;

        // 2. 截断 WAL
        self.wal.truncate()?;
        self.wal_size = 0;

        Ok(())
    }

    /// 获取 WAL 大小
    pub fn wal_size(&self) -> u64 {
        self.wal_size
    }
}
