use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use crate::storage::{MemoryEngine, SerializableEngineData};
use crate::types::{DbResult, DbError};

/// 快照管理器
pub struct SnapshotManager {
    base_path: std::path::PathBuf,
}

impl SnapshotManager {
    pub fn new(base_path: &Path) -> Self {
        SnapshotManager {
            base_path: base_path.to_path_buf(),
        }
    }

    /// 获取快照文件路径
    fn snapshot_path(&self) -> std::path::PathBuf {
        self.base_path.join("data.rdb")
    }

    /// 保存快照
    pub fn save(&self, engine: &MemoryEngine) -> DbResult<std::path::PathBuf> {
        let path = self.snapshot_path();
        let file = File::create(&path)
            .map_err(|e| DbError::IoError(e))?;
        let mut writer = BufWriter::new(file);

        // 使用 engine 提供的 serialize 方法
        let data = engine.serialize()
            .map_err(|e| DbError::InternalError(format!("Serialization error: {}", e)))?;

        bincode::serialize_into(&mut writer, &data)
            .map_err(|e| DbError::InternalError(format!("Bincode error: {}", e)))?;

        writer.flush()
            .map_err(|e| DbError::IoError(e))?;

        Ok(path)
    }

    /// 加载快照
    pub fn load(&self) -> DbResult<Option<MemoryEngine>> {
        let path = self.snapshot_path();

        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(&path)
            .map_err(|e| DbError::IoError(e))?;
        let reader = BufReader::new(file);

        let data: SerializableEngineData = bincode::deserialize_from(reader)
            .map_err(|e| DbError::InternalError(format!("Bincode error: {}", e)))?;

        // 使用 MemoryEngine::deserialize 恢复
        let engine = MemoryEngine::deserialize(data);
        Ok(Some(engine))
    }

    /// 检查快照是否存在
    pub fn exists(&self) -> bool {
        self.snapshot_path().exists()
    }
}
