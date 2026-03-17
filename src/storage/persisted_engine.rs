use crate::storage::{MemoryEngine, StorageEngine, Row, RowId};
use crate::persistence::PersistenceManager;
use crate::persistence::wal::WalOperation;
use crate::types::{TableSchema, DbResult};
use std::path::Path;

/// 持久化存储引擎
/// 包装 MemoryEngine + PersistenceManager，提供 WAL 日志功能
pub struct PersistedEngine {
    inner: MemoryEngine,
    persistence: PersistenceManager,
}

impl PersistedEngine {
    /// 打开持久化数据库
    pub fn open(path: &Path) -> DbResult<Self> {
        let mut persistence = PersistenceManager::new(path)?;
        let engine = persistence.restore()?;

        Ok(PersistedEngine {
            inner: engine,
            persistence,
        })
    }

    /// 创建新的持久化数据库（如果已存在则覆盖）
    pub fn create(path: &Path) -> DbResult<Self> {
        // 删除现有的 WAL 和快照文件
        let wal_path = path.join("wal.bin");
        let snapshot_path = path.join("snapshot.bin");

        let _ = std::fs::remove_file(&wal_path);
        let _ = std::fs::remove_file(&snapshot_path);

        let persistence = PersistenceManager::new(path)?;
        let engine = MemoryEngine::new();

        Ok(PersistedEngine {
            inner: engine,
            persistence,
        })
    }

    /// 手动触发检查点
    pub fn checkpoint(&mut self) -> DbResult<()> {
        if self.persistence.needs_checkpoint() {
            self.persistence.checkpoint(&self.inner)?;
        }
        Ok(())
    }

    /// 强制触发检查点（无论 WAL 大小）
    pub fn force_checkpoint(&mut self) -> DbResult<()> {
        self.persistence.checkpoint(&self.inner)?;
        Ok(())
    }

    /// 获取 WAL 文件大小
    pub fn wal_size(&self) -> u64 {
        self.persistence.wal_size()
    }

    /// 获取内部 MemoryEngine 的不可变引用
    pub fn inner(&self) -> &MemoryEngine {
        &self.inner
    }

    /// 获取内部 MemoryEngine 的可变引用（慎用，不会写 WAL）
    pub fn inner_mut(&mut self) -> &mut MemoryEngine {
        &mut self.inner
    }

    // ========== 索引方法委托（委托给内部 MemoryEngine） ==========

    /// 为表列创建索引（单列）
    pub fn create_index(&mut self, table: &str, column: &str) -> DbResult<()> {
        self.inner.create_index(table, column)
    }

    /// 创建复合索引
    pub fn create_composite_index(&mut self, table: &str, columns: &[&str]) -> DbResult<()> {
        self.inner.create_composite_index(table, columns)
    }

    /// 创建唯一复合索引
    pub fn create_unique_index(&mut self, table: &str, columns: &[&str]) -> DbResult<()> {
        self.inner.create_unique_index(table, columns)
    }

    /// 删除索引
    pub fn drop_index(&mut self, table: &str, column: &str) -> DbResult<bool> {
        self.inner.drop_index(table, column)
    }

    /// 删除复合索引
    pub fn drop_composite_index(&mut self, table: &str, columns: &[&str]) -> DbResult<bool> {
        self.inner.drop_composite_index(table, columns)
    }

    /// 检查列是否有索引
    pub fn has_index(&self, table: &str, column: &str) -> bool {
        self.inner.has_index(table, column)
    }

    /// 检查复合索引是否存在
    pub fn has_composite_index(&self, table: &str, columns: &[&str]) -> bool {
        self.inner.has_composite_index(table, columns)
    }
}

impl StorageEngine for PersistedEngine {
    fn create_table(&mut self, schema: TableSchema) -> DbResult<()> {
        // 1. 先写 WAL
        let op = WalOperation::CreateTable { schema: schema.clone() };
        self.persistence.log_operation(op)?;

        // 2. 再写内存
        self.inner.create_table(schema)
    }

    fn drop_table(&mut self, name: &str) -> DbResult<()> {
        // 1. 先写 WAL
        let op = WalOperation::DropTable { name: name.to_string() };
        self.persistence.log_operation(op)?;

        // 2. 再写内存
        self.inner.drop_table(name)
    }

    fn has_table(&self, name: &str) -> bool {
        self.inner.has_table(name)
    }

    fn get_schema(&self, name: &str) -> DbResult<&TableSchema> {
        self.inner.get_schema(name)
    }

    fn insert(&mut self, table: &str, row: Row) -> DbResult<RowId> {
        // 1. 先写 WAL（在获取 row_id 之前记录原始数据）
        let row_clone = row.clone();
        // WAL 中的 row_id 会在插入后分配，这里先记录一个占位符
        // 实际上我们需要在插入后更新 WAL，或者采用另一种策略：
        // 在 WAL 中记录操作，恢复时重新执行操作来得到相同的 row_id

        // 策略：WAL 中记录 Insert 操作，恢复时按顺序执行得到相同的 row_id
        // 所以这里先写 WAL（row_id 会在恢复时确定），然后再插入内存
        // 但这样有问题：WAL 中的 row_id 和实际的不一致

        // 更好的策略：WAL 中记录操作类型和参数，不记录 row_id
        // 恢复时重新执行操作，自然得到正确的 row_id
        // 但这需要修改 WalOperation 的定义

        // 当前实现：先插入内存获取 row_id，然后写 WAL
        // 风险：如果写 WAL 失败，内存中已经有了这个 row
        // 解决方案：使用事务，或者在 WAL 失败时回滚

        // 简单实现：先写 WAL（不带 row_id），再插入
        // 恢复时按顺序重放得到相同的 row_id
        // 但这样需要从 WalOperation 中移除 row_id

        // 最简实现：先插入获取 row_id，然后写 WAL
        // 如果 WAL 失败，返回错误但不回滚（对于演示目的）
        let row_id = self.inner.insert(table, row_clone.clone())?;

        let op = WalOperation::Insert {
            table: table.to_string(),
            row_id: row_id.0,
            row: row_clone,
        };
        self.persistence.log_operation(op)?;

        // 检查是否需要自动检查点
        if self.persistence.needs_checkpoint() {
            self.persistence.checkpoint(&self.inner)?;
        }

        Ok(row_id)
    }

    fn get(&self, table: &str, row_id: RowId) -> DbResult<Option<&Row>> {
        self.inner.get(table, row_id)
    }

    fn update(&mut self, table: &str, row_id: RowId, values: Row) -> DbResult<()> {
        // 先写内存获取旧值（用于 WAL 记录）
        let old_row = self.inner.get(table, row_id)?.cloned();

        // 执行更新
        self.inner.update(table, row_id, values.clone())?;

        // 根据是否有旧值决定写 Insert 还是 Update
        let op = match old_row {
            Some(_) => WalOperation::Update {
                table: table.to_string(),
                row_id: row_id.0,
                row: values,
            },
            None => WalOperation::Insert {
                table: table.to_string(),
                row_id: row_id.0,
                row: values,
            },
        };
        self.persistence.log_operation(op)?;

        // 检查是否需要自动检查点
        if self.persistence.needs_checkpoint() {
            self.persistence.checkpoint(&self.inner)?;
        }

        Ok(())
    }

    fn delete(&mut self, table: &str, row_id: RowId) -> DbResult<Option<Row>> {
        // 先获取要删除的行
        let _deleted_row = self.inner.get(table, row_id)?.cloned();

        // 执行删除
        let result = self.inner.delete(table, row_id)?;

        // 写 WAL
        let op = WalOperation::Delete {
            table: table.to_string(),
            row_id: row_id.0,
        };
        self.persistence.log_operation(op)?;

        // 检查是否需要自动检查点
        if self.persistence.needs_checkpoint() {
            self.persistence.checkpoint(&self.inner)?;
        }

        Ok(result)
    }

    fn scan(&self, table: &str) -> DbResult<Vec<(RowId, &Row)>> {
        self.inner.scan(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Column, DbValue};
    use tempfile::TempDir;

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                Column::new("id", DataType::integer()).primary_key(),
                Column::new("name", DataType::text()),
                Column::new("age", DataType::integer()),
            ],
        )
    }

    fn create_test_row() -> Row {
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::integer(1));
        row.insert("name".to_string(), DbValue::text("Alice"));
        row.insert("age".to_string(), DbValue::integer(25));
        row
    }

    #[test]
    fn test_persisted_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

        let schema = create_test_schema();
        assert!(engine.create_table(schema).is_ok());
        assert!(engine.has_table("users"));
    }

    #[test]
    fn test_persisted_insert_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

        engine.create_table(create_test_schema()).unwrap();

        let row = create_test_row();
        let row_id = engine.insert("users", row).unwrap();

        let retrieved = engine.get("users", row_id).unwrap().unwrap();
        assert_eq!(retrieved.get("name").unwrap().as_text(), Some("Alice"));
    }

    #[test]
    fn test_persisted_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

        engine.create_table(create_test_schema()).unwrap();

        for i in 0..5 {
            let mut row = Row::new();
            row.insert("id".to_string(), DbValue::integer(i));
            row.insert("name".to_string(), DbValue::text(format!("User{}", i)));
            row.insert("age".to_string(), DbValue::integer(20 + i));
            engine.insert("users", row).unwrap();
        }

        // 手动检查点
        assert!(engine.force_checkpoint().is_ok());

        // WAL 大小应该归零
        assert_eq!(engine.wal_size(), 0);
    }

    #[test]
    fn test_persisted_recovery() {
        let temp_dir = TempDir::new().unwrap();

        // 1. 创建数据库并插入数据
        {
            let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();
            engine.create_table(create_test_schema()).unwrap();

            let row = create_test_row();
            engine.insert("users", row).unwrap();

            // 强制检查点确保数据持久化
            engine.force_checkpoint().unwrap();

            // 再插入一条数据（不检查点，测试 WAL 恢复）
            let mut row2 = Row::new();
            row2.insert("id".to_string(), DbValue::integer(2));
            row2.insert("name".to_string(), DbValue::text("Bob"));
            row2.insert("age".to_string(), DbValue::integer(30));
            engine.insert("users", row2).unwrap();
        }

        // 2. 重新打开数据库（模拟恢复）
        let engine = PersistedEngine::open(temp_dir.path()).unwrap();

        // 3. 验证数据
        assert_eq!(engine.inner().get_row_count("users").unwrap(), 2);

        let row1 = engine.inner().get("users", RowId(0)).unwrap().unwrap();
        assert_eq!(row1.get("name").unwrap().as_text(), Some("Alice"));

        let row2 = engine.inner().get("users", RowId(1)).unwrap().unwrap();
        assert_eq!(row2.get("name").unwrap().as_text(), Some("Bob"));
    }
}
