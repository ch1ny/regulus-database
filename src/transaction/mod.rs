//! 事务模块 - 泛型实现，支持 MemoryEngine 和 PersistedEngine
//!
//! 设计策略：
//! 1. 使用 trait object (&'a mut dyn StorageEngine) 实现代码复用
//! 2. 事务操作直接执行到引擎（简化实现）
//! 3. PersistedEngine 内部处理事务 WAL 标记
//! 4. 回滚功能：通过 WriteSet 记录已执行的操作，rollback 时反向操作

use crate::storage::{StorageEngine, Row, RowId};
use crate::types::{DbValue, DbResult, DbError};

/// 写操作日志（用于回滚）
#[derive(Debug)]
pub enum WriteLog {
    /// 插入前的状态（回滚时需要删除）
    Inserted { table: String, row_id: RowId },
    /// 更新前的状态（回滚时需要恢复）
    Updated { table: String, row_id: RowId, old_row: Row },
    /// 删除前的状态（回滚时需要重新插入）
    Deleted { table: String, row_id: RowId, row: Row },
}

/// 事务包装器 - 使用 trait object 实现泛型
pub struct Transaction<'a> {
    engine: &'a mut dyn StorageEngine,
    write_log: Vec<WriteLog>,
    is_committed: bool,
    /// 是否启用回滚日志（MemoryEngine 可以禁用，PersistedEngine 启用）
    enable_rollback_log: bool,
}

impl<'a> Transaction<'a> {
    /// 创建新事务（默认启用回滚日志）
    pub fn new(engine: &'a mut dyn StorageEngine) -> Self {
        Transaction {
            engine,
            write_log: Vec::new(),
            is_committed: false,
            enable_rollback_log: true,
        }
    }

    /// 创建新事务（带回滚日志开关）
    pub fn with_rollback_log(engine: &'a mut dyn StorageEngine, enable_log: bool) -> Self {
        Transaction {
            engine,
            write_log: Vec::new(),
            is_committed: false,
            enable_rollback_log: enable_log,
        }
    }

    /// 提交事务
    pub fn commit(&mut self) -> DbResult<()> {
        if self.is_committed {
            return Err(DbError::TransactionError(
                "Transaction already committed".to_string()
            ));
        }
        // 清空日志（提交后不再需要回滚）
        self.write_log.clear();
        self.is_committed = true;
        Ok(())
    }

    /// 回滚事务
    ///
    /// 反向应用所有写操作：
    /// - Inserted: 删除该行
    /// - Updated: 恢复旧值
    /// - Deleted: 重新插入该行
    pub fn rollback(&mut self) -> DbResult<()> {
        if self.is_committed {
            return Err(DbError::TransactionError(
                "Cannot rollback a committed transaction".to_string()
            ));
        }

        if !self.enable_rollback_log {
            self.write_log.clear();
            return Ok(());
        }

        // 反向遍历日志
        for log in self.write_log.drain(..).rev() {
            match log {
                WriteLog::Inserted { table, row_id } => {
                    // 删除插入的行
                    let _ = self.engine.delete(&table, row_id);
                }
                WriteLog::Updated { table, row_id, old_row } => {
                    // 恢复旧值
                    let _ = self.engine.update(&table, row_id, old_row);
                }
                WriteLog::Deleted { table, row_id: _, row } => {
                    // 重新插入删除的行
                    let _ = self.engine.insert(&table, row);
                }
            }
        }

        Ok(())
    }

    /// 插入数据
    pub fn insert(&mut self, table: &str, values: Vec<(&str, DbValue)>) -> DbResult<RowId> {
        // 获取 schema
        let schema = self.engine.get_schema(table)?.clone();

        // 构建行
        let mut row = Row::new();
        for (name, value) in values {
            row.insert(name.to_string(), value);
        }

        // 填充默认值
        schema.fill_defaults(&mut row);

        // 执行插入
        let row_id = self.engine.insert(table, row.clone())?;

        // 记录日志（用于回滚）
        if self.enable_rollback_log {
            self.write_log.push(WriteLog::Inserted {
                table: table.to_string(),
                row_id,
            });
        }

        Ok(row_id)
    }

    /// 获取引擎的不可变引用（用于查询等操作）
    pub fn engine(&self) -> &dyn StorageEngine {
        self.engine
    }

    /// 获取引擎的可变引用
    pub fn engine_mut(&mut self) -> &mut dyn StorageEngine {
        self.engine
    }

    /// 查询表中所有行（简化查询接口）
    pub fn query_all(&self, table: &str) -> DbResult<Vec<Row>> {
        let rows = self.engine.scan(table)?;
        Ok(rows.into_iter().map(|(_, row)| row.clone()).collect())
    }

    /// 直接访问底层引擎执行操作（不记录回滚日志）
    /// 适用于查询等只读操作
    pub fn with_engine<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dyn StorageEngine) -> R,
    {
        f(self.engine)
    }

    /// 直接访问底层引擎执行操作（可变）
    pub fn with_engine_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut dyn StorageEngine) -> R,
    {
        f(self.engine)
    }

    /// 更新操作（带条件）
    pub fn update<F>(&mut self, table: &str, condition: F, updates: Vec<(&str, DbValue)>) -> DbResult<usize>
    where
        F: Fn(&Row) -> bool,
    {
        // 获取 schema 验证列名
        let schema = self.engine.get_schema(table)?;

        // 构建更新值
        let mut new_values = Row::new();
        for (name, value) in updates {
            new_values.insert(name.to_string(), value);
        }
        schema.validate(&new_values.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>())?;

        // 扫描全表，找到匹配的行并克隆出来（避免借用冲突）
        let rows = self.engine.scan(table)?;
        let matching_rows: Vec<(RowId, Row)> = rows
            .into_iter()
            .filter(|(_, row)| condition(row))
            .map(|(row_id, row)| (row_id, row.clone()))
            .collect();

        let mut updated_count = 0;

        for (row_id, old_row) in matching_rows {
            // 记录旧值（用于回滚）
            if self.enable_rollback_log {
                self.write_log.push(WriteLog::Updated {
                    table: table.to_string(),
                    row_id,
                    old_row: old_row.clone(),
                });
            }

            // 合并新旧值
            let mut updated_row = old_row.clone();
            for (key, value) in new_values.iter() {
                updated_row.insert(key.clone(), value.clone());
            }

            // 执行更新
            self.engine.update(table, row_id, updated_row)?;
            updated_count += 1;
        }

        Ok(updated_count)
    }

    /// 删除操作（带条件）
    pub fn delete<F>(&mut self, table: &str, condition: F) -> DbResult<usize>
    where
        F: Fn(&Row) -> bool,
    {
        // 扫描全表，找到匹配的行并克隆（避免借用冲突）
        let rows = self.engine.scan(table)?;
        let rows_to_delete: Vec<(RowId, Row)> = rows
            .into_iter()
            .filter(|(_, row)| condition(row))
            .map(|(row_id, row)| (row_id, row.clone()))
            .collect();

        let mut deleted_count = 0;

        for (row_id, row) in rows_to_delete {
            // 记录旧值（用于回滚）- 保存完整的行数据
            if self.enable_rollback_log {
                self.write_log.push(WriteLog::Deleted {
                    table: table.to_string(),
                    row_id,
                    row,
                });
            }

            self.engine.delete(table, row_id)?;
            deleted_count += 1;
        }

        Ok(deleted_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryEngine;
    use crate::types::{DataType, Column, TableSchema};

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                Column::new("id", DataType::integer()).primary_key(),
                Column::new("name", DataType::text()),
            ],
        )
    }

    #[test]
    fn test_transaction_insert() {
        let mut engine = MemoryEngine::new();
        engine.create_table(create_test_schema()).unwrap();

        let mut tx = Transaction::new(&mut engine);
        let row_id = tx.insert("users", vec![
            ("id", DbValue::integer(1)),
            ("name", DbValue::text("Alice")),
        ]).unwrap();

        assert_eq!(row_id.0, 0);

        tx.commit().unwrap();

        // 验证提交后数据存在
        let rows = engine.scan("users").unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_transaction_rollback() {
        let mut engine = MemoryEngine::new();
        engine.create_table(create_test_schema()).unwrap();

        {
            let mut tx = Transaction::new(&mut engine);
            tx.insert("users", vec![
                ("id", DbValue::integer(1)),
                ("name", DbValue::text("Alice")),
            ]).unwrap();

            // 回滚
            tx.rollback().unwrap();
        }

        // 验证回滚后数据不存在
        let rows = engine.scan("users").unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_transaction_update_rollback() {
        let mut engine = MemoryEngine::new();
        engine.create_table(create_test_schema()).unwrap();

        // 先插入一条数据
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::integer(1));
        row.insert("name".to_string(), DbValue::text("Alice"));
        engine.insert("users", row).unwrap();

        {
            let mut tx = Transaction::new(&mut engine);

            // 更新
            tx.update(
                "users",
                |row| row.get("id").and_then(|v| v.as_integer()) == Some(1),
                vec![("name", DbValue::text("Bob"))],
            ).unwrap();

            // 回滚
            tx.rollback().unwrap();
        }

        // 验证回滚后数据恢复
        let rows = engine.scan("users").unwrap();
        assert_eq!(rows[0].1.get("name").unwrap().as_text(), Some("Alice"));
    }

    #[test]
    fn test_transaction_delete_rollback() {
        let mut engine = MemoryEngine::new();
        engine.create_table(create_test_schema()).unwrap();

        // 先插入一条数据
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::integer(1));
        row.insert("name".to_string(), DbValue::text("Alice"));
        engine.insert("users", row).unwrap();

        {
            let mut tx = Transaction::new(&mut engine);

            // 删除
            tx.delete(
                "users",
                |row| row.get("id").and_then(|v| v.as_integer()) == Some(1),
            ).unwrap();

            // 回滚
            tx.rollback().unwrap();
        }

        // 验证回滚后数据恢复
        let rows = engine.scan("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.get("name").unwrap().as_text(), Some("Alice"));
    }

    #[test]
    fn test_transaction_insert_with_default_values_rollback() {
        let mut engine = MemoryEngine::new();
        let schema = TableSchema::new(
            "users",
            vec![
                Column::new("id", DataType::integer()).primary_key(),
                Column::new("name", DataType::text()).not_null(),
                Column::new("status", DataType::text()).default(DbValue::text("active")),
                Column::new("age", DataType::integer()).default(DbValue::integer(0)),
                Column::new("active", DataType::boolean()).default(DbValue::boolean(true)),
            ],
        );
        engine.create_table(schema).unwrap();

        {
            let mut tx = Transaction::new(&mut engine);
            // 插入时依赖默认值
            tx.insert("users", vec![
                ("id", DbValue::integer(1)),
                ("name", DbValue::text("Alice")),
            ]).unwrap();

            // 回滚
            tx.rollback().unwrap();
        }

        // 验证回滚后数据不存在（包括默认值填充的字段）
        let rows = engine.scan("users").unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_transaction_insert_with_default_values_commit() {
        let mut engine = MemoryEngine::new();
        let schema = TableSchema::new(
            "users",
            vec![
                Column::new("id", DataType::integer()).primary_key(),
                Column::new("name", DataType::text()).not_null(),
                Column::new("status", DataType::text()).default(DbValue::text("active")),
                Column::new("age", DataType::integer()).default(DbValue::integer(0)),
            ],
        );
        engine.create_table(schema).unwrap();

        {
            let mut tx = Transaction::new(&mut engine);
            // 插入时依赖默认值
            tx.insert("users", vec![
                ("id", DbValue::integer(1)),
                ("name", DbValue::text("Alice")),
            ]).unwrap();

            // 提交
            tx.commit().unwrap();
        }

        // 验证提交后默认值已正确填充
        let rows = engine.scan("users").unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0].1;
        assert_eq!(row.get("status").unwrap().as_text(), Some("active"));
        assert_eq!(row.get("age").unwrap().as_integer(), Some(0));
    }
}
