//! Regulus DB - 轻量级嵌入式数据库
//!
//! 支持两种模式：
//! - 内存模式：纯内存存储，快速但重启后数据丢失
//! - 持久化模式：WAL 日志 + 快照，支持崩溃恢复

pub mod types;
pub mod storage;
pub mod query;
pub mod index;
pub mod transaction;
pub mod persistence;

pub use types::{DbValue, DataType, Column, TableSchema, SchemaError, DbError, DbResult};
pub use storage::{MemoryEngine, PersistedEngine, StorageEngine, Row, RowId};
pub use query::{QueryBuilder, UpdateBuilder, DeleteBuilder, Order, JoinType, FilterExpr};
pub use transaction::Transaction;

use std::sync::{Arc, RwLock};
use std::path::Path;

/// 数据库引擎类型（用于统一 API）
pub enum DatabaseEngine {
    /// 内存模式
    Memory(Arc<RwLock<MemoryEngine>>),
    /// 持久化模式
    Persisted(Arc<RwLock<PersistedEngine>>),
}

/// 数据库主结构
///
/// 使用 enum 包装两种引擎模式，提供统一的 API
pub struct Database {
    engine: DatabaseEngine,
}

impl Database {
    /// 创建新的内存数据库实例
    pub fn new() -> Self {
        Database {
            engine: DatabaseEngine::Memory(Arc::new(RwLock::new(MemoryEngine::new()))),
        }
    }

    /// 打开持久化数据库
    pub fn open(path: &Path) -> DbResult<Self> {
        let engine = PersistedEngine::open(path)?;
        Ok(Database {
            engine: DatabaseEngine::Persisted(Arc::new(RwLock::new(engine))),
        })
    }

    /// 创建新的持久化数据库（如果已存在则覆盖）
    pub fn create(path: &Path) -> DbResult<Self> {
        let engine = PersistedEngine::create(path)?;
        Ok(Database {
            engine: DatabaseEngine::Persisted(Arc::new(RwLock::new(engine))),
        })
    }

    /// 创建表
    pub fn create_table(&self, name: &str, columns: Vec<Column>) -> DbResult<()> {
        let schema = TableSchema::new(name, columns);
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                e.create_table(schema)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                e.create_table(schema)
            }
        }
    }

    /// 删除表
    pub fn drop_table(&self, name: &str) -> DbResult<()> {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                e.drop_table(name)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                e.drop_table(name)
            }
        }
    }

    /// 检查表是否存在
    pub fn has_table(&self, name: &str) -> bool {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let e = engine.read().unwrap();
                e.has_table(name)
            }
            DatabaseEngine::Persisted(engine) => {
                let e = engine.read().unwrap();
                e.has_table(name)
            }
        }
    }

    /// 插入数据
    pub fn insert(&self, table: &str, values: Vec<(&str, DbValue)>) -> DbResult<RowId> {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                Self::insert_with_schema(&mut *e, table, values)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                Self::insert_with_schema(&mut *e, table, values)
            }
        }
    }

    /// 插入数据的通用实现
    fn insert_with_schema<E: StorageEngine>(
        engine: &mut E,
        table: &str,
        values: Vec<(&str, DbValue)>,
    ) -> DbResult<RowId> {
        // 获取 schema 验证列
        let schema = engine.get_schema(table)?.clone();

        // 构建行
        let mut row = Row::new();
        for (name, value) in values {
            row.insert(name.to_string(), value);
        }

        // 填充默认值
        schema.fill_defaults(&mut row);

        // 验证 schema
        let values_ref: Vec<(String, DbValue)> = row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        schema.validate(&values_ref)?;

        engine.insert(table, row)
    }

    /// 根据 row_id 获取单行
    pub fn get(&self, table: &str, row_id: RowId) -> DbResult<Option<Row>> {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let e = engine.read().unwrap();
                e.get(table, row_id)
            }
            DatabaseEngine::Persisted(engine) => {
                let e = engine.read().unwrap();
                e.get(table, row_id)
            }
        }
    }

    /// 查询构建器
    ///
    /// 内存模式：返回完整的 QueryBuilder，支持所有功能
    /// 持久化模式：同样支持 QueryBuilder（通过内部 MemoryEngine 的 Arc）
    pub fn query(&self, table: &str) -> QueryBuilder {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                QueryBuilder::new(table.to_string(), Arc::clone(engine))
            }
            DatabaseEngine::Persisted(engine) => {
                // 持久化模式：使用 PersistedEngine 内部的 MemoryEngine Arc
                let e = engine.read().unwrap();
                QueryBuilder::new(table.to_string(), e.inner_arc())
            }
        }
    }

    /// 更新构建器
    ///
    /// 内存模式：返回完整的 UpdateBuilder，支持所有功能
    /// 持久化模式：同样支持 UpdateBuilder（通过内部 MemoryEngine 的 Arc）
    pub fn update(&self, table: &str) -> UpdateBuilder {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                UpdateBuilder::new(table.to_string(), Arc::clone(engine))
            }
            DatabaseEngine::Persisted(engine) => {
                // 持久化模式：使用 PersistedEngine 内部的 MemoryEngine Arc
                let e = engine.read().unwrap();
                UpdateBuilder::new(table.to_string(), e.inner_arc())
            }
        }
    }

    /// 删除构建器
    ///
    /// 内存模式：返回完整的 DeleteBuilder，支持所有功能
    /// 持久化模式：同样支持 DeleteBuilder（通过内部 MemoryEngine 的 Arc）
    pub fn delete(&self, table: &str) -> DeleteBuilder {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                DeleteBuilder::new(table.to_string(), Arc::clone(engine))
            }
            DatabaseEngine::Persisted(engine) => {
                // 持久化模式：使用 PersistedEngine 内部的 MemoryEngine Arc
                let e = engine.read().unwrap();
                DeleteBuilder::new(table.to_string(), e.inner_arc())
            }
        }
    }

    /// 执行事务
    ///
    /// 事务在两种模式下都支持：
    /// - 内存模式：支持回滚（通过 WriteLog）
    /// - 持久化模式：支持回滚 + WAL 日志标记
    pub fn transaction<F, T>(&self, f: F) -> DbResult<T>
    where
        F: FnOnce(&mut Transaction<'_>) -> DbResult<T>,
    {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                // 将 MemoryEngine 转换为 trait object
                let mut tx = Transaction::new(&mut *e as &mut dyn StorageEngine);
                let result = f(&mut tx)?;
                tx.commit()?;
                Ok(result)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                // PersistedEngine 也实现了 StorageEngine
                let mut tx = Transaction::new(&mut *e as &mut dyn StorageEngine);
                let result = f(&mut tx)?;
                tx.commit()?;
                Ok(result)
            }
        }
    }

    /// 为表列创建索引（单列，向后兼容）
    pub fn create_index(&self, table: &str, column: &str) -> DbResult<()> {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                e.create_index(table, column)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                e.create_index(table, column)
            }
        }
    }

    /// 创建复合索引
    pub fn create_composite_index(&self, table: &str, columns: &[&str]) -> DbResult<()> {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                e.create_composite_index(table, columns)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                e.create_composite_index(table, columns)
            }
        }
    }

    /// 创建唯一复合索引
    pub fn create_unique_index(&self, table: &str, columns: &[&str]) -> DbResult<()> {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                e.create_unique_index(table, columns)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                e.create_unique_index(table, columns)
            }
        }
    }

    /// 删除索引
    pub fn drop_index(&self, table: &str, column: &str) -> DbResult<bool> {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                e.drop_index(table, column)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                e.drop_index(table, column)
            }
        }
    }

    /// 删除复合索引
    pub fn drop_composite_index(&self, table: &str, columns: &[&str]) -> DbResult<bool> {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let mut e = engine.write().unwrap();
                e.drop_composite_index(table, columns)
            }
            DatabaseEngine::Persisted(engine) => {
                let mut e = engine.write().unwrap();
                e.drop_composite_index(table, columns)
            }
        }
    }

    /// 检查列是否有索引
    pub fn has_index(&self, table: &str, column: &str) -> bool {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let e = engine.read().unwrap();
                e.has_index(table, column)
            }
            DatabaseEngine::Persisted(engine) => {
                let e = engine.read().unwrap();
                e.has_index(table, column)
            }
        }
    }

    /// 检查复合索引是否存在
    pub fn has_composite_index(&self, table: &str, columns: &[&str]) -> bool {
        match &self.engine {
            DatabaseEngine::Memory(engine) => {
                let e = engine.read().unwrap();
                e.has_composite_index(table, columns)
            }
            DatabaseEngine::Persisted(engine) => {
                let e = engine.read().unwrap();
                e.has_composite_index(table, columns)
            }
        }
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table() {
        let db = Database::new();
        let columns = vec![
            Column::new("id", DataType::integer()).primary_key(),
            Column::new("name", DataType::text()),
            Column::new("age", DataType::integer()),
        ];

        assert!(db.create_table("users", columns).is_ok());
        assert!(db.has_table("users"));
    }

    #[test]
    fn test_insert_and_query() {
        let db = Database::new();
        let columns = vec![
            Column::new("id", DataType::integer()).primary_key(),
            Column::new("name", DataType::text()),
            Column::new("age", DataType::integer()),
        ];
        db.create_table("users", columns).unwrap();

        db.insert("users", vec![
            ("id", DbValue::integer(1)),
            ("name", DbValue::text("Alice")),
            ("age", DbValue::integer(25)),
        ]).unwrap();

        db.insert("users", vec![
            ("id", DbValue::integer(2)),
            ("name", DbValue::text("Bob")),
            ("age", DbValue::integer(30)),
        ]).unwrap();

        // 使用 scan 验证
        match &db.engine {
            DatabaseEngine::Memory(engine) => {
                let e = engine.read().unwrap();
                let rows = e.scan("users").unwrap();
                assert_eq!(rows.len(), 2);
            }
            _ => {}
        }
    }

    #[test]
    fn test_transaction_basic() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        let result = db.transaction(|tx| {
            tx.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))])?;
            tx.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))])?;
            Ok(())
        });

        assert!(result.is_ok());

        // 验证
        match &db.engine {
            DatabaseEngine::Memory(engine) => {
                let e = engine.read().unwrap();
                let rows = e.scan("users").unwrap();
                assert_eq!(rows.len(), 2);
            }
            _ => {}
        }
    }

    #[test]
    fn test_transaction_rollback() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        // 先插入一条数据
        db.insert("users", vec![
            ("id", DbValue::integer(1)),
            ("name", DbValue::text("Alice")),
        ]).unwrap();

        // 尝试事务插入后回滚
        let result = db.transaction(|tx| {
            tx.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))])?;
            // 手动回滚
            tx.rollback()?;
            Ok(())
        });

        assert!(result.is_ok());

        // 验证 Bob 没有被插入
        match &db.engine {
            DatabaseEngine::Memory(engine) => {
                let e = engine.read().unwrap();
                let rows = e.scan("users").unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].1.get("name").unwrap().as_text(), Some("Alice"));
            }
            _ => {}
        }
    }

    #[test]
    fn test_insert_with_default_values() {
        let db = Database::new();
        let columns = vec![
            Column::new("id", DataType::integer()).primary_key(),
            Column::new("name", DataType::text()).not_null(),
            Column::new("status", DataType::text()).default(DbValue::text("active")),
            Column::new("age", DataType::integer()).default(DbValue::integer(0)),
            Column::new("active", DataType::boolean()).default(DbValue::boolean(true)),
        ];
        db.create_table("users", columns).unwrap();

        // 只插入部分字段，依赖默认值
        db.insert("users", vec![
            ("id", DbValue::integer(1)),
            ("name", DbValue::text("Alice")),
        ]).unwrap();

        // 验证默认值已填充
        let row = db.query("users").eq("id", DbValue::integer(1)).execute().unwrap();
        assert_eq!(row.len(), 1);
        assert_eq!(row[0].get("status").unwrap().as_text(), Some("active"));
        assert_eq!(row[0].get("age").unwrap().as_integer(), Some(0));
        assert_eq!(row[0].get("active").unwrap().as_boolean(), Some(true));
    }

    #[test]
    fn test_transaction_insert_with_default_values() {
        let db = Database::new();
        let columns = vec![
            Column::new("id", DataType::integer()).primary_key(),
            Column::new("name", DataType::text()).not_null(),
            Column::new("status", DataType::text()).default(DbValue::text("active")),
        ];
        db.create_table("users", columns).unwrap();

        // 在事务中插入，依赖默认值
        db.transaction(|tx| {
            tx.insert("users", vec![
                ("id", DbValue::integer(1)),
                ("name", DbValue::text("Alice")),
            ])?;
            Ok(())
        }).unwrap();

        // 验证默认值已填充
        let row = db.query("users").eq("id", DbValue::integer(1)).execute().unwrap();
        assert_eq!(row.len(), 1);
        assert_eq!(row[0].get("status").unwrap().as_text(), Some("active"));
    }
}
