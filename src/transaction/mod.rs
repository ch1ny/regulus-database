use crate::storage::{MemoryEngine, StorageEngine};
use crate::types::{DbValue, DbResult, DbError};
use std::sync::{Arc, RwLock};

/// 事务包装器
pub struct Transaction {
    engine: Arc<RwLock<MemoryEngine>>,
}

impl Transaction {
    pub fn new(engine: Arc<RwLock<MemoryEngine>>) -> Self {
        Transaction { engine }
    }

    /// 提交事务（内存引擎中是 no-op）
    pub fn commit(&mut self) -> DbResult<()> {
        // 内存引擎自动提交，这里预留 WAL 日志接口
        Ok(())
    }

    /// 回滚事务（内存引擎中暂不支持）
    pub fn rollback(&mut self) -> DbResult<()> {
        Err(DbError::TransactionError("Rollback not supported in memory engine".to_string()))
    }

    /// 插入数据
    pub fn insert(&mut self, table: &str, values: Vec<(&str, DbValue)>) -> DbResult<crate::storage::RowId> {
        let mut engine = self.engine.write().unwrap();

        let mut row = crate::storage::Row::new();
        for (name, value) in values {
            row.insert(name.to_string(), value);
        }

        engine.insert(table, row)
    }

    /// 查询
    pub fn query(&self, table: &str) -> crate::query::QueryBuilder {
        crate::query::QueryBuilder::new(table.to_string(), Arc::clone(&self.engine))
    }

    /// 更新
    pub fn update(&self, table: &str) -> crate::query::UpdateBuilder {
        crate::query::UpdateBuilder::new(table.to_string(), Arc::clone(&self.engine))
    }

    /// 删除
    pub fn delete(&self, table: &str) -> crate::query::DeleteBuilder {
        crate::query::DeleteBuilder::new(table.to_string(), Arc::clone(&self.engine))
    }
}
