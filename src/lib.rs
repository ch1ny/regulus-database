pub mod types;
pub mod storage;
pub mod query;
pub mod index;
pub mod transaction;
pub mod persistence;

pub use types::{DbValue, DataType, Column, TableSchema, DbError, DbResult};
pub use storage::{MemoryEngine, PersistedEngine, StorageEngine, Row, RowId};
pub use query::{QueryBuilder, UpdateBuilder, DeleteBuilder, Order, JoinType};
pub use transaction::Transaction;

use std::sync::{Arc, RwLock};

/// 数据库主结构（纯内存模式）
pub struct Database {
    engine: Arc<RwLock<MemoryEngine>>,
}

impl Database {
    /// 创建新的数据库实例
    pub fn new() -> Self {
        Database {
            engine: Arc::new(RwLock::new(MemoryEngine::new())),
        }
    }

    /// 创建表
    pub fn create_table(&self, name: &str, columns: Vec<Column>) -> DbResult<()> {
        let schema = TableSchema::new(name, columns);
        let mut engine = self.engine.write().unwrap();
        engine.create_table(schema)
    }

    /// 删除表
    pub fn drop_table(&self, name: &str) -> DbResult<()> {
        let mut engine = self.engine.write().unwrap();
        engine.drop_table(name)
    }

    /// 检查表是否存在
    pub fn has_table(&self, name: &str) -> bool {
        let engine = self.engine.read().unwrap();
        engine.has_table(name)
    }

    /// 插入数据
    pub fn insert(&self, table: &str, values: Vec<(&str, DbValue)>) -> DbResult<RowId> {
        let mut engine = self.engine.write().unwrap();

        // 获取 schema 验证列
        let schema = engine.get_schema(table)?.clone();

        // 构建行
        let mut row = Row::new();
        for (name, value) in values {
            row.insert(name.to_string(), value);
        }

        // 验证 schema
        let values_ref: Vec<(String, DbValue)> = row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        schema.validate(&values_ref)?;

        let row_id = engine.insert(table, row)?;
        Ok(row_id)
    }

    /// 根据 row_id 获取单行
    pub fn get(&self, table: &str, row_id: RowId) -> DbResult<Option<Row>> {
        let engine = self.engine.read().unwrap();
        engine.get(table, row_id).map(|opt| opt.cloned())
    }

    /// 查询构建器
    pub fn query(&self, table: &str) -> QueryBuilder {
        QueryBuilder::new(table.to_string(), Arc::clone(&self.engine))
    }

    /// 更新构建器
    pub fn update(&self, table: &str) -> UpdateBuilder {
        UpdateBuilder::new(table.to_string(), Arc::clone(&self.engine))
    }

    /// 删除构建器
    pub fn delete(&self, table: &str) -> DeleteBuilder {
        DeleteBuilder::new(table.to_string(), Arc::clone(&self.engine))
    }

    /// 执行事务
    pub fn transaction<F, T>(&self, f: F) -> DbResult<T>
    where
        F: FnOnce(&mut Transaction) -> DbResult<T>,
    {
        let mut tx = Transaction::new(Arc::clone(&self.engine));
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }

    /// 为表列创建索引（单列，向后兼容）
    pub fn create_index(&self, table: &str, column: &str) -> DbResult<()> {
        let mut engine = self.engine.write().unwrap();
        engine.create_index(table, column)
    }

    /// 创建复合索引
    pub fn create_composite_index(&self, table: &str, columns: &[&str]) -> DbResult<()> {
        let mut engine = self.engine.write().unwrap();
        engine.create_composite_index(table, columns)
    }

    /// 创建唯一复合索引
    pub fn create_unique_index(&self, table: &str, columns: &[&str]) -> DbResult<()> {
        let mut engine = self.engine.write().unwrap();
        engine.create_unique_index(table, columns)
    }

    /// 删除索引
    pub fn drop_index(&self, table: &str, column: &str) -> DbResult<bool> {
        let mut engine = self.engine.write().unwrap();
        engine.drop_index(table, column)
    }

    /// 删除复合索引
    pub fn drop_composite_index(&self, table: &str, columns: &[&str]) -> DbResult<bool> {
        let mut engine = self.engine.write().unwrap();
        engine.drop_composite_index(table, columns)
    }

    /// 检查列是否有索引
    pub fn has_index(&self, table: &str, column: &str) -> bool {
        let engine = self.engine.read().unwrap();
        engine.has_index(table, column)
    }

    /// 检查复合索引是否存在
    pub fn has_composite_index(&self, table: &str, columns: &[&str]) -> bool {
        let engine = self.engine.read().unwrap();
        engine.has_composite_index(table, columns)
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

        let results = db.query("users").execute().unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_filter() {
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

        let results = db.query("users")
            .eq("age", DbValue::integer(25))
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name").unwrap().as_text(), Some("Alice"));
    }

    #[test]
    fn test_update() {
        let db = Database::new();
        let columns = vec![
            Column::new("id", DataType::integer()).primary_key(),
            Column::new("name", DataType::text()),
            Column::new("age", DataType::integer()),
        ];
        db.create_table("users", columns).unwrap();

        let row_id = db.insert("users", vec![
            ("id", DbValue::integer(1)),
            ("name", DbValue::text("Alice")),
            ("age", DbValue::integer(25)),
        ]).unwrap();

        db.update("users")
            .eq("id", DbValue::integer(1))
            .set("age", DbValue::integer(26))
            .execute()
            .unwrap();

        let row = db.get("users", row_id).unwrap().unwrap();
        assert_eq!(row.get("age").unwrap().as_integer(), Some(26));
    }

    #[test]
    fn test_delete() {
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

        let deleted = db.delete("users")
            .eq("age", DbValue::integer(25))
            .execute()
            .unwrap();

        assert_eq!(deleted, 1);
        assert!(db.query("users").execute().unwrap().is_empty());
    }

    // ========== 查询构建器完整覆盖测试 ==========

    #[test]
    fn test_query_ne() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(3)), ("name", DbValue::text("Charlie"))]).unwrap();

        // 查询 id != 2 的用户
        let results = db.query("users")
            .ne("id", DbValue::integer(2))
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.get("id").unwrap().as_integer() != Some(2)));
    }

    #[test]
    fn test_query_le_ge() {
        let db = Database::new();
        db.create_table("scores", vec![
            Column::new("id", DataType::integer()),
            Column::new("score", DataType::integer()),
        ]).unwrap();

        for i in 1..=5 {
            db.insert("scores", vec![
                ("id", DbValue::integer(i)),
                ("score", DbValue::integer(i * 10)),
            ]).unwrap();
        }

        // score <= 30
        let results = db.query("scores")
            .le("score", DbValue::integer(30))
            .execute()
            .unwrap();
        assert_eq!(results.len(), 3);

        // score >= 30
        let results = db.query("scores")
            .ge("score", DbValue::integer(30))
            .execute()
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_query_in_list() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(3)), ("name", DbValue::text("Charlie"))]).unwrap();

        let results = db.query("users")
            .in_list("id", vec![DbValue::integer(1), DbValue::integer(3)])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);
        let ids: Vec<_> = results.iter().map(|r| r.get("id").unwrap().as_integer().unwrap()).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
    }

    #[test]
    fn test_query_contains() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(3)), ("name", DbValue::text("Alexander"))]).unwrap();

        // 名字包含 "Al" 的用户
        let results = db.query("users")
            .contains("name", "Al")
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2); // Alice, Alexander
    }

    #[test]
    fn test_query_order_by() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("age", DataType::integer()),
        ]).unwrap();

        db.insert("users", vec![("id", DbValue::integer(1)), ("age", DbValue::integer(30))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(2)), ("age", DbValue::integer(20))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(3)), ("age", DbValue::integer(25))]).unwrap();

        // 升序
        let results = db.query("users")
            .order_by("age", Order::Asc)
            .execute()
            .unwrap();
        assert_eq!(results[0].get("age").unwrap().as_integer(), Some(20));
        assert_eq!(results[2].get("age").unwrap().as_integer(), Some(30));

        // 降序
        let results = db.query("users")
            .order_by("age", Order::Desc)
            .execute()
            .unwrap();
        assert_eq!(results[0].get("age").unwrap().as_integer(), Some(30));
        assert_eq!(results[2].get("age").unwrap().as_integer(), Some(20));
    }

    #[test]
    fn test_query_limit_offset() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        for i in 1..=10 {
            db.insert("users", vec![
                ("id", DbValue::integer(i)),
                ("name", DbValue::text(format!("User{}", i))),
            ]).unwrap();
        }

        // limit 5
        let results = db.query("users").limit(5).execute().unwrap();
        assert_eq!(results.len(), 5);

        // limit 5 offset 5
        let results = db.query("users")
            .order_by("id", Order::Asc)
            .limit(5)
            .offset(5)
            .execute()
            .unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].get("id").unwrap().as_integer(), Some(6));
    }

    #[test]
    fn test_query_empty_table() {
        let db = Database::new();
        db.create_table("empty", vec![
            Column::new("id", DataType::integer()),
        ]).unwrap();

        let results = db.query("empty").execute().unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_query_multiple_conditions() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("age", DataType::integer()),
            Column::new("active", DataType::boolean()),
        ]).unwrap();

        db.insert("users", vec![("id", DbValue::integer(1)), ("age", DbValue::integer(25)), ("active", DbValue::boolean(true))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(2)), ("age", DbValue::integer(30)), ("active", DbValue::boolean(true))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(3)), ("age", DbValue::integer(35)), ("active", DbValue::boolean(false))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(4)), ("age", DbValue::integer(40)), ("active", DbValue::boolean(true))]).unwrap();

        // age > 25 AND age < 40 AND active = true
        let results = db.query("users")
            .gt("age", DbValue::integer(25))
            .lt("age", DbValue::integer(40))
            .eq("active", DbValue::boolean(true))
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id").unwrap().as_integer(), Some(2));
    }

    // ========== 边界和错误处理测试 ==========

    #[test]
    fn test_query_nonexistent_table() {
        let db = Database::new();
        let result = db.query("nonexistent").execute();
        assert!(matches!(result, Err(DbError::TableNotFound(_))));
    }

    #[test]
    fn test_insert_null_to_not_null() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("name", DataType::text()).not_null(),
        ]).unwrap();

        let result = db.insert("users", vec![("name", DbValue::Null)]);
        assert!(matches!(result, Err(DbError::SchemaError(_))));
    }

    #[test]
    fn test_type_mismatch() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("age", DataType::integer()),
        ]).unwrap();

        // 尝试插入文本到整数字段（应该失败）
        let result = db.insert("users", vec![("age", DbValue::text("not a number"))]);
        assert!(matches!(result, Err(DbError::SchemaError(_))));
    }

    #[test]
    fn test_drop_table() {
        let db = Database::new();
        db.create_table("temp", vec![
            Column::new("id", DataType::integer()),
        ]).unwrap();

        assert!(db.has_table("temp"));
        db.drop_table("temp").unwrap();
        assert!(!db.has_table("temp"));

        // 删除不存在的表应该失败
        let result = db.drop_table("nonexistent");
        assert!(matches!(result, Err(DbError::TableNotFound(_))));
    }

    // ========== 事务测试 ==========

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

        let users = db.query("users").execute().unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_transaction_query() {
        let db = Database::new();
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();

        let result = db.transaction(|tx| {
            let count = tx.query("users").execute()?.len();
            tx.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))])?;
            Ok(count)
        });

        assert_eq!(result.unwrap(), 1);
    }

    // ========== JOIN 测试 ==========

    #[test]
    fn test_inner_join() {
        let db = Database::new();

        // 创建用户表
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        // 创建订单表
        db.create_table("orders", vec![
            Column::new("id", DataType::integer()),
            Column::new("user_id", DataType::integer()),
            Column::new("product", DataType::text()),
            Column::new("amount", DataType::integer()),
        ]).unwrap();

        // 插入用户数据
        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(3)), ("name", DbValue::text("Charlie"))]).unwrap();

        // 插入订单数据
        db.insert("orders", vec![("id", DbValue::integer(1)), ("user_id", DbValue::integer(1)), ("product", DbValue::text("Book")), ("amount", DbValue::integer(100))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(2)), ("user_id", DbValue::integer(1)), ("product", DbValue::text("Pen")), ("amount", DbValue::integer(10))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(3)), ("user_id", DbValue::integer(2)), ("product", DbValue::text("Notebook")), ("amount", DbValue::integer(50))]).unwrap();

        // INNER JOIN：只返回有订单的用户
        let results = db.query("users")
            .inner_join("orders", "users.id", "orders.user_id")
            .execute()
            .unwrap();

        // 应该有 3 条结果（Alice 有 2 个订单，Bob 有 1 个订单）
        assert_eq!(results.len(), 3);

        // 验证字段名前缀
        let first_row = &results[0];
        assert!(first_row.contains_key("users.id"));
        assert!(first_row.contains_key("users.name"));
        assert!(first_row.contains_key("orders.id"));
        assert!(first_row.contains_key("orders.user_id"));
    }

    #[test]
    fn test_left_join() {
        let db = Database::new();

        // 创建用户表
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        // 创建订单表
        db.create_table("orders", vec![
            Column::new("id", DataType::integer()),
            Column::new("user_id", DataType::integer()),
            Column::new("product", DataType::text()),
        ]).unwrap();

        // 插入用户数据
        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(3)), ("name", DbValue::text("Charlie"))]).unwrap();

        // 插入订单数据（只有 Alice 有订单）
        db.insert("orders", vec![("id", DbValue::integer(1)), ("user_id", DbValue::integer(1)), ("product", DbValue::text("Book"))]).unwrap();

        // LEFT JOIN：返回所有用户，即使没有订单
        let results = db.query("users")
            .left_join("orders", "users.id", "orders.user_id")
            .execute()
            .unwrap();

        // 应该有 3 条结果（所有用户）
        assert_eq!(results.len(), 3);

        // Alice 的订单应该有数据
        let alice_orders: Vec<_> = results.iter()
            .filter(|r| r.get("users.name").unwrap().as_text() == Some("Alice"))
            .collect();
        assert_eq!(alice_orders.len(), 1);
        assert!(alice_orders[0].get("orders.id").unwrap().as_integer().is_some());

        // Bob 没有订单，orders.id 应该为 NULL
        let bob_rows: Vec<_> = results.iter()
            .filter(|r| r.get("users.name").unwrap().as_text() == Some("Bob"))
            .collect();
        assert_eq!(bob_rows.len(), 1);
        assert!(bob_rows[0].get("orders.id").unwrap().is_null());
    }

    #[test]
    fn test_join_with_select() {
        let db = Database::new();

        // 创建用户表
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        // 创建订单表
        db.create_table("orders", vec![
            Column::new("id", DataType::integer()),
            Column::new("user_id", DataType::integer()),
            Column::new("product", DataType::text()),
            Column::new("amount", DataType::integer()),
        ]).unwrap();

        // 插入数据
        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(1)), ("user_id", DbValue::integer(1)), ("product", DbValue::text("Book")), ("amount", DbValue::integer(100))]).unwrap();

        // 使用 select 选择特定字段
        let results = db.query("users")
            .inner_join("orders", "users.id", "orders.user_id")
            .select(&["users.name", "orders.product", "orders.amount"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].contains_key("users.name"));
        assert!(results[0].contains_key("orders.product"));
        assert!(results[0].contains_key("orders.amount"));
        assert_eq!(results[0].get("users.name").unwrap().as_text(), Some("Alice"));
        assert_eq!(results[0].get("orders.product").unwrap().as_text(), Some("Book"));
        assert_eq!(results[0].get("orders.amount").unwrap().as_integer(), Some(100));
    }

    #[test]
    fn test_join_with_filter() {
        let db = Database::new();

        // 创建用户表
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        // 创建订单表
        db.create_table("orders", vec![
            Column::new("id", DataType::integer()),
            Column::new("user_id", DataType::integer()),
            Column::new("amount", DataType::integer()),
        ]).unwrap();

        // 插入数据
        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        db.insert("users", vec![("id", DbValue::integer(2)), ("name", DbValue::text("Bob"))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(1)), ("user_id", DbValue::integer(1)), ("amount", DbValue::integer(100))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(2)), ("user_id", DbValue::integer(1)), ("amount", DbValue::integer(200))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(3)), ("user_id", DbValue::integer(2)), ("amount", DbValue::integer(50))]).unwrap();

        // JOIN 后应用过滤条件
        let results = db.query("users")
            .inner_join("orders", "users.id", "orders.user_id")
            .gt("orders.amount", DbValue::integer(90))
            .execute()
            .unwrap();

        // 应该有 2 条结果（amount > 90 的订单）
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_join_with_order_by() {
        let db = Database::new();

        // 创建用户表
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        // 创建订单表
        db.create_table("orders", vec![
            Column::new("id", DataType::integer()),
            Column::new("user_id", DataType::integer()),
            Column::new("amount", DataType::integer()),
        ]).unwrap();

        // 插入数据
        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(1)), ("user_id", DbValue::integer(1)), ("amount", DbValue::integer(100))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(2)), ("user_id", DbValue::integer(1)), ("amount", DbValue::integer(200))]).unwrap();
        db.insert("orders", vec![("id", DbValue::integer(3)), ("user_id", DbValue::integer(1)), ("amount", DbValue::integer(50))]).unwrap();

        // JOIN 后排序
        let results = db.query("users")
            .inner_join("orders", "users.id", "orders.user_id")
            .order_by("orders.amount", Order::Desc)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("orders.amount").unwrap().as_integer(), Some(200));
        assert_eq!(results[2].get("orders.amount").unwrap().as_integer(), Some(50));
    }

    #[test]
    fn test_join_with_limit_offset() {
        let db = Database::new();

        // 创建用户表
        db.create_table("users", vec![
            Column::new("id", DataType::integer()),
            Column::new("name", DataType::text()),
        ]).unwrap();

        // 创建订单表
        db.create_table("orders", vec![
            Column::new("id", DataType::integer()),
            Column::new("user_id", DataType::integer()),
            Column::new("amount", DataType::integer()),
        ]).unwrap();

        // 插入数据
        db.insert("users", vec![("id", DbValue::integer(1)), ("name", DbValue::text("Alice"))]).unwrap();
        for i in 0..10 {
            db.insert("orders", vec![
                ("id", DbValue::integer(i)),
                ("user_id", DbValue::integer(1)),
                ("amount", DbValue::integer((i + 1) * 10)),
            ]).unwrap();
        }

        // JOIN 后分页
        let results = db.query("users")
            .inner_join("orders", "users.id", "orders.user_id")
            .order_by("orders.amount", Order::Asc)
            .limit(5)
            .offset(2)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 5);
        assert_eq!(results[0].get("orders.amount").unwrap().as_integer(), Some(30));
    }
}
