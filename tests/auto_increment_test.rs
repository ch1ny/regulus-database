//! AUTO_INCREMENT 自增主键测试

use regulus_db::{Database, DbValue, DataType, Column};

/// 测试基础自增功能
#[test]
fn test_auto_increment_basic() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 插入三条数据，不提供 id
    db.insert("users", vec![
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    db.insert("users", vec![
        ("name", DbValue::text("Charlie")),
    ]).unwrap();

    // 验证 id 自动生成
    let users = db.query("users").order_by("id", regulus_db::Order::Asc).execute().unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].get("id").unwrap().as_integer(), Some(1));
    assert_eq!(users[0].get("name").unwrap().as_text(), Some("Alice"));
    assert_eq!(users[1].get("id").unwrap().as_integer(), Some(2));
    assert_eq!(users[1].get("name").unwrap().as_text(), Some("Bob"));
    assert_eq!(users[2].get("id").unwrap().as_integer(), Some(3));
    assert_eq!(users[2].get("name").unwrap().as_text(), Some("Charlie"));
}

/// 测试显式提供自增列值
#[test]
fn test_auto_increment_explicit_value() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 第一次插入，使用自增
    db.insert("users", vec![
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    // 第二次插入，显式指定 id=10
    db.insert("users", vec![
        ("id", DbValue::integer(10)),
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    // 第三次插入，使用自增（应该从 11 开始）
    db.insert("users", vec![
        ("name", DbValue::text("Charlie")),
    ]).unwrap();

    // 验证结果
    let users = db.query("users").order_by("id", regulus_db::Order::Asc).execute().unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].get("id").unwrap().as_integer(), Some(1));   // 自增
    assert_eq!(users[1].get("id").unwrap().as_integer(), Some(10));  // 显式
    assert_eq!(users[2].get("id").unwrap().as_integer(), Some(11));  // 自增（从 11 开始）
}

/// 测试主键冲突
#[test]
fn test_auto_increment_primary_key_conflict() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 插入第一条数据
    db.insert("users", vec![
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    // 尝试插入相同 id 的数据（应该失败）
    let result = db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Bob")),
    ]);

    assert!(result.is_err());
}

/// 测试多行插入后的自增
#[test]
fn test_auto_increment_multiple_inserts() {
    let db = Database::new();

    db.create_table("items", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("value", DataType::integer()),
    ]).unwrap();

    // 插入 10 条数据
    for i in 1..=10 {
        db.insert("items", vec![
            ("value", DbValue::integer(i * 10)),
        ]).unwrap();
    }

    // 验证
    let items = db.query("items").order_by("id", regulus_db::Order::Asc).execute().unwrap();
    assert_eq!(items.len(), 10);

    // 验证 id 从 1 到 10
    for i in 0..10 {
        assert_eq!(items[i].get("id").unwrap().as_integer(), Some((i + 1) as i64));
        assert_eq!(items[i].get("value").unwrap().as_integer(), Some(((i + 1) * 10) as i64));
    }
}

/// 测试删除后自增继续
#[test]
fn test_auto_increment_after_delete() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 插入 3 条数据
    db.insert("users", vec![("name", DbValue::text("Alice"))]).unwrap();
    db.insert("users", vec![("name", DbValue::text("Bob"))]).unwrap();
    db.insert("users", vec![("name", DbValue::text("Charlie"))]).unwrap();

    // 删除中间的记录
    db.delete("users").eq("id", DbValue::integer(2)).execute().unwrap();

    // 再插入一条
    db.insert("users", vec![("name", DbValue::text("David"))]).unwrap();

    // 验证新记录的 id 应该是 4（不会重用已删除的 id）
    let users = db.query("users").order_by("id", regulus_db::Order::Asc).execute().unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(users[2].get("id").unwrap().as_integer(), Some(4));
    assert_eq!(users[2].get("name").unwrap().as_text(), Some("David"));
}

/// 测试事务中的自增
#[test]
fn test_auto_increment_in_transaction() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 事务中插入
    db.transaction(|tx| {
        tx.insert("users", vec![("name", DbValue::text("Alice"))])?;
        tx.insert("users", vec![("name", DbValue::text("Bob"))])?;
        Ok(())
    }).unwrap();

    // 验证
    let users = db.query("users").order_by("id", regulus_db::Order::Asc).execute().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0].get("id").unwrap().as_integer(), Some(1));
    assert_eq!(users[1].get("id").unwrap().as_integer(), Some(2));
}

/// 测试事务回滚后自增
///
/// 注意：本测试采用的行为是自增值不回滚，这符合 MySQL/PostgreSQL 的标准行为
/// 自增值是内存状态，不会因事务回滚而重置
#[test]
fn test_auto_increment_transaction_rollback() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 先插入一条
    db.insert("users", vec![("name", DbValue::text("Alice"))]).unwrap();

    // 事务中插入后回滚
    db.transaction(|tx| {
        tx.insert("users", vec![("name", DbValue::text("Bob"))])?;
        tx.rollback()?;
        Ok(())
    }).unwrap();

    // 验证只有 Alice
    let users = db.query("users").execute().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("name").unwrap().as_text(), Some("Alice"));

    // 再插入一条，id 应该是 3（自增值不会回滚）
    db.insert("users", vec![("name", DbValue::text("Charlie"))]).unwrap();
    let users = db.query("users").order_by("id", regulus_db::Order::Asc).execute().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[1].get("id").unwrap().as_integer(), Some(3));
}

/// 测试非整数类型不能用于自增（编译时限制）
/// 这里测试整数类型自增正常工作
#[test]
fn test_auto_increment_integer_type() {
    let db = Database::new();

    db.create_table("items", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("name", DataType::text()),
        Column::new("price", DataType::real()),
    ]).unwrap();

    // 插入数据
    db.insert("items", vec![
        ("name", DbValue::text("Item1")),
        ("price", DbValue::real(99.99)),
    ]).unwrap();

    db.insert("items", vec![
        ("name", DbValue::text("Item2")),
        ("price", DbValue::real(199.99)),
    ]).unwrap();

    // 验证
    let items = db.query("items").order_by("id", regulus_db::Order::Asc).execute().unwrap();
    assert_eq!(items.len(), 2);

    // id 应该是整数
    assert!(items[0].get("id").unwrap().as_integer().is_some());
    assert!(items[1].get("id").unwrap().as_integer().is_some());
}

/// 测试自增列与索引的兼容性
#[test]
fn test_auto_increment_with_index() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("email", DataType::text()),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 为 email 创建索引
    db.create_index("users", "email").unwrap();

    // 插入数据
    db.insert("users", vec![
        ("email", DbValue::text("alice@example.com")),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("email", DbValue::text("bob@example.com")),
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    // 通过索引查询
    let user = db.query("users").eq("email", DbValue::text("alice@example.com")).execute().unwrap();
    assert_eq!(user.len(), 1);
    assert_eq!(user[0].get("id").unwrap().as_integer(), Some(1));
    assert_eq!(user[0].get("name").unwrap().as_text(), Some("Alice"));
}

/// 测试混合自增和默认值
#[test]
fn test_auto_increment_with_default_values() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer())
            .primary_key()
            .auto_increment(),
        Column::new("name", DataType::text()),
        Column::new("status", DataType::text()).default(DbValue::text("active")),
        Column::new("score", DataType::integer()).default(DbValue::integer(100)),
    ]).unwrap();

    // 插入数据，只提供 name
    db.insert("users", vec![
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    // 验证自增 id 和默认值
    let users = db.query("users").order_by("id", regulus_db::Order::Asc).execute().unwrap();
    assert_eq!(users.len(), 2);

    assert_eq!(users[0].get("id").unwrap().as_integer(), Some(1));
    assert_eq!(users[0].get("name").unwrap().as_text(), Some("Alice"));
    assert_eq!(users[0].get("status").unwrap().as_text(), Some("active"));
    assert_eq!(users[0].get("score").unwrap().as_integer(), Some(100));

    assert_eq!(users[1].get("id").unwrap().as_integer(), Some(2));
    assert_eq!(users[1].get("name").unwrap().as_text(), Some("Bob"));
    assert_eq!(users[1].get("status").unwrap().as_text(), Some("active"));
    assert_eq!(users[1].get("score").unwrap().as_integer(), Some(100));
}
