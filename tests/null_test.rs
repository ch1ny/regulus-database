//! IS NULL / IS NOT NULL 功能测试

use regulus_db::{Database, DbValue, DataType, Column};

/// 测试基础 IS NULL 查询
#[test]
fn test_is_null_basic() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("email", DataType::text()),
    ]).unwrap();

    // 插入包含 NULL 的数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
        ("email", DbValue::text("alice@example.com")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        // email 为 NULL
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Charlie")),
        ("email", DbValue::text("charlie@example.com")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(4)),
        ("name", DbValue::text("David")),
        // email 为 NULL
    ]).unwrap();

    // 查询 email 为 NULL 的用户
    let null_emails = db.query("users")
        .is_null("email")
        .order_by("id", regulus_db::Order::Asc)
        .execute()
        .unwrap();

    assert_eq!(null_emails.len(), 2);
    assert_eq!(null_emails[0].get("id").unwrap().as_integer(), Some(2));
    assert_eq!(null_emails[0].get("name").unwrap().as_text(), Some("Bob"));
    assert_eq!(null_emails[1].get("id").unwrap().as_integer(), Some(4));
    assert_eq!(null_emails[1].get("name").unwrap().as_text(), Some("David"));
}

/// 测试基础 IS NOT NULL 查询
#[test]
fn test_is_not_null_basic() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("email", DataType::text()),
    ]).unwrap();

    // 插入包含 NULL 的数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
        ("email", DbValue::text("alice@example.com")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        // email 为 NULL
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Charlie")),
        ("email", DbValue::text("charlie@example.com")),
    ]).unwrap();

    // 查询 email 不为 NULL 的用户
    let not_null_emails = db.query("users")
        .is_not_null("email")
        .order_by("id", regulus_db::Order::Asc)
        .execute()
        .unwrap();

    assert_eq!(not_null_emails.len(), 2);
    assert_eq!(not_null_emails[0].get("id").unwrap().as_integer(), Some(1));
    assert_eq!(not_null_emails[0].get("email").unwrap().as_text(), Some("alice@example.com"));
    assert_eq!(not_null_emails[1].get("id").unwrap().as_integer(), Some(3));
    assert_eq!(not_null_emails[1].get("email").unwrap().as_text(), Some("charlie@example.com"));
}

/// 测试 IS NULL 与 WHERE 条件组合
#[test]
fn test_is_null_with_where() {
    let db = Database::new();

    db.create_table("products", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("discount", DataType::real()),
        Column::new("stock", DataType::integer()),
    ]).unwrap();

    // 插入测试数据
    db.insert("products", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Laptop")),
        ("discount", DbValue::real(0.1)),
        ("stock", DbValue::integer(10)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Mouse")),
        // discount 为 NULL
        ("stock", DbValue::integer(50)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Keyboard")),
        ("discount", DbValue::real(0.2)),
        ("stock", DbValue::integer(0)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(4)),
        ("name", DbValue::text("Monitor")),
        // discount 为 NULL
        ("stock", DbValue::integer(5)),
    ]).unwrap();

    // 查询 discount 为 NULL 且 stock > 10 的产品
    let results = db.query("products")
        .is_null("discount")
        .gt("stock", DbValue::integer(10))
        .order_by("id", regulus_db::Order::Asc)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap().as_integer(), Some(2));
    assert_eq!(results[0].get("name").unwrap().as_text(), Some("Mouse"));
}

/// 测试 IS NOT NULL 与 ORDER BY 和 LIMIT 组合
#[test]
fn test_is_not_null_with_order_by_and_limit() {
    let db = Database::new();

    db.create_table("scores", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("player", DataType::text()),
        Column::new("score", DataType::integer()),
    ]).unwrap();

    // 插入测试数据
    for i in 1..=5 {
        let mut values = vec![
            ("id", DbValue::integer(i)),
            ("player", DbValue::text(format!("Player{}", i))),
        ];
        if i != 3 {
            values.push(("score", DbValue::integer(i * 10)));
        }
        db.insert("scores", values).unwrap();
    }

    // 查询 score 不为 NULL 的记录，按分数降序，限制 2 条
    let results = db.query("scores")
        .is_not_null("score")
        .order_by("score", regulus_db::Order::Desc)
        .limit(2)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("score").unwrap().as_integer(), Some(50));
    assert_eq!(results[1].get("score").unwrap().as_integer(), Some(40));
}

/// 测试 NOT + IS NULL 组合（相当于 IS NOT NULL）
#[test]
fn test_not_is_null() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("phone", DataType::text()),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
        ("phone", DbValue::text("123456")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        // phone 为 NULL
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Charlie")),
        ("phone", DbValue::text("789012")),
    ]).unwrap();

    // 使用 NOT + IS NULL 查询 phone 不为 NULL 的用户
    // 使用新的简洁 API：not().is_null() 相当于 IS NOT NULL
    let results = db.query("users")
        .not()
        .is_null("phone")
        .order_by("id", regulus_db::Order::Asc)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap().as_integer(), Some(1));
    assert_eq!(results[1].get("id").unwrap().as_integer(), Some(3));
}

/// 测试空表的 IS NULL 查询
#[test]
fn test_is_null_empty_table() {
    let db = Database::new();

    db.create_table("empty", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("value", DataType::text()),
    ]).unwrap();

    // 查询空表的 IS NULL
    let results = db.query("empty")
        .is_null("value")
        .execute()
        .unwrap();

    assert!(results.is_empty());
}

/// 测试所有字段都有值时的 IS NULL 查询
#[test]
fn test_is_null_no_nulls() {
    let db = Database::new();

    db.create_table("items", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
    ]).unwrap();

    for i in 1..=5 {
        db.insert("items", vec![
            ("id", DbValue::integer(i)),
            ("name", DbValue::text(format!("Item{}", i))),
        ]).unwrap();
    }

    // 查询 name 为 NULL 的记录（应该没有）
    let results = db.query("items")
        .is_null("name")
        .execute()
        .unwrap();

    assert!(results.is_empty());

    // 查询 name 不为 NULL 的记录（应该有 5 条）
    let results = db.query("items")
        .is_not_null("name")
        .execute()
        .unwrap();

    assert_eq!(results.len(), 5);
}

/// 测试 IS NULL 与聚合函数组合
#[test]
fn test_is_null_with_aggregation() {
    let db = Database::new();

    db.create_table("orders", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("customer", DataType::text()),
        Column::new("shipped_date", DataType::text()),
    ]).unwrap();

    // 插入测试数据
    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("customer", DbValue::text("Alice")),
        ("shipped_date", DbValue::text("2024-01-01")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("customer", DbValue::text("Bob")),
        // shipped_date 为 NULL（未发货）
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("customer", DbValue::text("Charlie")),
        ("shipped_date", DbValue::text("2024-01-02")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(4)),
        ("customer", DbValue::text("David")),
        // shipped_date 为 NULL（未发货）
    ]).unwrap();

    // 统计未发货订单数量
    let results = db.query("orders")
        .is_null("shipped_date")
        .select(&["customer"])
        .execute()
        .unwrap();

    assert_eq!(results.len(), 2);
}
