//! 默认值测试

use regulus_db::{Database, DbValue, DataType, Column};

/// 测试各种数据类型的默认值
#[test]
fn test_default_values_all_types() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        // 整数默认值
        Column::new("int_val", DataType::integer()).default(DbValue::integer(42)),
        // 实数默认值
        Column::new("real_val", DataType::real()).default(DbValue::real(3.14)),
        // 文本默认值
        Column::new("text_val", DataType::text()).default(DbValue::text("default")),
        // 布尔默认值
        Column::new("bool_val", DataType::boolean()).default(DbValue::boolean(true)),
        // NULL 默认值
        Column::new("null_val", DataType::null()).default(DbValue::Null),
    ];

    db.create_table("test_table", columns).unwrap();

    // 只插入 id 和 name，其他字段使用默认值
    db.insert("test_table", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("test")),
    ]).unwrap();

    // 验证所有默认值
    let row = db.query("test_table").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row.len(), 1);
    let row = &row[0];

    assert_eq!(row.get("int_val").unwrap().as_integer(), Some(42));
    assert_eq!(row.get("real_val").unwrap().as_real(), Some(3.14));
    assert_eq!(row.get("text_val").unwrap().as_text(), Some("default"));
    assert_eq!(row.get("bool_val").unwrap().as_boolean(), Some(true));
    assert!(row.get("null_val").unwrap().is_null());
}

/// 测试整数类型默认值为 0
#[test]
fn test_integer_default_zero() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("count", DataType::integer()).default(DbValue::integer(0)),
    ];

    db.create_table("items", columns).unwrap();

    db.insert("items", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("item1")),
    ]).unwrap();

    let row = db.query("items").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row[0].get("count").unwrap().as_integer(), Some(0));
}

/// 测试实数类型默认值为 0.0
#[test]
fn test_real_default_zero() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("price", DataType::real()).default(DbValue::real(0.0)),
    ];

    db.create_table("products", columns).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("product1")),
    ]).unwrap();

    let row = db.query("products").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row[0].get("price").unwrap().as_real(), Some(0.0));
}

/// 测试文本类型默认值为空字符串
#[test]
fn test_text_default_empty_string() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("description", DataType::text()).default(DbValue::text("")),
    ];

    db.create_table("products", columns).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("product1")),
    ]).unwrap();

    let row = db.query("products").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row[0].get("description").unwrap().as_text(), Some(""));
}

/// 测试布尔类型默认值为 false
#[test]
fn test_boolean_default_false() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("active", DataType::boolean()).default(DbValue::boolean(false)),
    ];

    db.create_table("users", columns).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("user1")),
    ]).unwrap();

    let row = db.query("users").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row[0].get("active").unwrap().as_boolean(), Some(false));
}

/// 测试更新操作不会重置未修改列为默认值
#[test]
fn test_update_does_not_reset_default_values() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("status", DataType::text()).default(DbValue::text("active")),
        Column::new("age", DataType::integer()).default(DbValue::integer(0)),
    ];

    db.create_table("users", columns).unwrap();

    // 插入时使用默认值
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    // 更新 age 字段
    db.update("users").eq("id", DbValue::integer(1)).set("age", DbValue::integer(25)).execute().unwrap();

    // 验证 status 保持默认值，age 被更新
    let row = db.query("users").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get("status").unwrap().as_text(), Some("active"));
    assert_eq!(row[0].get("age").unwrap().as_integer(), Some(25));
}

/// 测试 NULL 类型默认值
/// 当列类型为 DataType::Null 时，默认值为 NULL
#[test]
fn test_null_type_default_value() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("null_col", DataType::null()).default(DbValue::Null),
    ];

    db.create_table("test_table", columns).unwrap();

    // 插入时不提供 null_col，使用默认值 NULL
    db.insert("test_table", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("test")),
    ]).unwrap();

    // 验证默认值 NULL 被填充
    let row = db.query("test_table").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row.len(), 1);
    assert!(row[0].get("null_col").unwrap().is_null());
}

/// 测试索引与默认值的交互
#[test]
fn test_index_with_default_values() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("status", DataType::text()).default(DbValue::text("active")),
    ];

    db.create_table("users", columns).unwrap();

    // 为 status 列创建索引
    db.create_index("users", "status").unwrap();

    // 插入使用默认值的数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        ("status", DbValue::text("inactive")),
    ]).unwrap();

    // 查询默认值
    let active_users = db.query("users").eq("status", DbValue::text("active")).execute().unwrap();
    assert_eq!(active_users.len(), 1);
    assert_eq!(active_users[0].get("name").unwrap().as_text(), Some("Alice"));

    // 查询显式值
    let inactive_users = db.query("users").eq("status", DbValue::text("inactive")).execute().unwrap();
    assert_eq!(inactive_users.len(), 1);
    assert_eq!(inactive_users[0].get("name").unwrap().as_text(), Some("Bob"));
}

/// 测试多行插入时的默认值
#[test]
fn test_multiple_inserts_with_default_values() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("status", DataType::text()).default(DbValue::text("pending")),
        Column::new("score", DataType::integer()).default(DbValue::integer(100)),
    ];

    db.create_table("players", columns).unwrap();

    // 插入多行，都使用默认值
    for i in 1..=5 {
        db.insert("players", vec![
            ("id", DbValue::integer(i)),
            ("name", DbValue::text(format!("Player{}", i))),
        ]).unwrap();
    }

    // 验证所有行都有正确的默认值
    for i in 1..=5 {
        let row = db.query("players").eq("id", DbValue::integer(i)).execute().unwrap();
        assert_eq!(row.len(), 1);
        assert_eq!(row[0].get("status").unwrap().as_text(), Some("pending"));
        assert_eq!(row[0].get("score").unwrap().as_integer(), Some(100));
    }
}

/// 测试混合使用显式值和默认值
#[test]
fn test_mixed_explicit_and_default_values() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("status", DataType::text()).default(DbValue::text("active")),
        Column::new("level", DataType::integer()).default(DbValue::integer(1)),
        Column::new("active", DataType::boolean()).default(DbValue::boolean(true)),
    ];

    db.create_table("users", columns).unwrap();

    // 第一行：使用默认值
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    // 第二行：显式指定 status，其他使用默认值
    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        ("status", DbValue::text("vip")),
    ]).unwrap();

    // 第三行：显式指定所有字段
    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Charlie")),
        ("status", DbValue::text("admin")),
        ("level", DbValue::integer(10)),
        ("active", DbValue::boolean(false)),
    ]).unwrap();

    // 验证第一行（全默认值）
    let row1 = db.query("users").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row1[0].get("status").unwrap().as_text(), Some("active"));
    assert_eq!(row1[0].get("level").unwrap().as_integer(), Some(1));
    assert_eq!(row1[0].get("active").unwrap().as_boolean(), Some(true));

    // 验证第二行（部分默认值）
    let row2 = db.query("users").eq("id", DbValue::integer(2)).execute().unwrap();
    assert_eq!(row2[0].get("status").unwrap().as_text(), Some("vip"));
    assert_eq!(row2[0].get("level").unwrap().as_integer(), Some(1));
    assert_eq!(row2[0].get("active").unwrap().as_boolean(), Some(true));

    // 验证第三行（全显式值）
    let row3 = db.query("users").eq("id", DbValue::integer(3)).execute().unwrap();
    assert_eq!(row3[0].get("status").unwrap().as_text(), Some("admin"));
    assert_eq!(row3[0].get("level").unwrap().as_integer(), Some(10));
    assert_eq!(row3[0].get("active").unwrap().as_boolean(), Some(false));
}

/// 测试事务中默认值的回滚行为
#[test]
fn test_transaction_default_values_rollback() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("status", DataType::text()).default(DbValue::text("active")),
    ];

    db.create_table("users", columns).unwrap();

    // 插入一条初始数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    // 事务中插入后回滚
    db.transaction(|tx| {
        tx.insert("users", vec![
            ("id", DbValue::integer(2)),
            ("name", DbValue::text("Bob")),
        ]).unwrap();
        tx.rollback()?;
        Ok(())
    }).unwrap();

    // 验证回滚后只有 Alice
    let rows = db.query("users").execute().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name").unwrap().as_text(), Some("Alice"));
}

/// 测试事务中默认值的提交行为
#[test]
fn test_transaction_default_values_commit() {
    let db = Database::new();

    let columns = vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()).not_null(),
        Column::new("status", DataType::text()).default(DbValue::text("active")),
        Column::new("level", DataType::integer()).default(DbValue::integer(1)),
    ];

    db.create_table("users", columns).unwrap();

    // 事务中插入，使用默认值
    db.transaction(|tx| {
        tx.insert("users", vec![
            ("id", DbValue::integer(1)),
            ("name", DbValue::text("Alice")),
        ]).unwrap();
        Ok(())
    }).unwrap();

    // 验证提交后默认值正确
    let row = db.query("users").eq("id", DbValue::integer(1)).execute().unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get("status").unwrap().as_text(), Some("active"));
    assert_eq!(row[0].get("level").unwrap().as_integer(), Some(1));
}
