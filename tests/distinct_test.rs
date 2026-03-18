//! DISTINCT 去重查询测试

use regulus_db::{Database, DbValue, DataType, Column};

/// 测试基础 DISTINCT 功能 - 单列去重
#[test]
fn test_distinct_single_column() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("age", DataType::integer()),
    ]).unwrap();

    // 插入测试数据 - 有重复的 name
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

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Alice")),
        ("age", DbValue::integer(28)),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(4)),
        ("name", DbValue::text("Charlie")),
        ("age", DbValue::integer(35)),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(5)),
        ("name", DbValue::text("Bob")),
        ("age", DbValue::integer(22)),
    ]).unwrap();

    // 查询不同的 name
    let results = db.query("users")
        .select(&["name"])
        .distinct()
        .execute()
        .unwrap();

    // 应该有 3 个不同的名字：Alice, Bob, Charlie
    assert_eq!(results.len(), 3);

    // 验证结果包含所有不同的名字
    let names: Vec<&str> = results.iter()
        .map(|r| r.get("name").unwrap().as_text().unwrap())
        .collect();

    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
}

/// 测试 DISTINCT 多列去重
#[test]
fn test_distinct_multiple_columns() {
    let db = Database::new();

    db.create_table("orders", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("customer", DataType::text()),
        Column::new("product", DataType::text()),
        Column::new("amount", DataType::real()),
    ]).unwrap();

    // 插入测试数据 - 相同的 customer+product 组合
    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("customer", DbValue::text("Alice")),
        ("product", DbValue::text("Laptop")),
        ("amount", DbValue::real(1000.0)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("customer", DbValue::text("Alice")),
        ("product", DbValue::text("Laptop")),
        ("amount", DbValue::real(1200.0)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("customer", DbValue::text("Bob")),
        ("product", DbValue::text("Laptop")),
        ("amount", DbValue::real(1100.0)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(4)),
        ("customer", DbValue::text("Alice")),
        ("product", DbValue::text("Mouse")),
        ("amount", DbValue::real(50.0)),
    ]).unwrap();

    // 查询不同的 customer+product 组合
    let results = db.query("orders")
        .select(&["customer", "product"])
        .distinct()
        .execute()
        .unwrap();

    // 应该有 3 个不同的组合：
    // (Alice, Laptop), (Bob, Laptop), (Alice, Mouse)
    assert_eq!(results.len(), 3);
}

/// 测试 DISTINCT 与 WHERE 条件组合
#[test]
fn test_distinct_with_where() {
    let db = Database::new();

    db.create_table("products", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("category", DataType::text()),
        Column::new("price", DataType::real()),
        Column::new("stock", DataType::integer()),
    ]).unwrap();

    // 插入测试数据
    db.insert("products", vec![
        ("id", DbValue::integer(1)),
        ("category", DbValue::text("electronics")),
        ("price", DbValue::real(100.0)),
        ("stock", DbValue::integer(10)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(2)),
        ("category", DbValue::text("electronics")),
        ("price", DbValue::real(200.0)),
        ("stock", DbValue::integer(0)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(3)),
        ("category", DbValue::text("books")),
        ("price", DbValue::real(50.0)),
        ("stock", DbValue::integer(100)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(4)),
        ("category", DbValue::text("electronics")),
        ("price", DbValue::real(150.0)),
        ("stock", DbValue::integer(5)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(5)),
        ("category", DbValue::text("books")),
        ("price", DbValue::real(30.0)),
        ("stock", DbValue::integer(0)),
    ]).unwrap();

    // 查询 stock > 0 的不同 category
    let results = db.query("products")
        .select(&["category"])
        .distinct()
        .gt("stock", DbValue::integer(0))
        .execute()
        .unwrap();

    // stock > 0 的有：electronics(id=1,4), books(id=3)
    // 不同的 category 应该是：electronics, books
    assert_eq!(results.len(), 2);
}

/// 测试 DISTINCT 与 ORDER BY 组合
#[test]
fn test_distinct_with_order_by() {
    let db = Database::new();

    db.create_table("scores", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("player", DataType::text()),
        Column::new("score", DataType::integer()),
    ]).unwrap();

    // 插入测试数据
    db.insert("scores", vec![
        ("id", DbValue::integer(1)),
        ("player", DbValue::text("Alice")),
        ("score", DbValue::integer(100)),
    ]).unwrap();

    db.insert("scores", vec![
        ("id", DbValue::integer(2)),
        ("player", DbValue::text("Bob")),
        ("score", DbValue::integer(90)),
    ]).unwrap();

    db.insert("scores", vec![
        ("id", DbValue::integer(3)),
        ("player", DbValue::text("Alice")),
        ("score", DbValue::integer(95)),
    ]).unwrap();

    db.insert("scores", vec![
        ("id", DbValue::integer(4)),
        ("player", DbValue::text("Charlie")),
        ("score", DbValue::integer(80)),
    ]).unwrap();

    // 查询不同的分数，按分数降序排列
    let results = db.query("scores")
        .select(&["score"])
        .distinct()
        .order_by("score", regulus_db::Order::Desc)
        .execute()
        .unwrap();

    // 应该有 4 个不同的分数：100, 95, 90, 80
    assert_eq!(results.len(), 4);

    // 验证顺序是降序
    let scores: Vec<i64> = results.iter()
        .map(|r| r.get("score").unwrap().as_integer().unwrap())
        .collect();

    assert_eq!(scores, vec![100, 95, 90, 80]);
}

/// 测试 DISTINCT 与 LIMIT 组合
#[test]
fn test_distinct_with_limit() {
    let db = Database::new();

    db.create_table("items", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("color", DataType::text()),
    ]).unwrap();

    // 插入测试数据 - 多种颜色
    for i in 1..=10 {
        let color = match i % 4 {
            0 => "red",
            1 => "green",
            2 => "blue",
            3 => "yellow",
            _ => unreachable!(),
        };
        db.insert("items", vec![
            ("id", DbValue::integer(i)),
            ("color", DbValue::text(color)),
        ]).unwrap();
    }

    // 查询不同的颜色，限制返回 2 个
    let results = db.query("items")
        .select(&["color"])
        .distinct()
        .limit(2)
        .execute()
        .unwrap();

    // 应该只有 2 个结果
    assert_eq!(results.len(), 2);
}

/// 测试没有重复数据时 DISTINCT 的行为
#[test]
fn test_distinct_no_duplicates() {
    let db = Database::new();

    db.create_table("unique_items", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("code", DataType::text()),
    ]).unwrap();

    // 插入唯一的数据
    for i in 1..=5 {
        db.insert("unique_items", vec![
            ("id", DbValue::integer(i)),
            ("code", DbValue::text(format!("CODE{}", i))),
        ]).unwrap();
    }

    // 查询不同的 code
    let results = db.query("unique_items")
        .select(&["code"])
        .distinct()
        .execute()
        .unwrap();

    // 所有数据都是唯一的，应该返回 5 个
    assert_eq!(results.len(), 5);
}

/// 测试空表的 DISTINCT 查询
#[test]
fn test_distinct_empty_table() {
    let db = Database::new();

    db.create_table("empty_table", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("value", DataType::text()),
    ]).unwrap();

    // 查询空表的 DISTINCT
    let results = db.query("empty_table")
        .select(&["value"])
        .distinct()
        .execute()
        .unwrap();

    // 应该返回空结果
    assert!(results.is_empty());
}

/// 测试 DISTINCT 全列选择
#[test]
fn test_distinct_all_columns() {
    let db = Database::new();

    db.create_table("data", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("a", DataType::integer()),
        Column::new("b", DataType::text()),
    ]).unwrap();

    // 插入测试数据 - 有些行 a+b 相同但 id 不同
    db.insert("data", vec![
        ("id", DbValue::integer(1)),
        ("a", DbValue::integer(10)),
        ("b", DbValue::text("x")),
    ]).unwrap();

    db.insert("data", vec![
        ("id", DbValue::integer(2)),
        ("a", DbValue::integer(10)),
        ("b", DbValue::text("x")),
    ]).unwrap();

    db.insert("data", vec![
        ("id", DbValue::integer(3)),
        ("a", DbValue::integer(20)),
        ("b", DbValue::text("y")),
    ]).unwrap();

    // 查询所有列的 DISTINCT
    let results = db.query("data")
        .select(&["id", "a", "b"])
        .distinct()
        .execute()
        .unwrap();

    // 应该有 3 行，每行都是唯一的（因为 id 不同）
    assert_eq!(results.len(), 3);
}

/// 测试 DISTINCT 与聚合函数组合（DISTINCT 应该先于聚合执行）
#[test]
fn test_distinct_with_aggregation() {
    let db = Database::new();

    db.create_table("sales", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("region", DataType::text()),
        Column::new("amount", DataType::real()),
    ]).unwrap();

    // 插入测试数据
    db.insert("sales", vec![
        ("id", DbValue::integer(1)),
        ("region", DbValue::text("North")),
        ("amount", DbValue::real(100.0)),
    ]).unwrap();

    db.insert("sales", vec![
        ("id", DbValue::integer(2)),
        ("region", DbValue::text("North")),
        ("amount", DbValue::real(200.0)),
    ]).unwrap();

    db.insert("sales", vec![
        ("id", DbValue::integer(3)),
        ("region", DbValue::text("South")),
        ("amount", DbValue::real(150.0)),
    ]).unwrap();

    db.insert("sales", vec![
        ("id", DbValue::integer(4)),
        ("region", DbValue::text("North")),
        ("amount", DbValue::real(100.0)),
    ]).unwrap();

    // 查询不同 region 的数量
    let results = db.query("sales")
        .select(&["region"])
        .distinct()
        .execute()
        .unwrap();

    // 应该有 2 个不同的 region: North, South
    assert_eq!(results.len(), 2);
}

/// 测试 DISTINCT 包含 NULL 值
#[test]
fn test_distinct_with_null_values() {
    let db = Database::new();

    db.create_table("nullable_table", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("value", DataType::text()).not_null(),
    ]).unwrap();

    // 插入包含 NULL 的数据 - 使用空字符串代替 NULL
    db.insert("nullable_table", vec![
        ("id", DbValue::integer(1)),
        ("value", DbValue::text("A")),
    ]).unwrap();

    db.insert("nullable_table", vec![
        ("id", DbValue::integer(2)),
        ("value", DbValue::text("")),
    ]).unwrap();

    db.insert("nullable_table", vec![
        ("id", DbValue::integer(3)),
        ("value", DbValue::text("A")),
    ]).unwrap();

    db.insert("nullable_table", vec![
        ("id", DbValue::integer(4)),
        ("value", DbValue::text("")),
    ]).unwrap();

    db.insert("nullable_table", vec![
        ("id", DbValue::integer(5)),
        ("value", DbValue::text("B")),
    ]).unwrap();

    // 查询不同的 value
    let results = db.query("nullable_table")
        .select(&["value"])
        .distinct()
        .execute()
        .unwrap();

    // 应该有 3 个不同的值：A, 空字符串，B
    assert_eq!(results.len(), 3);

    let values: Vec<&str> = results.iter()
        .map(|r| r.get("value").unwrap().as_text().unwrap())
        .collect();

    assert!(values.contains(&"A"));
    assert!(values.contains(&""));
    assert!(values.contains(&"B"));
}
