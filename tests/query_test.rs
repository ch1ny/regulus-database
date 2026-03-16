//! 查询集成测试

use regulus_db::{Database, DbValue, DataType, Column, Order};

#[test]
fn test_multi_table_operations() {
    let db = Database::new();

    // 创建用户表
    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("email", DataType::text()),
    ]).unwrap();

    // 创建订单表
    db.create_table("orders", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("user_id", DataType::integer()),
        Column::new("product", DataType::text()),
        Column::new("amount", DataType::real()),
    ]).unwrap();

    // 插入用户
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
        ("email", DbValue::text("alice@example.com")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        ("email", DbValue::text("bob@example.com")),
    ]).unwrap();

    // 插入订单
    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Laptop")),
        ("amount", DbValue::real(999.99)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Mouse")),
        ("amount", DbValue::real(29.99)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("user_id", DbValue::integer(2)),
        ("product", DbValue::text("Keyboard")),
        ("amount", DbValue::real(79.99)),
    ]).unwrap();

    // 查询所有用户
    let users = db.query("users").execute().unwrap();
    assert_eq!(users.len(), 2);

    // 查询所有订单
    let orders = db.query("orders").execute().unwrap();
    assert_eq!(orders.len(), 3);

    // 查询用户 1 的订单
    let user1_orders = db.query("orders")
        .eq("user_id", DbValue::integer(1))
        .execute()
        .unwrap();
    assert_eq!(user1_orders.len(), 2);
}

#[test]
fn test_complex_filter_chain() {
    let db = Database::new();

    db.create_table("products", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
        Column::new("price", DataType::real()),
        Column::new("stock", DataType::integer()),
        Column::new("category", DataType::text()),
    ]).unwrap();

    // 插入测试数据
    let products = vec![
        (1, "Laptop", 999.99, 50, "Electronics"),
        (2, "Mouse", 29.99, 200, "Electronics"),
        (3, "Desk", 299.99, 30, "Furniture"),
        (4, "Chair", 199.99, 100, "Furniture"),
        (5, "Monitor", 399.99, 75, "Electronics"),
        (6, "Keyboard", 79.99, 150, "Electronics"),
    ];

    for (id, name, price, stock, category) in products {
        db.insert("products", vec![
            ("id", DbValue::integer(id)),
            ("name", DbValue::text(name)),
            ("price", DbValue::real(price)),
            ("stock", DbValue::integer(stock)),
            ("category", DbValue::text(category)),
        ]).unwrap();
    }

    // 查询：Electronics 类别，价格 < 500，库存 > 100
    let results = db.query("products")
        .eq("category", DbValue::text("Electronics"))
        .lt("price", DbValue::real(500.0))
        .gt("stock", DbValue::integer(100))
        .order_by("price", Order::Asc)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 2); // Mouse, Keyboard
    assert!(results[0].get("price").unwrap().as_real().unwrap() < results[1].get("price").unwrap().as_real().unwrap());
}

#[test]
fn test_pagination() {
    let db = Database::new();

    db.create_table("items", vec![
        Column::new("id", DataType::integer()),
        Column::new("value", DataType::text()),
    ]).unwrap();

    // 插入 100 条数据
    for i in 1..=100 {
        db.insert("items", vec![
            ("id", DbValue::integer(i)),
            ("value", DbValue::text(format!("Item{}", i))),
        ]).unwrap();
    }

    // 第 1 页，每页 10 条
    let page1 = db.query("items")
        .order_by("id", Order::Asc)
        .limit(10)
        .offset(0)
        .execute()
        .unwrap();
    assert_eq!(page1.len(), 10);
    assert_eq!(page1[0].get("id").unwrap().as_integer(), Some(1));

    // 第 2 页
    let page2 = db.query("items")
        .order_by("id", Order::Asc)
        .limit(10)
        .offset(10)
        .execute()
        .unwrap();
    assert_eq!(page2.len(), 10);
    assert_eq!(page2[0].get("id").unwrap().as_integer(), Some(11));

    // 第 10 页（最后一页）
    let page10 = db.query("items")
        .order_by("id", Order::Asc)
        .limit(10)
        .offset(90)
        .execute()
        .unwrap();
    assert_eq!(page10.len(), 10);
    assert_eq!(page10[9].get("id").unwrap().as_integer(), Some(100));
}

#[test]
fn test_text_search() {
    let db = Database::new();

    db.create_table("articles", vec![
        Column::new("id", DataType::integer()),
        Column::new("title", DataType::text()),
        Column::new("content", DataType::text()),
    ]).unwrap();

    db.insert("articles", vec![
        ("id", DbValue::integer(1)),
        ("title", DbValue::text("Rust Programming Guide")),
        ("content", DbValue::text("Learn Rust from scratch")),
    ]).unwrap();

    db.insert("articles", vec![
        ("id", DbValue::integer(2)),
        ("title", DbValue::text("Python vs Rust")),
        ("content", DbValue::text("Comparing Python and Rust performance")),
    ]).unwrap();

    db.insert("articles", vec![
        ("id", DbValue::integer(3)),
        ("title", DbValue::text("Web Development with Rust")),
        ("content", DbValue::text("Build web apps using Rust")),
    ]).unwrap();

    // 搜索标题包含 "Rust" 的文章
    let results = db.query("articles")
        .contains("title", "Rust")
        .execute()
        .unwrap();
    assert_eq!(results.len(), 3);

    // 搜索标题包含 "Python" 的文章
    let results = db.query("articles")
        .contains("title", "Python")
        .execute()
        .unwrap();
    assert_eq!(results.len(), 1);
}
