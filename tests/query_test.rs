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

/// 测试索引优化的 JOIN
#[test]
fn test_join_with_index() {
    let db = Database::new();

    // 创建用户表
    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
        Column::new("email", DataType::text()),
    ]).unwrap();

    // 创建订单表
    db.create_table("orders", vec![
        Column::new("id", DataType::integer()),
        Column::new("user_id", DataType::integer()),
        Column::new("product", DataType::text()),
        Column::new("amount", DataType::integer()),
    ]).unwrap();

    // 为 orders.user_id 创建索引
    db.create_index("orders", "user_id").unwrap();

    // 插入用户数据
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

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Charlie")),
        ("email", DbValue::text("charlie@example.com")),
    ]).unwrap();

    // 插入订单数据
    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Book")),
        ("amount", DbValue::integer(100)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Pen")),
        ("amount", DbValue::integer(10)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("user_id", DbValue::integer(2)),
        ("product", DbValue::text("Notebook")),
        ("amount", DbValue::integer(50)),
    ]).unwrap();

    // INNER JOIN 测试
    let results = db.query("users")
        .inner_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product", "orders.amount"])
        .execute()
        .unwrap();

    // Alice 有 2 个订单，Bob 有 1 个订单，Charlie 没有订单
    assert_eq!(results.len(), 3);

    // LEFT JOIN 测试 - 应该返回所有用户（包括没有订单的 Charlie）
    let results = db.query("users")
        .left_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product"])
        .execute()
        .unwrap();

    // Alice 2 条 + Bob 1 条 + Charlie 1 条（NULL）= 4 条
    assert_eq!(results.len(), 4);

    // 验证 Charlie 的订单为 NULL
    let charlie_rows: Vec<_> = results.iter()
        .filter(|row| row.get("users.name").unwrap().as_text() == Some("Charlie"))
        .collect();
    assert_eq!(charlie_rows.len(), 1);
    assert!(charlie_rows[0].get("orders.product").unwrap().as_text().is_none());
}

/// 测试带有过滤条件的索引 JOIN
#[test]
fn test_join_with_index_and_filter() {
    let db = Database::new();

    // 创建表
    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
    ]).unwrap();

    db.create_table("orders", vec![
        Column::new("id", DataType::integer()),
        Column::new("user_id", DataType::integer()),
        Column::new("product", DataType::text()),
        Column::new("amount", DataType::integer()),
    ]).unwrap();

    // 为 orders.user_id 创建索引
    db.create_index("orders", "user_id").unwrap();

    // 插入数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Book")),
        ("amount", DbValue::integer(100)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Pen")),
        ("amount", DbValue::integer(10)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("user_id", DbValue::integer(2)),
        ("product", DbValue::text("Notebook")),
        ("amount", DbValue::integer(50)),
    ]).unwrap();

    // JOIN + 过滤：订单金额大于 50
    let results = db.query("users")
        .inner_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product", "orders.amount"])
        .gt("orders.amount", DbValue::integer(50))
        .execute()
        .unwrap();

    // 只有 Alice 的 Book 订单（$100）符合条件
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("users.name").unwrap().as_text(), Some("Alice"));
    assert_eq!(results[0].get("orders.product").unwrap().as_text(), Some("Book"));
}

/// 测试带有排序和分页的索引 JOIN
#[test]
fn test_join_with_index_order_and_pagination() {
    let db = Database::new();

    // 创建表
    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
    ]).unwrap();

    db.create_table("orders", vec![
        Column::new("id", DataType::integer()),
        Column::new("user_id", DataType::integer()),
        Column::new("product", DataType::text()),
        Column::new("amount", DataType::integer()),
    ]).unwrap();

    // 为 orders.user_id 创建索引
    db.create_index("orders", "user_id").unwrap();

    // 插入数据
    for i in 1..=5 {
        db.insert("users", vec![
            ("id", DbValue::integer(i)),
            ("name", DbValue::text(format!("User{}", i))),
        ]).unwrap();

        db.insert("orders", vec![
            ("id", DbValue::integer(i)),
            ("user_id", DbValue::integer(i)),
            ("product", DbValue::text(format!("Product{}", i))),
            ("amount", DbValue::integer(i * 100)),
        ]).unwrap();
    }

    // JOIN + 排序 + 分页
    let results = db.query("users")
        .inner_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.amount"])
        .order_by("orders.amount", Order::Desc)
        .limit(2)
        .offset(1)
        .execute()
        .unwrap();

    // 总共 5 条，每页 2 条，第 2 页应该有 2 条（第 2 和第 3 大的金额）
    assert_eq!(results.len(), 2);
    // 第 2 大的金额应该是 400（User4）
    assert_eq!(results[0].get("orders.amount").unwrap().as_integer(), Some(400));
    // 第 3 大的金额应该是 300（User3）
    assert_eq!(results[1].get("orders.amount").unwrap().as_integer(), Some(300));
}

/// 测试 RIGHT JOIN 基本功能
#[test]
fn test_right_join_basic() {
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

    // 插入用户数据（只有 2 个用户）
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    // 插入订单数据（有 3 个订单，其中 user_id=3 不存在）
    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Book")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Pen")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("user_id", DbValue::integer(3)),  // 用户 3 不存在
        ("product", DbValue::text("Notebook")),
    ]).unwrap();

    // RIGHT JOIN：返回所有订单，包括没有对应客户的订单
    let results = db.query("users")
        .right_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product"])
        .execute()
        .unwrap();

    // 应该有 3 条订单记录
    assert_eq!(results.len(), 3);

    // 验证 Notebook 订单的用户名为 NULL（因为用户 3 不存在）
    let notebook_row = results.iter()
        .find(|row| row.get("orders.product").unwrap().as_text() == Some("Notebook"))
        .unwrap();
    assert!(notebook_row.get("users.name").unwrap().as_text().is_none());
}

/// 测试 RIGHT JOIN 与索引
#[test]
fn test_right_join_with_index() {
    let db = Database::new();

    // 创建表
    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
    ]).unwrap();

    db.create_table("orders", vec![
        Column::new("id", DataType::integer()),
        Column::new("user_id", DataType::integer()),
        Column::new("product", DataType::text()),
    ]).unwrap();

    // 为 orders.user_id 创建索引
    db.create_index("orders", "user_id").unwrap();

    // 插入数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Book")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("user_id", DbValue::integer(3)),  // 用户 3 不存在
        ("product", DbValue::text("Pen")),
    ]).unwrap();

    // RIGHT JOIN with index
    let results = db.query("users")
        .right_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product"])
        .execute()
        .unwrap();

    // 应该有 2 条记录
    assert_eq!(results.len(), 2);

    // 验证 Pen 订单的用户名为 NULL
    let pen_row = results.iter()
        .find(|row| row.get("orders.product").unwrap().as_text() == Some("Pen"))
        .unwrap();
    assert!(pen_row.get("users.name").unwrap().as_text().is_none());
}

/// 测试 FULL OUTER JOIN 基本功能
#[test]
fn test_full_outer_join_basic() {
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

    // 插入用户数据（只有 2 个用户）
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    // 插入订单数据（有 3 个订单，其中 user_id=3 不存在，user_id=4 也不存在）
    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Book")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Pen")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("user_id", DbValue::integer(3)),  // 用户 3 不存在
        ("product", DbValue::text("Notebook")),
    ]).unwrap();

    // FULL OUTER JOIN：返回所有用户和所有订单
    // Alice 有 2 个订单，Bob 没有订单，订单 3 没有对应用户
    // 期望：Alice+Book, Alice+Pen, Bob+NULL, NULL+Notebook = 4 条
    let results = db.query("users")
        .full_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product"])
        .execute()
        .unwrap();

    // 应该有 4 条记录
    assert_eq!(results.len(), 4);

    // 验证 Bob 的行（用户存在，订单为 NULL）
    let bob_null_rows: Vec<_> = results.iter()
        .filter(|row| row.get("users.name").unwrap().as_text() == Some("Bob"))
        .collect();
    assert_eq!(bob_null_rows.len(), 1);
    assert!(bob_null_rows[0].get("orders.product").unwrap().as_text().is_none());

    // 验证 Notebook 订单的行（用户为 NULL，订单存在）
    let notebook_rows: Vec<_> = results.iter()
        .filter(|row| row.get("orders.product").unwrap().as_text() == Some("Notebook"))
        .collect();
    assert_eq!(notebook_rows.len(), 1);
    assert!(notebook_rows[0].get("users.name").unwrap().as_text().is_none());

    // 验证 Alice 的订单（用户和订单都存在）
    let alice_rows: Vec<_> = results.iter()
        .filter(|row| row.get("users.name").unwrap().as_text() == Some("Alice"))
        .collect();
    assert_eq!(alice_rows.len(), 2); // Book 和 Pen
}

/// 测试 FULL OUTER JOIN 与索引
#[test]
fn test_full_outer_join_with_index() {
    let db = Database::new();

    // 创建表
    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
    ]).unwrap();

    db.create_table("orders", vec![
        Column::new("id", DataType::integer()),
        Column::new("user_id", DataType::integer()),
        Column::new("product", DataType::text()),
    ]).unwrap();

    // 为 orders.user_id 创建索引
    db.create_index("orders", "user_id").unwrap();

    // 插入数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Book")),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("user_id", DbValue::integer(3)),  // 用户 3 不存在
        ("product", DbValue::text("Pen")),
    ]).unwrap();

    // FULL OUTER JOIN with index
    // 期望：Alice+Book, Bob+NULL, NULL+Pen = 3 条
    let results = db.query("users")
        .full_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product"])
        .execute()
        .unwrap();

    // 应该有 3 条记录
    assert_eq!(results.len(), 3);

    // 验证 Bob 的行（用户存在，订单为 NULL）
    let bob_null_rows: Vec<_> = results.iter()
        .filter(|row| row.get("users.name").unwrap().as_text() == Some("Bob"))
        .collect();
    assert_eq!(bob_null_rows.len(), 1);
    assert!(bob_null_rows[0].get("orders.product").unwrap().as_text().is_none());

    // 验证 Pen 订单的行（用户为 NULL，订单存在）
    let pen_rows: Vec<_> = results.iter()
        .filter(|row| row.get("orders.product").unwrap().as_text() == Some("Pen"))
        .collect();
    assert_eq!(pen_rows.len(), 1);
    assert!(pen_rows[0].get("users.name").unwrap().as_text().is_none());
}
