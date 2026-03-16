//! JOIN 操作示例
//!
//! 运行：cargo run --example join_example

use regulus_db::{Database, DbValue, DataType, Column, Order};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new();

    // 创建用户表
    println!("创建用户表...");
    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
        Column::new("email", DataType::text()),
    ])?;

    // 创建订单表
    println!("创建订单表...");
    db.create_table("orders", vec![
        Column::new("id", DataType::integer()),
        Column::new("user_id", DataType::integer()),
        Column::new("product", DataType::text()),
        Column::new("amount", DataType::integer()),
    ])?;

    // 创建产品表
    println!("创建产品表...");
    db.create_table("products", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
        Column::new("price", DataType::integer()),
    ])?;

    // 插入用户数据
    println!("\n插入用户数据...");
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
        ("email", DbValue::text("alice@example.com")),
    ])?;
    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        ("email", DbValue::text("bob@example.com")),
    ])?;
    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Charlie")),
        ("email", DbValue::text("charlie@example.com")),
    ])?;

    // 插入订单数据
    println!("插入订单数据...");
    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Book")),
        ("amount", DbValue::integer(100)),
    ])?;
    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Pen")),
        ("amount", DbValue::integer(10)),
    ])?;
    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("user_id", DbValue::integer(2)),
        ("product", DbValue::text("Notebook")),
        ("amount", DbValue::integer(50)),
    ])?;
    db.insert("orders", vec![
        ("id", DbValue::integer(4)),
        ("user_id", DbValue::integer(1)),
        ("product", DbValue::text("Desk")),
        ("amount", DbValue::integer(500)),
    ])?;

    println!("\n========== INNER JOIN 示例 ==========\n");

    // INNER JOIN：获取用户及其订单
    let results = db.query("users")
        .inner_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "users.email", "orders.product", "orders.amount"])
        .execute()?;

    println!("用户订单（INNER JOIN）：");
    for row in &results {
        let name = row.get("users.name").unwrap().as_text().unwrap();
        let email = row.get("users.email").unwrap().as_text().unwrap();
        let product = row.get("orders.product").unwrap().as_text().unwrap();
        let amount = row.get("orders.amount").unwrap().as_integer().unwrap();
        println!("  {} <{}>: 购买 {} - ${}", name, email, product, amount);
    }

    println!("\n========== LEFT JOIN 示例 ==========\n");

    // LEFT JOIN：获取所有用户及其订单（即使没有订单）
    let results = db.query("users")
        .left_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product", "orders.amount"])
        .execute()?;

    println!("所有用户及订单（LEFT JOIN）：");
    for row in &results {
        let name = row.get("users.name").unwrap().as_text().unwrap();
        let product = row.get("orders.product");
        let amount = row.get("orders.amount");
        if let (Some(p), Some(a)) = (product.as_ref().and_then(|v| v.as_text()), amount.as_ref().and_then(|v| v.as_integer())) {
            println!("  {}: 购买 {} - ${}", name, p, a);
        } else {
            println!("  {}: 无订单", name);
        }
    }

    println!("\n========== JOIN with FILTER 示例 ==========\n");

    // JOIN + 过滤：获取订单金额大于 50 的用户
    let results = db.query("users")
        .inner_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product", "orders.amount"])
        .gt("orders.amount", DbValue::integer(50))
        .execute()?;

    println!("订单金额大于 $50：");
    for row in &results {
        let name = row.get("users.name").unwrap().as_text().unwrap();
        let product = row.get("orders.product").unwrap().as_text().unwrap();
        let amount = row.get("orders.amount").unwrap().as_integer().unwrap();
        println!("  {}: 购买 {} - ${}", name, product, amount);
    }

    println!("\n========== JOIN with ORDER BY 示例 ==========\n");

    // JOIN + 排序：按订单金额降序排列
    let results = db.query("users")
        .inner_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product", "orders.amount"])
        .order_by("orders.amount", Order::Desc)
        .execute()?;

    println!("按订单金额降序排列：");
    for row in &results {
        let name = row.get("users.name").unwrap().as_text().unwrap();
        let product = row.get("orders.product").unwrap().as_text().unwrap();
        let amount = row.get("orders.amount").unwrap().as_integer().unwrap();
        println!("  {}: 购买 {} - ${}", name, product, amount);
    }

    println!("\n========== JOIN with PAGINATION 示例 ==========\n");

    // JOIN + 分页：获取第 2 页（每页 2 条）
    let results = db.query("users")
        .inner_join("orders", "users.id", "orders.user_id")
        .select(&["users.name", "orders.product", "orders.amount"])
        .order_by("orders.amount", Order::Asc)
        .limit(2)
        .offset(1)
        .execute()?;

    println!("分页显示（第 2 页，每页 2 条）：");
    for row in &results {
        let name = row.get("users.name").unwrap().as_text().unwrap();
        let product = row.get("orders.product").unwrap().as_text().unwrap();
        let amount = row.get("orders.amount").unwrap().as_integer().unwrap();
        println!("  {}: 购买 {} - ${}", name, product, amount);
    }

    println!("\n========== 完成 ==========");

    Ok(())
}
