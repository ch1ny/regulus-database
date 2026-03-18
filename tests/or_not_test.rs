//! OR 和 NOT 操作符测试

use regulus_db::{Database, DbValue, DataType, Column, FilterExpr};

/// 测试 OR 操作符基础功能
#[test]
fn test_or_operator_basic() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("age", DataType::integer()),
        Column::new("status", DataType::text()),
    ]).unwrap();

    // 插入测试数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("age", DbValue::integer(25)),
        ("status", DbValue::text("active")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("age", DbValue::integer(15)),
        ("status", DbValue::text("active")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("age", DbValue::integer(30)),
        ("status", DbValue::text("inactive")),
    ]).unwrap();

    // 查询 age > 18 OR status = 'active'
    let results = db.query("users")
        .or(
            FilterExpr::Gt { field: "age".to_string(), value: DbValue::integer(18) },
            FilterExpr::Eq { field: "status".to_string(), value: DbValue::text("active") }
        )
        .execute()
        .unwrap();

    // 应该返回 id=1, id=2, id=3（所有记录）
    // id=1: age=25 > 18 ✓
    // id=2: status='active' ✓
    // id=3: age=30 > 18 ✓
    assert_eq!(results.len(), 3);
}

/// 测试 OR 操作符只匹配一边
#[test]
fn test_or_operator_one_side_match() {
    let db = Database::new();

    db.create_table("products", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("price", DataType::real()),
        Column::new("stock", DataType::integer()),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(1)),
        ("price", DbValue::real(100.0)),
        ("stock", DbValue::integer(0)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(2)),
        ("price", DbValue::real(10.0)),
        ("stock", DbValue::integer(50)),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(3)),
        ("price", DbValue::real(5.0)),
        ("stock", DbValue::integer(0)),
    ]).unwrap();

    // 查询 price < 10 OR stock > 0
    let results = db.query("products")
        .or(
            FilterExpr::Lt { field: "price".to_string(), value: DbValue::real(10.0) },
            FilterExpr::Gt { field: "stock".to_string(), value: DbValue::integer(0) }
        )
        .execute()
        .unwrap();

    // 应该返回 id=2（price=10 不满足 <10，但 stock=50 满足 >0）
    // 实际上 id=2 的 price=10 不满足 <10，但 stock=50 满足 >0
    // id=3 的 price=5 满足 <10
    assert_eq!(results.len(), 2);
}

/// 测试 NOT 操作符基础功能
#[test]
fn test_not_operator_basic() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("status", DataType::text()),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
        ("status", DbValue::text("active")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        ("status", DbValue::text("deleted")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Charlie")),
        ("status", DbValue::text("active")),
    ]).unwrap();

    // 查询 NOT (status = 'deleted')
    // 使用新的简洁 API
    let results = db.query("users")
        .not()
        .eq("status", DbValue::text("deleted"))
        .execute()
        .unwrap();

    // 应该返回 id=1 和 id=3
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.get("status").unwrap().as_text() != Some("deleted")));
}

/// 测试 NOT 操作符与 IN 组合
#[test]
fn test_not_with_in_list() {
    let db = Database::new();

    db.create_table("products", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("category", DataType::text()),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(1)),
        ("category", DbValue::text("electronics")),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(2)),
        ("category", DbValue::text("books")),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(3)),
        ("category", DbValue::text("clothing")),
    ]).unwrap();

    db.insert("products", vec![
        ("id", DbValue::integer(4)),
        ("category", DbValue::text("food")),
    ]).unwrap();

    // 查询 NOT (category IN ['electronics', 'books'])
    // 使用新的简洁 API
    let results = db.query("products")
        .not()
        .in_list("category", vec![DbValue::text("electronics"), DbValue::text("books")])
        .execute()
        .unwrap();

    // 应该返回 category 为 'clothing' 和 'food' 的产品
    assert_eq!(results.len(), 2);
}

/// 测试 or_simple 便捷方法
#[test]
fn test_or_simple_helper() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("age", DataType::integer()),
        Column::new("role", DataType::text()),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("age", DbValue::integer(25)),
        ("role", DbValue::text("user")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("age", DbValue::integer(15)),
        ("role", DbValue::text("admin")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("age", DbValue::integer(30)),
        ("role", DbValue::text("user")),
    ]).unwrap();

    // 使用 or_simple: age > 18 OR role = 'admin'
    let results = db.query("users")
        .or_simple(
            |q| q.gt("age", DbValue::integer(18)),
            |q| q.eq("role", DbValue::text("admin"))
        )
        .execute()
        .unwrap();

    // 应该返回所有 3 条记录
    // id=1: age=25 > 18 ✓
    // id=2: role='admin' ✓
    // id=3: age=30 > 18 ✓
    assert_eq!(results.len(), 3);
}

/// 测试 not_simple 便捷方法
#[test]
fn test_not_simple_helper() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("status", DataType::text()),
        Column::new("active", DataType::boolean()),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("status", DbValue::text("active")),
        ("active", DbValue::boolean(true)),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("status", DbValue::text("deleted")),
        ("active", DbValue::boolean(false)),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("status", DbValue::text("active")),
        ("active", DbValue::boolean(false)),
    ]).unwrap();

    // 使用 not_simple: NOT (status = 'deleted')
    let results = db.query("users")
        .not_simple(|q| q.eq("status", DbValue::text("deleted")))
        .execute()
        .unwrap();

    // 应该返回 id=1 和 id=3
    assert_eq!(results.len(), 2);
}

/// 测试 OR 和 NOT 组合使用
#[test]
fn test_or_and_not_combined() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("age", DataType::integer()),
        Column::new("status", DataType::text()),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("age", DbValue::integer(25)),
        ("status", DbValue::text("active")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("age", DbValue::integer(15)),
        ("status", DbValue::text("deleted")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("age", DbValue::integer(30)),
        ("status", DbValue::text("active")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(4)),
        ("age", DbValue::integer(5)),
        ("status", DbValue::text("active")),
    ]).unwrap();

    // 查询：NOT (age < 10) OR status = 'deleted'
    // 等价于：age >= 10 OR status = 'deleted'
    let results = db.query("users")
        .or(
            FilterExpr::Not(Box::new(FilterExpr::Lt {
                field: "age".to_string(),
                value: DbValue::integer(10)
            })),
            FilterExpr::Eq { field: "status".to_string(), value: DbValue::text("deleted") }
        )
        .execute()
        .unwrap();

    // id=1: age=25 >= 10 ✓
    // id=2: status='deleted' ✓
    // id=3: age=30 >= 10 ✓
    // id=4: age=5 < 10, status != 'deleted' ✗
    assert_eq!(results.len(), 3);
}

/// 测试 where_expr 直接添加复杂表达式
#[test]
fn test_where_expr_complex() {
    let db = Database::new();

    db.create_table("orders", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("amount", DataType::real()),
        Column::new("status", DataType::text()),
        Column::new("priority", DataType::integer()),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(1)),
        ("amount", DbValue::real(1000.0)),
        ("status", DbValue::text("pending")),
        ("priority", DbValue::integer(1)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(2)),
        ("amount", DbValue::real(500.0)),
        ("status", DbValue::text("completed")),
        ("priority", DbValue::integer(2)),
    ]).unwrap();

    db.insert("orders", vec![
        ("id", DbValue::integer(3)),
        ("amount", DbValue::real(2000.0)),
        ("status", DbValue::text("pending")),
        ("priority", DbValue::integer(3)),
    ]).unwrap();

    // 使用 where_expr 添加复杂的嵌套表达式
    // (amount > 800 AND status = 'pending') OR priority > 2
    let expr = FilterExpr::Or(
        Box::new(FilterExpr::And(
            Box::new(FilterExpr::Gt { field: "amount".to_string(), value: DbValue::real(800.0) }),
            Box::new(FilterExpr::Eq { field: "status".to_string(), value: DbValue::text("pending") })
        )),
        Box::new(FilterExpr::Gt { field: "priority".to_string(), value: DbValue::integer(2) })
    );

    let results = db.query("orders")
        .where_expr(expr)
        .execute()
        .unwrap();

    // id=1: amount=1000>800 AND status='pending' ✓
    // id=2: amount=500<800, priority=2 ✗
    // id=3: amount=2000>800 AND status='pending' ✓ (also priority=3>2)
    assert_eq!(results.len(), 2);
}

/// 测试多个 OR 条件链式调用
#[test]
fn test_multiple_or_conditions() {
    let db = Database::new();

    db.create_table("items", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("color", DataType::text()),
        Column::new("size", DataType::text()),
    ]).unwrap();

    db.insert("items", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("item1")),
        ("color", DbValue::text("red")),
        ("size", DbValue::text("small")),
    ]).unwrap();

    db.insert("items", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("item2")),
        ("color", DbValue::text("green")),
        ("size", DbValue::text("medium")),
    ]).unwrap();

    db.insert("items", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("item3")),
        ("color", DbValue::text("blue")),
        ("size", DbValue::text("large")),
    ]).unwrap();

    // 查询：color = 'red' OR color = 'blue'
    let results = db.query("items")
        .or(
            FilterExpr::Eq { field: "color".to_string(), value: DbValue::text("red") },
            FilterExpr::Eq { field: "color".to_string(), value: DbValue::text("blue") }
        )
        .execute()
        .unwrap();

    assert_eq!(results.len(), 2);
}

/// 测试 NOT 与 Contains 组合
#[test]
fn test_not_with_contains() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("email", DataType::text()),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("email", DbValue::text("alice@example.com")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("email", DbValue::text("bob@test.com")),
    ]).unwrap();

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("email", DbValue::text("charlie@example.com")),
    ]).unwrap();

    // 查询 NOT (email contains 'test')
    // 使用新的简洁 API
    let results = db.query("users")
        .not()
        .contains("email", "test")
        .execute()
        .unwrap();

    // 应该返回不包含 'test' 的邮箱：alice 和 charlie
    assert_eq!(results.len(), 2);
}
