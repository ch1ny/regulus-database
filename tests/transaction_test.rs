//! 事务测试

use regulus_db::{Database, DbValue, DataType, Column};

#[test]
fn test_transaction_atomicity() {
    let db = Database::new();

    db.create_table("accounts", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("balance", DataType::real()),
    ]).unwrap();

    // 初始化两个账户
    db.insert("accounts", vec![
        ("id", DbValue::integer(1)),
        ("balance", DbValue::real(1000.0)),
    ]).unwrap();

    db.insert("accounts", vec![
        ("id", DbValue::integer(2)),
        ("balance", DbValue::real(500.0)),
    ]).unwrap();

    // 转账事务：从账户 1 转 200 到账户 2
    let result = db.transaction(|tx| {
        // 扣款
        tx.update("accounts",
            |row| row.get("id").and_then(|v| v.as_integer()) == Some(1),
            vec![("balance", DbValue::real(800.0))],
        ).unwrap();

        // 收款
        tx.update("accounts",
            |row| row.get("id").and_then(|v| v.as_integer()) == Some(2),
            vec![("balance", DbValue::real(700.0))],
        ).unwrap();

        Ok(())
    });

    assert!(result.is_ok());

    // 验证余额
    let rows = db.transaction(|tx| {
        let rows = tx.query_all("accounts")?;
        Ok(rows)
    }).unwrap();

    let acc1: Vec<_> = rows.iter()
        .filter(|r| r.get("id").and_then(|v| v.as_integer()) == Some(1))
        .collect();
    assert_eq!(acc1[0].get("balance").unwrap().as_real(), Some(800.0));

    let acc2: Vec<_> = rows.iter()
        .filter(|r| r.get("id").and_then(|v| v.as_integer()) == Some(2))
        .collect();
    assert_eq!(acc2[0].get("balance").unwrap().as_real(), Some(700.0));
}

#[test]
fn test_transaction_multiple_operations() {
    let db = Database::new();

    db.create_table("orders", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("product_id", DataType::integer()),
        Column::new("quantity", DataType::integer()),
        Column::new("status", DataType::text()),
    ]).unwrap();

    // 在一个事务中创建多个订单
    let result = db.transaction(|tx| {
        for i in 1..=5 {
            tx.insert("orders", vec![
                ("id", DbValue::integer(i)),
                ("product_id", DbValue::integer(100 + i)),
                ("quantity", DbValue::integer(i * 10)),
                ("status", DbValue::text("pending")),
            ])?;
        }
        Ok(())
    });

    assert!(result.is_ok());

    let result = db.transaction(|tx| {
        let orders = tx.query_all("orders")?;
        Ok(orders.len())
    });

    assert_eq!(result.unwrap(), 5);
}

#[test]
fn test_transaction_query_within_transaction() {
    let db = Database::new();

    db.create_table("items", vec![
        Column::new("id", DataType::integer()),
        Column::new("value", DataType::integer()),
    ]).unwrap();

    db.insert("items", vec![
        ("id", DbValue::integer(1)),
        ("value", DbValue::integer(10)),
    ]).unwrap();

    // 在事务中查询并更新
    let result = db.transaction(|tx| {
        // 查询当前值
        let items = tx.query_all("items")?;
        let current_value = items[0].get("value").unwrap().as_integer().unwrap();

        // 更新为新值
        tx.update("items",
            |row| row.get("id").and_then(|v| v.as_integer()) == Some(1),
            vec![("value", DbValue::integer(current_value + 5))],
        )?;

        Ok(current_value)
    });

    assert_eq!(result.unwrap(), 10);

    // 验证更新后的值
    let result = db.transaction(|tx| {
        let items = tx.query_all("items")?;
        let item = items.iter()
            .find(|r| r.get("id").and_then(|v| v.as_integer()) == Some(1))
            .unwrap();
        Ok(item.get("value").unwrap().as_integer().unwrap())
    });

    assert_eq!(result.unwrap(), 15);
}

#[test]
fn test_transaction_empty() {
    let db = Database::new();

    db.create_table("data", vec![
        Column::new("id", DataType::integer()),
        Column::new("value", DataType::text()),
    ]).unwrap();

    // 空事务（只查询）
    let result = db.transaction(|tx| {
        let rows = tx.query_all("data")?;
        Ok(rows.len())
    });

    assert_eq!(result.unwrap(), 0);
}

#[test]
fn test_transaction_rollback() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 插入一条数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    // 事务中插入后回滚
    let result = db.transaction(|tx| {
        tx.insert("users", vec![
            ("id", DbValue::integer(2)),
            ("name", DbValue::text("Bob")),
        ])?;

        // 手动回滚
        tx.rollback()?;
        Ok(())
    });

    assert!(result.is_ok());

    // 验证 Bob 没有被插入
    let result = db.transaction(|tx| {
        let rows = tx.query_all("users")?;
        Ok(rows.len())
    });

    assert_eq!(result.unwrap(), 1);
}

#[test]
fn test_transaction_update_rollback() {
    let db = Database::new();

    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
    ]).unwrap();

    // 插入一条数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ]).unwrap();

    // 事务中更新后回滚
    let result = db.transaction(|tx| {
        tx.update("users",
            |row| row.get("id").and_then(|v| v.as_integer()) == Some(1),
            vec![("name", DbValue::text("Bob"))],
        )?;

        // 手动回滚
        tx.rollback()?;
        Ok(())
    });

    assert!(result.is_ok());

    // 验证名字没有被更新
    let result = db.transaction(|tx| {
        let rows = tx.query_all("users")?;
        Ok(rows[0].get("name").unwrap().as_text().unwrap().to_string())
    });

    assert_eq!(result.unwrap(), "Alice");
}
