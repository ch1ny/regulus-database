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
        tx.update("accounts")
            .eq("id", DbValue::integer(1))
            .set("balance", DbValue::real(800.0))
            .execute()
            .unwrap();

        // 收款
        tx.update("accounts")
            .eq("id", DbValue::integer(2))
            .set("balance", DbValue::real(700.0))
            .execute()
            .unwrap();

        Ok(())
    });

    assert!(result.is_ok());

    // 验证余额
    let acc1 = db.query("accounts")
        .eq("id", DbValue::integer(1))
        .execute()
        .unwrap();
    assert_eq!(acc1[0].get("balance").unwrap().as_real(), Some(800.0));

    let acc2 = db.query("accounts")
        .eq("id", DbValue::integer(2))
        .execute()
        .unwrap();
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

    let orders = db.query("orders").execute().unwrap();
    assert_eq!(orders.len(), 5);
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
        let items = tx.query("items")
            .eq("id", DbValue::integer(1))
            .execute()
            .unwrap();

        let current_value = items[0].get("value").unwrap().as_integer().unwrap();

        // 更新为新值
        tx.update("items")
            .eq("id", DbValue::integer(1))
            .set("value", DbValue::integer(current_value + 5))
            .execute()
            .unwrap();

        Ok(current_value)
    });

    assert_eq!(result.unwrap(), 10);

    // 验证更新后的值
    let item = db.query("items")
        .eq("id", DbValue::integer(1))
        .execute()
        .unwrap();
    assert_eq!(item[0].get("value").unwrap().as_integer(), Some(15));
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
        let count = tx.query("data").execute()?.len();
        Ok(count)
    });

    assert_eq!(result.unwrap(), 0);
}
