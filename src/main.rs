use regulus_db::{Database, DbValue, DataType, Column, Order};

fn main() {
    // 创建数据库
    let db = Database::new();

    // 创建表
    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("age", DataType::integer()),
        Column::new("active", DataType::boolean()),
    ]).expect("Failed to create table");

    // 插入数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
        ("age", DbValue::integer(25)),
        ("active", DbValue::boolean(true)),
    ]).expect("Failed to insert");

    db.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
        ("age", DbValue::integer(30)),
        ("active", DbValue::boolean(true)),
    ]).expect("Failed to insert");

    db.insert("users", vec![
        ("id", DbValue::integer(3)),
        ("name", DbValue::text("Charlie")),
        ("age", DbValue::integer(35)),
        ("active", DbValue::boolean(false)),
    ]).expect("Failed to insert");

    // 查询所有
    println!("All users:");
    let all = db.query("users").execute().expect("Query failed");
    for row in &all {
        println!("  {:?}", row);
    }

    // 条件查询
    println!("\nActive users:");
    let active = db.query("users")
        .eq("active", DbValue::boolean(true))
        .execute()
        .expect("Query failed");
    for row in &active {
        println!("  {} - {} ({})",
            row.get("name").unwrap().as_text().unwrap(),
            row.get("age").unwrap().as_integer().unwrap(),
            row.get("id").unwrap().as_integer().unwrap()
        );
    }

    // 范围查询 + 排序
    println!("\nUsers age > 25, ordered by age desc:");
    let filtered = db.query("users")
        .gt("age", DbValue::integer(25))
        .order_by("age", Order::Desc)
        .execute()
        .expect("Query failed");
    for row in &filtered {
        println!("  {} - {} years old",
            row.get("name").unwrap().as_text().unwrap(),
            row.get("age").unwrap().as_integer().unwrap()
        );
    }

    // 更新
    db.update("users")
        .eq("id", DbValue::integer(1))
        .set("age", DbValue::integer(26))
        .execute()
        .expect("Update failed");
    println!("\nUpdated user 1 age to 26");

    // 删除
    let deleted = db.delete("users")
        .eq("active", DbValue::boolean(false))
        .execute()
        .expect("Delete failed");
    println!("Deleted {} inactive users", deleted);

    // 最终结果
    println!("\nFinal users:");
    let final_result = db.query("users")
        .order_by("id", Order::Asc)
        .execute()
        .expect("Query failed");
    for row in &final_result {
        println!("  {:?}", row);
    }

    // 事务示例
    println!("\n--- Transaction demo ---");
    db.transaction(|tx| {
        tx.insert("users", vec![
            ("id", DbValue::integer(100)),
            ("name", DbValue::text("Transaction User")),
            ("age", DbValue::integer(99)),
            ("active", DbValue::boolean(true)),
        ])?;

        let count = tx.query("users").execute()?.len();
        println!("Users count in transaction: {}", count);

        Ok(())
    }).expect("Transaction failed");
}
