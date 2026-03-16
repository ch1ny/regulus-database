//! JOIN 性能基准测试
//!
//! 比较有索引和无索引时 JOIN 操作的性能差异

use regulus_db::{Database, DbValue, DataType, Column};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn create_large_dataset(db: &Database, users_count: usize, orders_per_user: usize) {
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

    // 插入用户
    for i in 1..=users_count {
        db.insert("users", vec![
            ("id", DbValue::integer(i as i64)),
            ("name", DbValue::text(format!("User{}", i))),
            ("email", DbValue::text(format!("user{}@example.com", i))),
        ]).unwrap();
    }

    // 插入订单
    let mut order_id = 1;
    for user_id in 1..=users_count {
        for _ in 0..orders_per_user {
            db.insert("orders", vec![
                ("id", DbValue::integer(order_id)),
                ("user_id", DbValue::integer(user_id as i64)),
                ("product", DbValue::text(format!("Product{}", order_id))),
                ("amount", DbValue::integer((order_id * 10) as i64)),
            ]).unwrap();
            order_id += 1;
        }
    }
}

fn benchmark_join_without_index(c: &mut Criterion) {
    let db = Database::new();
    create_large_dataset(&db, 100, 10); // 100 用户，每用户 10 订单 = 1000 订单

    c.bench_function("join_100_users_1000_orders_no_index", |b| {
        b.iter(|| {
            let results = db.query("users")
                .inner_join("orders", "users.id", "orders.user_id")
                .select(&["users.name", "orders.product"])
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });
}

fn benchmark_join_with_index(c: &mut Criterion) {
    let db = Database::new();
    create_large_dataset(&db, 100, 10);

    // 为 orders.user_id 创建索引
    db.create_index("orders", "user_id").unwrap();

    c.bench_function("join_100_users_1000_orders_with_index", |b| {
        b.iter(|| {
            let results = db.query("users")
                .inner_join("orders", "users.id", "orders.user_id")
                .select(&["users.name", "orders.product"])
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });
}

fn benchmark_join_large_without_index(c: &mut Criterion) {
    let db = Database::new();
    create_large_dataset(&db, 500, 20); // 500 用户，每用户 20 订单 = 10000 订单

    c.bench_function("join_500_users_10000_orders_no_index", |b| {
        b.iter(|| {
            let results = db.query("users")
                .inner_join("orders", "users.id", "orders.user_id")
                .select(&["users.name", "orders.product"])
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });
}

fn benchmark_join_large_with_index(c: &mut Criterion) {
    let db = Database::new();
    create_large_dataset(&db, 500, 20);

    // 为 orders.user_id 创建索引
    db.create_index("orders", "user_id").unwrap();

    c.bench_function("join_500_users_10000_orders_with_index", |b| {
        b.iter(|| {
            let results = db.query("users")
                .inner_join("orders", "users.id", "orders.user_id")
                .select(&["users.name", "orders.product"])
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });
}

criterion_group!(
    benches,
    benchmark_join_without_index,
    benchmark_join_with_index,
    benchmark_join_large_without_index,
    benchmark_join_large_with_index,
);
criterion_main!(benches);
