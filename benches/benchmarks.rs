//! 性能基准测试

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use regulus_db::{Database, DbValue, DataType, Column, Order};

/// 基准测试：插入性能
fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    group.bench_function("insert_100_rows", |b| {
        b.iter(|| {
            let db = Database::new();
            db.create_table("users", vec![
                Column::new("id", DataType::integer()),
                Column::new("name", DataType::text()),
                Column::new("email", DataType::text()),
            ]).unwrap();

            for i in 0..100 {
                db.insert("users", vec![
                    ("id", DbValue::integer(i)),
                    ("name", DbValue::text(format!("User{}", i))),
                    ("email", DbValue::text(format!("user{}@example.com", i))),
                ]).unwrap();
            }
        })
    });

    group.bench_function("insert_1000_rows", |b| {
        b.iter(|| {
            let db = Database::new();
            db.create_table("users", vec![
                Column::new("id", DataType::integer()),
                Column::new("name", DataType::text()),
                Column::new("email", DataType::text()),
            ]).unwrap();

            for i in 0..1000 {
                db.insert("users", vec![
                    ("id", DbValue::integer(i)),
                    ("name", DbValue::text(format!("User{}", i))),
                    ("email", DbValue::text(format!("user{}@example.com", i))),
                ]).unwrap();
            }
        })
    });

    group.finish();
}

/// 基准测试：查询性能（无索引）
fn bench_query_no_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_no_index");

    // 准备数据
    let db = Database::new();
    db.create_table("users", vec![
        Column::new("id", DataType::integer()),
        Column::new("name", DataType::text()),
        Column::new("age", DataType::integer()),
    ]).unwrap();

    for i in 0..1000 {
        db.insert("users", vec![
            ("id", DbValue::integer(i)),
            ("name", DbValue::text(format!("User{}", i))),
            ("age", DbValue::integer(20 + (i % 50))),
        ]).unwrap();
    }

    group.bench_function("query_full_scan", |b| {
        b.iter(|| {
            let results = db.query("users").execute().unwrap();
            black_box(results.len())
        })
    });

    group.bench_function("query_eq_filter", |b| {
        b.iter(|| {
            let results = db.query("users")
                .eq("age", DbValue::integer(30))
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });

    group.bench_function("query_range_filter", |b| {
        b.iter(|| {
            let results = db.query("users")
                .gt("age", DbValue::integer(40))
                .lt("age", DbValue::integer(50))
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });

    group.bench_function("query_multiple_conditions", |b| {
        b.iter(|| {
            let results = db.query("users")
                .gt("age", DbValue::integer(30))
                .lt("age", DbValue::integer(50))
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });

    group.finish();
}

/// 基准测试：排序性能
fn bench_order_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("order_by");

    for size in [100, 500, 1000] {
        let db = Database::new();
        db.create_table("items", vec![
            Column::new("id", DataType::integer()),
            Column::new("value", DataType::integer()),
        ]).unwrap();

        for i in 0..size {
            db.insert("items", vec![
                ("id", DbValue::integer(i)),
                ("value", DbValue::integer(size as i64 - i)),
            ]).unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("asc", size),
            &db,
            |b, db| {
                b.iter(|| {
                    let results = db.query("items")
                        .order_by("value", Order::Asc)
                        .execute()
                        .unwrap();
                    black_box(results.len())
                })
            }
        );

        group.bench_with_input(
            BenchmarkId::new("desc", size),
            &db,
            |b, db| {
                b.iter(|| {
                    let results = db.query("items")
                        .order_by("value", Order::Desc)
                        .execute()
                        .unwrap();
                    black_box(results.len())
                })
            }
        );
    }

    group.finish();
}

/// 基准测试：分页性能
fn bench_pagination(c: &mut Criterion) {
    let mut group = c.benchmark_group("pagination");

    let db = Database::new();
    db.create_table("items", vec![
        Column::new("id", DataType::integer()),
        Column::new("value", DataType::text()),
    ]).unwrap();

    for i in 0..10000 {
        db.insert("items", vec![
            ("id", DbValue::integer(i)),
            ("value", DbValue::text(format!("Item{}", i))),
        ]).unwrap();
    }

    group.bench_function("limit_10", |b| {
        b.iter(|| {
            let results = db.query("items")
                .limit(10)
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });

    group.bench_function("limit_100", |b| {
        b.iter(|| {
            let results = db.query("items")
                .limit(100)
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });

    group.bench_function("offset_100_limit_10", |b| {
        b.iter(|| {
            let results = db.query("items")
                .order_by("id", Order::Asc)
                .limit(10)
                .offset(100)
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });

    group.bench_function("offset_5000_limit_10", |b| {
        b.iter(|| {
            let results = db.query("items")
                .order_by("id", Order::Asc)
                .limit(10)
                .offset(5000)
                .execute()
                .unwrap();
            black_box(results.len())
        })
    });

    group.finish();
}

/// 基准测试：更新和删除性能
fn bench_update_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_delete");

    group.bench_function("update_100_rows", |b| {
        b.iter(|| {
            let db = Database::new();
            db.create_table("items", vec![
                Column::new("id", DataType::integer()),
                Column::new("value", DataType::integer()),
            ]).unwrap();

            for i in 0..100 {
                db.insert("items", vec![
                    ("id", DbValue::integer(i)),
                    ("value", DbValue::integer(i)),
                ]).unwrap();
            }

            db.update("items")
                .eq("value", DbValue::integer(50))
                .set("value", DbValue::integer(999))
                .execute()
                .unwrap();
        })
    });

    group.bench_function("delete_100_rows", |b| {
        b.iter(|| {
            let db = Database::new();
            db.create_table("items", vec![
                Column::new("id", DataType::integer()),
                Column::new("value", DataType::integer()),
            ]).unwrap();

            for i in 0..100 {
                db.insert("items", vec![
                    ("id", DbValue::integer(i)),
                    ("value", DbValue::integer(i)),
                ]).unwrap();
            }

            db.delete("items")
                .eq("value", DbValue::integer(50))
                .execute()
                .unwrap();
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_query_no_index,
    bench_order_by,
    bench_pagination,
    bench_update_delete,
);

criterion_main!(benches);
