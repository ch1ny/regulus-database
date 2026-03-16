//! 并发测试

use regulus_db::{Database, DbValue, DataType, Column};
use std::thread;
use std::sync::Arc;

#[test]
fn test_concurrent_reads() {
    let db = Arc::new(Database::new());

    db.create_table("data", vec![
        Column::new("id", DataType::integer()),
        Column::new("value", DataType::text()),
    ]).unwrap();

    // 插入测试数据
    for i in 1..=10 {
        db.insert("data", vec![
            ("id", DbValue::integer(i)),
            ("value", DbValue::text(format!("Value{}", i))),
        ]).unwrap();
    }

    // 多个线程同时读取
    let mut handles = vec![];
    for thread_id in 0..5 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let results = db_clone.query("data")
                .execute()
                .unwrap();
            assert_eq!(results.len(), 10, "Thread {} should see 10 rows", thread_id);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_writes() {
    let db = Arc::new(Database::new());

    db.create_table("counters", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("count", DataType::integer()),
    ]).unwrap();

    // 初始化计数器
    db.insert("counters", vec![
        ("id", DbValue::integer(1)),
        ("count", DbValue::integer(0)),
    ]).unwrap();

    // 多个线程同时写入不同的行
    let mut handles = vec![];
    for thread_id in 1..=5 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 1..=10 {
                let row_id = thread_id * 100 + i;
                db_clone.insert("counters", vec![
                    ("id", DbValue::integer(row_id)),
                    ("count", DbValue::integer(i)),
                ]).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // 验证所有数据都已插入
    let results = db.query("counters").execute().unwrap();
    // 1 个初始行 + 5 个线程 * 10 行 = 51 行
    assert_eq!(results.len(), 51);
}

#[test]
fn test_read_write_concurrent() {
    let db = Arc::new(Database::new());

    db.create_table("shared_data", vec![
        Column::new("id", DataType::integer()),
        Column::new("value", DataType::integer()),
    ]).unwrap();

    // 插入初始数据
    for i in 1..=5 {
        db.insert("shared_data", vec![
            ("id", DbValue::integer(i)),
            ("value", DbValue::integer(i * 10)),
        ]).unwrap();
    }

    let db_write = Arc::clone(&db);
    let db_read = Arc::clone(&db);

    // 写线程
    let write_handle = thread::spawn(move || {
        for i in 6..=10 {
            db_write.insert("shared_data", vec![
                ("id", DbValue::integer(i)),
                ("value", DbValue::integer(i * 10)),
            ]).unwrap();
        }
    });

    // 读线程
    let read_handle = thread::spawn(move || {
        // 循环读取直到写完成
        let mut total_reads = 0;
        while total_reads < 5 {
            let results = db_read.query("shared_data").execute().unwrap();
            total_reads = results.len();
            // 至少应该能看到初始的 5 行
            assert!(results.len() >= 5);
        }
    });

    write_handle.join().unwrap();
    read_handle.join().unwrap();

    // 最终应该有 10 行
    let final_results = db.query("shared_data").execute().unwrap();
    assert_eq!(final_results.len(), 10);
}
