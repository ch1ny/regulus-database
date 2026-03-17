use regulus_db::{PersistedEngine, StorageEngine, Row, RowId, DbValue, Column, DataType, TableSchema};
use tempfile::TempDir;

fn create_test_schema() -> TableSchema {
    TableSchema::new(
        "users",
        vec![
            Column::new("id", DataType::integer()).primary_key(),
            Column::new("name", DataType::text()),
            Column::new("age", DataType::integer()),
        ],
    )
}

fn create_test_row() -> Row {
    let mut row = Row::new();
    row.insert("id".to_string(), DbValue::integer(1));
    row.insert("name".to_string(), DbValue::text("Alice"));
    row.insert("age".to_string(), DbValue::integer(25));
    row
}

#[test]
fn test_persisted_create_table() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

    let schema = create_test_schema();
    assert!(engine.create_table(schema).is_ok());
    assert!(engine.has_table("users"));
}

#[test]
fn test_persisted_insert_and_get() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

    engine.create_table(create_test_schema()).unwrap();

    let row = create_test_row();
    let row_id = engine.insert("users", row).unwrap();

    let retrieved = engine.get("users", row_id).unwrap().unwrap();
    assert_eq!(retrieved.get("name").unwrap().as_text(), Some("Alice"));
}

#[test]
fn test_persisted_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

    engine.create_table(create_test_schema()).unwrap();

    for i in 0..5 {
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::integer(i));
        row.insert("name".to_string(), DbValue::text(format!("User{}", i)));
        row.insert("age".to_string(), DbValue::integer(20 + i));
        engine.insert("users", row).unwrap();
    }

    // 手动检查点
    assert!(engine.force_checkpoint().is_ok());

    // WAL 大小应该归零
    assert_eq!(engine.wal_size(), 0);
}

#[test]
fn test_persisted_recovery() {
    let temp_dir = TempDir::new().unwrap();

    // 1. 创建数据库并插入数据
    {
        let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();
        engine.create_table(create_test_schema()).unwrap();

        let row = create_test_row();
        engine.insert("users", row).unwrap();

        // 强制检查点确保数据持久化
        engine.force_checkpoint().unwrap();

        // 再插入一条数据（不检查点，测试 WAL 恢复）
        let mut row2 = Row::new();
        row2.insert("id".to_string(), DbValue::integer(2));
        row2.insert("name".to_string(), DbValue::text("Bob"));
        row2.insert("age".to_string(), DbValue::integer(30));
        engine.insert("users", row2).unwrap();
    }

    // 2. 重新打开数据库（模拟恢复）
    let engine = PersistedEngine::open(temp_dir.path()).unwrap();

    // 3. 验证数据
    assert_eq!(engine.inner().get_row_count("users").unwrap(), 2);

    let row1 = engine.inner().get("users", RowId(0)).unwrap().unwrap();
    assert_eq!(row1.get("name").unwrap().as_text(), Some("Alice"));

    let row2 = engine.inner().get("users", RowId(1)).unwrap().unwrap();
    assert_eq!(row2.get("name").unwrap().as_text(), Some("Bob"));
}

#[test]
fn test_persisted_update() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

    engine.create_table(create_test_schema()).unwrap();

    let row = create_test_row();
    let row_id = engine.insert("users", row).unwrap();

    // 更新数据
    let mut update_values = Row::new();
    update_values.insert("age".to_string(), DbValue::integer(26));

    engine.update("users", row_id, update_values).unwrap();

    // 验证更新
    let retrieved = engine.get("users", row_id).unwrap().unwrap();
    assert_eq!(retrieved.get("age").unwrap().as_integer(), Some(26));

    // 重启验证持久化
    engine.force_checkpoint().unwrap();
    drop(engine);

    let engine = PersistedEngine::open(temp_dir.path()).unwrap();
    let retrieved = engine.inner().get("users", row_id).unwrap().unwrap();
    assert_eq!(retrieved.get("age").unwrap().as_integer(), Some(26));
}

#[test]
fn test_persisted_delete() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

    engine.create_table(create_test_schema()).unwrap();

    let row = create_test_row();
    let row_id = engine.insert("users", row).unwrap();

    // 删除数据
    let deleted = engine.delete("users", row_id).unwrap();
    assert!(deleted.is_some());

    // 验证删除
    assert!(engine.get("users", row_id).unwrap().is_none());

    // 重启验证持久化
    engine.force_checkpoint().unwrap();
    drop(engine);

    let engine = PersistedEngine::open(temp_dir.path()).unwrap();
    assert_eq!(engine.inner().get_row_count("users").unwrap(), 0);
}

#[test]
fn test_persisted_drop_table() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

    engine.create_table(create_test_schema()).unwrap();
    assert!(engine.has_table("users"));

    // 删除表
    assert!(engine.drop_table("users").is_ok());
    assert!(!engine.has_table("users"));

    // 重启验证持久化
    engine.force_checkpoint().unwrap();
    drop(engine);

    let engine = PersistedEngine::open(temp_dir.path()).unwrap();
    assert!(!engine.inner().has_table("users"));
}

#[test]
fn test_wal_only_recovery() {
    // 测试仅 WAL 恢复（没有快照）
    let temp_dir = TempDir::new().unwrap();

    // 1. 创建数据库并插入数据
    {
        let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();
        engine.create_table(create_test_schema()).unwrap();

        let row = create_test_row();
        engine.insert("users", row).unwrap();

        // 不检查点，仅依赖 WAL
    }

    // 2. 重新打开数据库（模拟恢复）
    let engine = PersistedEngine::open(temp_dir.path()).unwrap();

    // 3. 验证数据（应该从 WAL 恢复）
    assert_eq!(engine.inner().get_row_count("users").unwrap(), 1);

    let row1 = engine.inner().get("users", RowId(0)).unwrap().unwrap();
    assert_eq!(row1.get("name").unwrap().as_text(), Some("Alice"));
}

#[test]
fn test_multiple_checkpoints() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PersistedEngine::create(temp_dir.path()).unwrap();

    engine.create_table(create_test_schema()).unwrap();

    // 第一次写入
    for i in 0..3 {
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::integer(i));
        row.insert("name".to_string(), DbValue::text(format!("User{}", i)));
        row.insert("age".to_string(), DbValue::integer(20 + i));
        engine.insert("users", row).unwrap();
    }

    engine.force_checkpoint().unwrap();

    // 第二次写入
    for i in 3..6 {
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::integer(i));
        row.insert("name".to_string(), DbValue::text(format!("User{}", i)));
        row.insert("age".to_string(), DbValue::integer(20 + i));
        engine.insert("users", row).unwrap();
    }

    engine.force_checkpoint().unwrap();

    // 重启验证
    drop(engine);
    let engine = PersistedEngine::open(temp_dir.path()).unwrap();
    assert_eq!(engine.inner().get_row_count("users").unwrap(), 6);
}
