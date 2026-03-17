# RegulusDB

RegulusDB 是一个用 Rust 编写的高性能嵌入式数据库，支持内存存储和持久化存储两种模式。它提供了类 SQL 的 API 查询构建器、B+ 树索引、事务支持以及 WAL（预写日志）+ 快照的持久化机制。

## 特性

- **多种数据类型**: Null, Integer, Real, Text, Blob, Boolean, Date, Datetime
- **灵活的存储引擎**:
  - `MemoryEngine`: 纯内存存储，适用于临时数据和缓存场景
  - `PersistedEngine`: 持久化存储，支持崩溃恢复
- **WAL + 快照持久化**:
  - 预写日志（WAL）确保数据持久性
  - 定期快照（Snapshot）优化恢复速度
  - 自动检查点（Checkpoint）机制（WAL > 10MB 时触发）
- **查询构建器**:
  - SELECT, UPDATE, DELETE 操作
  - 比较操作符：=, !=, <, <=, >, >=, IN, LIKE
  - 聚合函数：COUNT, SUM, AVG, MAX, MIN
  - ORDER BY, LIMIT, OFFSET
  - GROUP BY 和 HAVING 子句
  - INNER JOIN, LEFT JOIN
- **B+ 树索引**:
  - 单列索引和复合索引
  - 唯一索引支持
  - 自动索引选择优化查询
- **事务支持**: ACID 语义的事务操作

## 安装

将以下依赖添加到你的 `Cargo.toml`:

```toml
[dependencies]
regulus-db = "0.1.0"
```

## 快速开始

### 内存模式

```rust
use regulus_db::{Database, Column, DataType, DbValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new();

    // 创建表
    db.create_table("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("age", DataType::integer()),
    ])?;

    // 插入数据
    db.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
        ("age", DbValue::integer(25)),
    ])?;

    // 查询数据
    let results = db.query("users")
        .eq("age", DbValue::integer(25))
        .execute()?;

    for row in results {
        println!("Name: {:?}", row.get("name"));
    }

    Ok(())
}
```

### 持久化模式

```rust
use regulus_db::{PersistedEngine, StorageEngine, Column, DataType, DbValue, Row};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 打开（或创建）持久化数据库
    let mut engine = PersistedEngine::open(Path::new("./data"))?;

    // 创建表（自动记录 WAL）
    let schema = regulus_db::TableSchema::new("users", vec![
        Column::new("id", DataType::integer()).primary_key(),
        Column::new("name", DataType::text()),
        Column::new("age", DataType::integer()),
    ]);
    engine.create_table(schema)?;

    // 插入数据（自动记录 WAL）
    let mut row = Row::new();
    row.insert("id".to_string(), DbValue::integer(1));
    row.insert("name".to_string(), DbValue::text("Alice"));
    row.insert("age".to_string(), DbValue::integer(25));
    engine.insert("users", row)?;

    // 手动触发检查点（可选，WAL > 10MB 时会自动触发）
    engine.force_checkpoint()?;

    Ok(())
}
```

## 数据类型

| 类型 | 描述 | 示例 |
|------|------|------|
| `Null` | 空值 | `DbValue::Null` |
| `Integer` | 64 位有符号整数 | `DbValue::integer(42)` |
| `Real` | 64 位浮点数 | `DbValue::real(3.14)` |
| `Text` | UTF-8 字符串 | `DbValue::text("hello")` |
| `Blob` | 二进制数据 | `DbValue::blob(vec![1, 2, 3])` |
| `Boolean` | 布尔值 | `DbValue::boolean(true)` |
| `Date` | 日期（自 1970-01-01 的天数） | `DbValue::date(19000)` |
| `Datetime` | 日期时间（自 1970-01-01 的毫秒数） | `DbValue::datetime(1609459200000)` |

## 查询构建器

### 基本查询

```rust
// 查询所有用户
let all = db.query("users").execute()?;

// 条件查询
let adults = db.query("users")
    .ge("age", DbValue::integer(18))
    .execute()?;

// 多个条件
let result = db.query("users")
    .ge("age", DbValue::integer(18))
    .eq("active", DbValue::boolean(true))
    .execute()?;
```

### 比较操作符

```rust
db.query("users").eq("id", DbValue::integer(1));      // =
db.query("users").ne("id", DbValue::integer(1));      // !=
db.query("users").lt("age", DbValue::integer(18));    // <
db.query("users").le("age", DbValue::integer(18));    // <=
db.query("users").gt("age", DbValue::integer(18));    // >
db.query("users").ge("age", DbValue::integer(18));    // >=
db.query("users").in_list("id", vec![DbValue::integer(1), DbValue::integer(2)]);  // IN
db.query("users").contains("name", "Ali");            // LIKE (包含)
```

### 排序和分页

```rust
// 按年龄降序排序
let sorted = db.query("users")
    .order_by("age", Order::Desc)
    .execute()?;

// 分页：每页 10 条，第 3 页
let page = db.query("users")
    .order_by("id", Order::Asc)
    .limit(10)
    .offset(20)  // 跳过前 20 条
    .execute()?;
```

### 聚合函数

```rust
use regulus_db::Aggregate;

// COUNT
let count = db.query("users")
    .select_with_aggregates(&[Aggregate::count("*")])
    .execute()?;

// AVG
let avg_age = db.query("users")
    .select_with_aggregates(&[Aggregate::avg("age")])
    .execute()?;

// SUM, MAX, MIN
let stats = db.query("orders")
    .select_with_aggregates(&[
        Aggregate::sum("amount"),
        Aggregate::max("amount"),
        Aggregate::min("amount"),
    ])
    .execute()?;
```

### GROUP BY 和 HAVING

```rust
use regulus_db::Aggregate;

// 按部门分组，统计每部门人数
let grouped = db.query("employees")
    .select(&["department"])
    .select_with_aggregates(&[Aggregate::count("*")])
    .group_by(&["department"])
    .execute()?;

// HAVING 子句过滤分组
let high_departments = db.query("employees")
    .select(&["department"])
    .select_with_aggregates(&[Aggregate::avg("salary")])
    .group_by(&["department"])
    .having(Aggregate::avg("salary").gt(DbValue::real(50000.0)))
    .execute()?;
```

### JOIN 操作

```rust
// INNER JOIN
let results = db.query("users")
    .inner_join("orders", "users.id", "orders.user_id")
    .execute()?;

// LEFT JOIN
let results = db.query("users")
    .left_join("orders", "users.id", "orders.user_id")
    .execute()?;

// 带字段选择和过滤
let results = db.query("users")
    .inner_join("orders", "users.id", "orders.user_id")
    .select(&["users.name", "orders.product", "orders.amount"])
    .gt("orders.amount", DbValue::integer(100))
    .order_by("orders.amount", Order::Desc)
    .execute()?;
```

## 更新和删除

### UPDATE

```rust
// 更新单个字段
db.update("users")
    .eq("id", DbValue::integer(1))
    .set("age", DbValue::integer(26))
    .execute()?;

// 更新多个字段
db.update("users")
    .eq("id", DbValue::integer(1))
    .set("age", DbValue::integer(26))
    .set("name", DbValue::text("Alicia"))
    .execute()?;
```

### DELETE

```rust
// 删除单条
let deleted = db.delete("users")
    .eq("id", DbValue::integer(1))
    .execute()?;

// 批量删除
let deleted = db.delete("users")
    .lt("age", DbValue::integer(18))
    .execute()?;
```

## 索引

### 创建索引

```rust
// 单列索引
db.create_index("users", "name")?;

// 复合索引
db.create_composite_index("users", &["last_name", "first_name"])?;

// 唯一复合索引
db.create_unique_index("users", &["email"])?;
```

### 删除索引

```rust
// 删除单列索引
db.drop_index("users", "name")?;

// 删除复合索引
db.drop_composite_index("users", &["last_name", "first_name"])?;
```

### 检查索引

```rust
if db.has_index("users", "name") {
    println!("索引存在");
}

if db.has_composite_index("users", &["last_name", "first_name"]) {
    println!("复合索引存在");
}
```

## 事务

```rust
db.transaction(|tx| {
    // 事务内的所有操作要么全部成功，要么全部回滚
    tx.insert("users", vec![
        ("id", DbValue::integer(1)),
        ("name", DbValue::text("Alice")),
    ])?;

    tx.insert("users", vec![
        ("id", DbValue::integer(2)),
        ("name", DbValue::text("Bob")),
    ])?;

    // 查询事务内的数据（包括未提交的）
    let count = tx.query("users").execute()?.len();
    println!("事务内用户数：{}", count);

    Ok(())  // 提交事务
    // 或者返回 Err 来回滚事务
})?;
```

## 持久化机制

### 文件结构

RegulusDB 使用两个文件进行持久化：

| 文件 | 说明 |
|------|------|
| `data.rdb` | 数据快照文件，包含完整的数据状态 |
| `data.rdb.wal` | 预写日志文件，记录所有数据变更操作 |

### 恢复流程

1. 加载 `data.rdb` 快照到内存
2. 重放 `data.rdb.wal` 中的所有操作
3. 数据恢复完成

### 检查点（Checkpoint）

- **自动触发**: 当 WAL 文件大小超过 10MB 时自动触发
- **手动触发**: 调用 `engine.force_checkpoint()?`
- **过程**: 保存快照 → 截断 WAL → 重置计数器

```rust
use regulus_db::{PersistedEngine, StorageEngine};

let mut engine = PersistedEngine::open(Path::new("./data"))?;

// 执行一些操作...

// 手动触发检查点
engine.force_checkpoint()?;

// 检查是否需要检查点
if engine.wal_size() > 0 {
    println!("WAL 大小：{} bytes", engine.wal_size());
}
```

## 测试

运行所有测试：

```bash
cargo test
```

运行特定测试：

```bash
cargo test test_persisted_recovery
```

运行基准测试：

```bash
cargo bench
```

## 许可证

本项目采用 MIT 许可证。详见 LICENSE 文件。

## 致谢

RegulusDB 的设计受到了以下数据库的启发：

- **SQLite**: WAL 模式和 B+ 树索引
- **Redis**: RDB 快照和 AOF 日志
- **PostgreSQL**: MVCC 和事务处理
