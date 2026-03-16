use crate::storage::{MemoryEngine, Row, StorageEngine};
use crate::types::{DbValue, DbResult};
use std::sync::{Arc, RwLock};

/// 排序方向
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Order {
    Asc,
    Desc,
}

/// 过滤表达式 AST
#[derive(Debug, Clone)]
pub enum FilterExpr {
    Eq { field: String, value: DbValue },
    Ne { field: String, value: DbValue },
    Lt { field: String, value: DbValue },
    Le { field: String, value: DbValue },
    Gt { field: String, value: DbValue },
    Ge { field: String, value: DbValue },
    In { field: String, values: Vec<DbValue> },
    Contains { field: String, value: String },
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
}

/// 评估过滤表达式
fn evaluate_filter(expr: &FilterExpr, row: &Row) -> bool {
    match expr {
        FilterExpr::Eq { field, value } => {
            row.get(field).map(|v| v == value).unwrap_or(false)
        }
        FilterExpr::Ne { field, value } => {
            row.get(field).map(|v| v != value).unwrap_or(true)
        }
        FilterExpr::Lt { field, value } => compare_field(row, field, value, |a, b| a < b),
        FilterExpr::Le { field, value } => compare_field(row, field, value, |a, b| a <= b),
        FilterExpr::Gt { field, value } => compare_field(row, field, value, |a, b| a > b),
        FilterExpr::Ge { field, value } => compare_field(row, field, value, |a, b| a >= b),
        FilterExpr::In { field, values } => {
            row.get(field).map(|v| values.contains(v)).unwrap_or(false)
        }
        FilterExpr::Contains { field, value } => {
            row.get(field)
                .and_then(|v| v.as_text())
                .map(|s| s.contains(value))
                .unwrap_or(false)
        }
        FilterExpr::And(left, right) => {
            evaluate_filter(left, row) && evaluate_filter(right, row)
        }
        FilterExpr::Or(left, right) => {
            evaluate_filter(left, row) || evaluate_filter(right, row)
        }
        FilterExpr::Not(inner) => {
            !evaluate_filter(inner, row)
        }
    }
}

/// 比较字段值（支持数值比较）
fn compare_field<F>(row: &Row, field: &str, value: &DbValue, cmp: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    let row_val = match row.get(field) {
        Some(v) => v,
        None => return false,
    };

    let row_num = match row_val {
        DbValue::Integer(i) => *i as f64,
        DbValue::Real(r) => *r,
        _ => return false,
    };

    let cmp_num = match value {
        DbValue::Integer(i) => *i as f64,
        DbValue::Real(r) => *r,
        _ => return false,
    };

    cmp(row_num, cmp_num)
}

/// 查询构建器
pub struct QueryBuilder {
    table: String,
    filters: Vec<FilterExpr>,
    order_by: Option<(String, Order)>,
    limit: Option<usize>,
    offset: Option<usize>,
    engine: Arc<RwLock<MemoryEngine>>,
}

impl QueryBuilder {
    pub fn new(table: String, engine: Arc<RwLock<MemoryEngine>>) -> Self {
        QueryBuilder {
            table,
            filters: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
            engine,
        }
    }

    // 过滤条件
    pub fn eq(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Eq {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn ne(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Ne {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn lt(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Lt {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn le(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Le {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn gt(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Gt {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn ge(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Ge {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn in_list(mut self, field: &str, values: Vec<DbValue>) -> Self {
        self.filters.push(FilterExpr::In {
            field: field.to_string(),
            values,
        });
        self
    }

    pub fn contains(mut self, field: &str, value: &str) -> Self {
        self.filters.push(FilterExpr::Contains {
            field: field.to_string(),
            value: value.to_string(),
        });
        self
    }

    // 逻辑组合
    pub fn and(mut self, other: QueryBuilder) -> Self {
        // 合并另一个 QueryBuilder 的过滤条件
        for filter in other.filters {
            self.filters.push(filter);
        }
        self
    }

    // 排序
    pub fn order_by(mut self, field: &str, order: Order) -> Self {
        self.order_by = Some((field.to_string(), order));
        self
    }

    // 分页
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// 执行查询
    pub fn execute(self) -> DbResult<Vec<Row>> {
        let engine = self.engine.read().unwrap();
        let rows = engine.scan(&self.table)?;

        // 过滤
        let mut filtered: Vec<Row> = rows
            .into_iter()
            .filter(|(_, row)| {
                self.filters.iter().all(|expr| evaluate_filter(expr, row))
            })
            .map(|(_, row)| row.clone())
            .collect();

        // 排序
        if let Some((field, order)) = self.order_by {
            filtered.sort_by(|a, b| {
                let a_val = a.get(&field);
                let b_val = b.get(&field);

                let cmp = match (a_val, b_val) {
                    (Some(DbValue::Integer(a)), Some(DbValue::Integer(b))) => a.partial_cmp(b),
                    (Some(DbValue::Real(a)), Some(DbValue::Real(b))) => a.partial_cmp(b),
                    (Some(DbValue::Integer(a)), Some(DbValue::Real(b))) => (*a as f64).partial_cmp(b),
                    (Some(DbValue::Real(a)), Some(DbValue::Integer(b))) => a.partial_cmp(&(*b as f64)),
                    (Some(DbValue::Text(a)), Some(DbValue::Text(b))) => Some(a.cmp(b)),
                    _ => Some(std::cmp::Ordering::Equal),
                };

                match order {
                    Order::Asc => cmp.unwrap_or(std::cmp::Ordering::Equal),
                    Order::Desc => cmp.unwrap_or(std::cmp::Ordering::Equal).reverse(),
                }
            });
        }

        // 分页
        let start = self.offset.unwrap_or(0);
        let end = start + self.limit.unwrap_or(filtered.len());

        Ok(filtered.into_iter().skip(start).take(end - start).collect())
    }
}

/// 更新构建器
pub struct UpdateBuilder {
    table: String,
    filters: Vec<FilterExpr>,
    values: Vec<(String, DbValue)>,
    engine: Arc<RwLock<MemoryEngine>>,
}

impl UpdateBuilder {
    pub fn new(table: String, engine: Arc<RwLock<MemoryEngine>>) -> Self {
        UpdateBuilder {
            table,
            filters: Vec::new(),
            values: Vec::new(),
            engine,
        }
    }

    pub fn eq(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Eq {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn lt(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Lt {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn gt(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Gt {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn set(mut self, field: &str, value: DbValue) -> Self {
        self.values.push((field.to_string(), value));
        self
    }

    /// 执行更新，返回受影响的行数
    pub fn execute(self) -> DbResult<usize> {
        let mut engine = self.engine.write().unwrap();
        let rows = engine.scan(&self.table)?;

        // 先收集需要更新的 row_id
        let mut to_update = Vec::new();
        for (row_id, row) in rows {
            let matches = self.filters.iter().all(|expr| evaluate_filter(expr, row));
            if matches {
                to_update.push(row_id);
            }
        }

        // 执行更新
        let mut count = 0;
        for row_id in to_update {
            let mut update_row: Row = self.values.iter().cloned().collect();
            // 保留原有行的其他字段
            if let Some(existing) = engine.get(&self.table, row_id)? {
                for (key, value) in existing {
                    if !update_row.contains_key(key) {
                        update_row.insert(key.clone(), value.clone());
                    }
                }
            }
            engine.update(&self.table, row_id, update_row)?;
            count += 1;
        }

        Ok(count)
    }
}

/// 删除构建器
pub struct DeleteBuilder {
    table: String,
    filters: Vec<FilterExpr>,
    engine: Arc<RwLock<MemoryEngine>>,
}

impl DeleteBuilder {
    pub fn new(table: String, engine: Arc<RwLock<MemoryEngine>>) -> Self {
        DeleteBuilder {
            table,
            filters: Vec::new(),
            engine,
        }
    }

    pub fn eq(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Eq {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn lt(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Lt {
            field: field.to_string(),
            value,
        });
        self
    }

    pub fn gt(mut self, field: &str, value: DbValue) -> Self {
        self.filters.push(FilterExpr::Gt {
            field: field.to_string(),
            value,
        });
        self
    }

    /// 执行删除，返回删除的行数
    pub fn execute(self) -> DbResult<usize> {
        let mut engine = self.engine.write().unwrap();
        let rows = engine.scan(&self.table)?;

        // 先收集需要删除的 row_id
        let mut to_delete = Vec::new();
        for (row_id, row) in rows {
            let matches = self.filters.iter().all(|expr| evaluate_filter(expr, row));
            if matches {
                to_delete.push(row_id);
            }
        }

        // 执行删除
        let mut count = 0;
        for row_id in to_delete {
            engine.delete(&self.table, row_id)?;
            count += 1;
        }

        Ok(count)
    }
}
