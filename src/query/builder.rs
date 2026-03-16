use crate::storage::{MemoryEngine, Row, StorageEngine, RowId};
use crate::types::{DbValue, DbResult};
use crate::index::btree::BTreeIndex;
use std::sync::{Arc, RwLock};

/// 排序方向
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Order {
    Asc,
    Desc,
}

/// JOIN 类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,   // INNER JOIN - 只返回匹配行
    Left,    // LEFT JOIN - 返回左表所有行，右表无匹配则 NULL
    Right,   // RIGHT JOIN - 返回右表所有行，左表无匹配则 NULL
}

/// 连接条件
#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub join_type: JoinType,
    pub right_table: String,
    pub left_field: String,   // 格式："table.column"
    pub right_field: String,  // 格式："table.column"
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
    joins: Vec<JoinCondition>,           // 新增：JOIN 列表
    selected_columns: Vec<String>,       // 新增：选择字段
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
            joins: Vec::new(),
            selected_columns: Vec::new(),
            filters: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
            engine,
        }
    }

    /// 查找最佳可用索引（优先等值条件，其次范围条件）
    fn find_best_index<'a>(&'a self, engine: &'a MemoryEngine) -> Option<(&'a String, &'a BTreeIndex)> {
        // 优先查找等值条件的字段
        for filter in &self.filters {
            if let FilterExpr::Eq { field, .. } = filter {
                if let Some(index) = engine.get_index(&self.table, field) {
                    return Some((field, index));
                }
            }
        }
        // 其次查找范围条件的字段
        for filter in &self.filters {
            if let FilterExpr::Gt { field, .. }
               | FilterExpr::Ge { field, .. }
               | FilterExpr::Lt { field, .. }
               | FilterExpr::Le { field, .. } = filter {
                if let Some(index) = engine.get_index(&self.table, field) {
                    return Some((field, index));
                }
            }
        }
        None
    }

    /// 使用索引执行查询（优化版本）
    fn execute_with_index(&self, engine: &MemoryEngine, _field: &str, index: &BTreeIndex) -> DbResult<Vec<Row>> {
        // 从索引中获取匹配的 row_id
        let row_ids = self.get_matching_row_ids(index);

        // 根据 row_ids 回表查询完整行数据
        let mut results = Vec::new();
        for row_id in row_ids {
            if let Some(row) = engine.get(&self.table, row_id)? {
                // 需要验证所有过滤条件（因为索引只针对一个字段）
                if self.matches_all_filters(row) {
                    results.push(row.clone());
                }
            }
        }

        // 排序
        if let Some((field, order)) = &self.order_by {
            results.sort_by(|a, b| {
                self.compare_rows(a, b, field, *order)
            });
        }

        // 分页
        let start = self.offset.unwrap_or(0);
        let end = start + self.limit.unwrap_or(results.len());

        Ok(results.into_iter().skip(start).take(end - start).collect())
    }

    /// 根据过滤条件从索引中获取匹配的 row_ids
    fn get_matching_row_ids(&self, index: &BTreeIndex) -> Vec<RowId> {
        let mut row_ids = Vec::new();

        // 收集所有针对同一字段的等值和范围条件
        let mut eq_value: Option<&DbValue> = None;
        let mut gt_value: Option<&DbValue> = None;
        let mut ge_value: Option<&DbValue> = None;
        let mut lt_value: Option<&DbValue> = None;
        let mut le_value: Option<&DbValue> = None;

        for filter in &self.filters {
            match filter {
                FilterExpr::Eq { field, value } if eq_value.is_none() => {
                    eq_value = Some(value);
                }
                FilterExpr::Gt { field: _, value } => {
                    if gt_value.is_none() || value > gt_value.unwrap() {
                        gt_value = Some(value);
                    }
                }
                FilterExpr::Ge { field: _, value } => {
                    if ge_value.is_none() || value > ge_value.unwrap() {
                        ge_value = Some(value);
                    }
                }
                FilterExpr::Lt { field: _, value } => {
                    if lt_value.is_none() || value < lt_value.unwrap() {
                        lt_value = Some(value);
                    }
                }
                FilterExpr::Le { field: _, value } => {
                    if le_value.is_none() || value < le_value.unwrap() {
                        le_value = Some(value);
                    }
                }
                _ => {}
            }
        }

        // 如果有等值条件，直接使用 search
        if let Some(eq) = eq_value {
            row_ids.extend(index.search(eq));
        } else {
            // 计算范围 [range_start, range_end)
            let range_start = gt_value.or(ge_value);
            let range_end = lt_value.or(le_value);

            if let Some(start) = range_start {
                // 需要调整范围边界（gt 需要 +1，但这里简化处理）
                if let Some(end) = range_end {
                    row_ids.extend(index.range(start, end));
                } else {
                    row_ids.extend(index.range_from(start));
                }
            } else if let Some(_end) = range_end {
                // 只有上界，从最小值开始
                // 这里需要一个最小值，简化处理：使用 range_from 然后过滤
                // 由于 DbValue 没有明确的最小值，我们直接扫描
                // 这种情况较少，暂不优化
            } else {
                // 没有范围条件，返回所有
                // 这不应该发生，因为调用者会先检查是否有可用索引
            }
        }

        row_ids
    }

    /// 检查行是否匹配所有过滤条件
    fn matches_all_filters(&self, row: &Row) -> bool {
        self.filters.iter().all(|expr| evaluate_filter(expr, row))
    }

    /// 比较两行（用于排序）
    fn compare_rows(&self, a: &Row, b: &Row, field: &str, order: Order) -> std::cmp::Ordering {
        let a_val = a.get(field);
        let b_val = b.get(field);

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
    }

    // ==================== JOIN 相关辅助方法 ====================

    /// 字段名前缀化："name" -> "table.name"
    fn prefix_row(&self, row: &Row, table: &str) -> Row {
        row.iter()
            .map(|(k, v)| (format!("{}.{}", table, k), v.clone()))
            .collect()
    }

    /// 创建 NULL 行（用于 LEFT/RIGHT JOIN 无匹配时）
    fn create_null_row(&self, engine: &MemoryEngine, table: &str) -> DbResult<Row> {
        let schema = engine.get_schema(table)?;
        let mut row = Row::new();
        for column in &schema.columns {
            row.insert(format!("{}.{}", table, column.name), DbValue::Null);
        }
        Ok(row)
    }

    /// 匹配 JOIN 条件
    fn match_join_condition(&self, left: &Row, right: &Row, join: &JoinCondition) -> bool {
        let left_val = left.get(&join.left_field);
        let right_val = right.get(&join.right_field);
        left_val == right_val
    }

    /// 执行 JOIN 查询（嵌套循环连接）
    fn execute_join(&self, engine: &MemoryEngine) -> DbResult<Vec<Row>> {
        if self.joins.is_empty() {
            return self.execute_simple_scan(engine);
        }

        // 从第一个 JOIN 开始，逐步连接所有表
        let mut results: Vec<Row> = Vec::new();

        // 扫描主表
        let main_table_rows = engine.scan(&self.table)?;

        for (_row_id, main_row) in main_table_rows {
            let main_prefixed = self.prefix_row(main_row, &self.table);

            // 递归处理所有 JOIN
            self.process_joins(engine, main_prefixed, 0, &mut results)?;
        }

        // 应用过滤条件
        let mut filtered: Vec<Row> = results
            .into_iter()
            .filter(|row| self.filters.iter().all(|expr| evaluate_filter(expr, row)))
            .collect();

        // 应用字段选择（投影）
        if !self.selected_columns.is_empty() {
            filtered = filtered
                .into_iter()
                .map(|row| {
                    let mut projected = Row::new();
                    for col in &self.selected_columns {
                        if let Some(value) = row.get(col) {
                            projected.insert(col.clone(), value.clone());
                        }
                    }
                    projected
                })
                .collect();
        }

        // 排序
        if let Some((ref field, order)) = self.order_by {
            filtered.sort_by(|a, b| self.compare_rows(a, b, field, order));
        }

        // 分页
        let start = self.offset.unwrap_or(0);
        let end = start + self.limit.unwrap_or(filtered.len());

        Ok(filtered.into_iter().skip(start).take(end - start).collect())
    }

    /// 递归处理 JOIN 链
    fn process_joins(
        &self,
        engine: &MemoryEngine,
        current_row: Row,
        join_index: usize,
        results: &mut Vec<Row>,
    ) -> DbResult<()> {
        if join_index >= self.joins.len() {
            // 所有 JOIN 处理完毕，添加结果
            results.push(current_row);
            return Ok(());
        }

        let join = &self.joins[join_index];
        let right_rows = engine.scan(&join.right_table)?;

        let mut has_match = false;

        for (_right_id, right_row) in right_rows {
            let right_prefixed = self.prefix_row(right_row, &join.right_table);

            // 检查 JOIN 条件
            if self.match_join_condition(&current_row, &right_prefixed, join) {
                has_match = true;
                // 合并行
                let mut merged = current_row.clone();
                merged.extend(right_prefixed);

                // 递归处理下一个 JOIN
                self.process_joins(engine, merged, join_index + 1, results)?;
            }
        }

        // 处理 LEFT JOIN 无匹配情况
        if !has_match && matches!(join.join_type, JoinType::Left) {
            let null_row = self.create_null_row(engine, &join.right_table)?;
            let mut merged = current_row.clone();
            merged.extend(null_row);

            // 递归处理下一个 JOIN
            self.process_joins(engine, merged, join_index + 1, results)?;
        }

        // RIGHT JOIN 处理：需要单独处理右表有但左表无匹配的行
        // 注意：RIGHT JOIN 的完整实现需要更复杂的逻辑，这里简化处理
        // 暂时不在此处处理 RIGHT JOIN，因为需要访问主表数据

        Ok(())
    }

    /// 简单全表扫描（无 JOIN 时）
    fn execute_simple_scan(&self, engine: &MemoryEngine) -> DbResult<Vec<Row>> {
        let rows = engine.scan(&self.table)?;

        let mut filtered: Vec<Row> = rows
            .into_iter()
            .filter(|(_, row)| self.filters.iter().all(|expr| evaluate_filter(expr, row)))
            .map(|(_, row)| row.clone())
            .collect();

        // 应用字段选择
        if !self.selected_columns.is_empty() {
            filtered = filtered
                .into_iter()
                .map(|row| {
                    let mut projected = Row::new();
                    for col in &self.selected_columns {
                        if let Some(value) = row.get(col) {
                            projected.insert(col.clone(), value.clone());
                        }
                    }
                    projected
                })
                .collect();
        }

        // 排序
        if let Some((ref field, order)) = self.order_by {
            filtered.sort_by(|a, b| self.compare_rows(a, b, field, order));
        }

        // 分页
        let start = self.offset.unwrap_or(0);
        let end = start + self.limit.unwrap_or(filtered.len());

        Ok(filtered.into_iter().skip(start).take(end - start).collect())
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

    // JOIN 操作
    pub fn inner_join(mut self, right_table: &str, left_field: &str, right_field: &str) -> Self {
        self.joins.push(JoinCondition {
            join_type: JoinType::Inner,
            right_table: right_table.to_string(),
            left_field: left_field.to_string(),
            right_field: right_field.to_string(),
        });
        self
    }

    pub fn left_join(mut self, right_table: &str, left_field: &str, right_field: &str) -> Self {
        self.joins.push(JoinCondition {
            join_type: JoinType::Left,
            right_table: right_table.to_string(),
            left_field: left_field.to_string(),
            right_field: right_field.to_string(),
        });
        self
    }

    pub fn right_join(mut self, right_table: &str, left_field: &str, right_field: &str) -> Self {
        self.joins.push(JoinCondition {
            join_type: JoinType::Right,
            right_table: right_table.to_string(),
            left_field: left_field.to_string(),
            right_field: right_field.to_string(),
        });
        self
    }

    // 选择字段（支持 qualified names）
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.selected_columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    /// 执行查询
    pub fn execute(self) -> DbResult<Vec<Row>> {
        let engine = self.engine.read().unwrap();

        // 检查是否有 JOIN
        if !self.joins.is_empty() {
            return self.execute_join(&engine);
        }

        // 尝试查找最佳索引
        if let Some((field, index)) = self.find_best_index(&engine) {
            // 使用索引优化查询
            return self.execute_with_index(&engine, field, index);
        }

        // 降级为全表扫描
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
        if let Some((ref field, order)) = self.order_by {
            filtered.sort_by(|a, b| {
                self.compare_rows(a, b, field, order)
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
