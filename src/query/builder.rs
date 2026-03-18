use crate::storage::{MemoryEngine, Row, StorageEngine, RowId};
use crate::types::{DbValue, DbResult};
use crate::index::btree::BTreeIndex;
use crate::index::manager::IndexMeta;
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
    Full,    // FULL OUTER JOIN - 返回左右表所有行，无匹配时双方填充 NULL
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

/// 聚合函数类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

/// 聚合表达式
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFunction,
    pub column: Option<String>,  // COUNT(*) 为 None
    pub alias: Option<String>,   // 可选的别名
}

/// HAVING 子句表达式（用于过滤分组）
#[derive(Debug, Clone)]
pub enum HavingExpr {
    Eq { value: DbValue },
    Ne { value: DbValue },
    Lt { value: DbValue },
    Le { value: DbValue },
    Gt { value: DbValue },
    Ge { value: DbValue },
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
    // 聚合函数相关
    aggregate: Option<AggregateExpr>,    // 聚合函数
    group_by: Vec<String>,               // GROUP BY 字段
    having: Option<HavingExpr>,          // HAVING 子句
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
            aggregate: None,
            group_by: Vec::new(),
            having: None,
        }
    }

    /// 查找最佳可用索引（优先等值条件，其次范围条件）
    /// 返回：(索引列名，索引对象，构建的复合键值)
    fn find_best_index<'a>(&'a self, engine: &'a MemoryEngine) -> Option<(&'a IndexMeta, &'a BTreeIndex, Vec<DbValue>)> {
        // 收集所有等值条件
        let eq_filters: Vec<&FilterExpr> = self.filters.iter().filter(|f| matches!(f, FilterExpr::Eq { .. })).collect();

        // 收集所有范围条件
        let range_filters: Vec<&FilterExpr> = self.filters.iter().filter(|f| {
            matches!(f, FilterExpr::Gt { .. } | FilterExpr::Ge { .. } | FilterExpr::Lt { .. } | FilterExpr::Le { .. })
        }).collect();

        // 使用引擎的 find_best_index 方法查找最佳复合索引
        if !eq_filters.is_empty() {
            let eq_columns: Vec<&str> = eq_filters.iter().map(|f| {
                if let FilterExpr::Eq { field, .. } = f {
                    field.as_str()
                } else {
                    ""
                }
            }).collect();

            if let Some((meta, index)) = engine.find_best_index(&self.table, &eq_columns) {
                // 构建复合键值
                let mut key_values = Vec::new();
                for col in &meta.columns {
                    let value = eq_filters.iter()
                        .find_map(|f| {
                            if let FilterExpr::Eq { field, value } = f {
                                if field == col { Some(value) } else { None }
                            } else { None }
                        });
                    if let Some(v) = value {
                        key_values.push(v.clone());
                    } else {
                        // 对于前缀匹配，后续列不需要
                        break;
                    }
                }
                if !key_values.is_empty() {
                    return Some((meta, index, key_values));
                }
            }
        }

        // 检查范围条件（暂时不处理，由单列索引方法处理）
        if let Some(_filter) = range_filters.first() {
            // 范围条件的索引查找在 find_best_single_index 中处理
        }

        None
    }

    /// 查找最佳单列索引（向后兼容）
    fn find_best_single_index<'a>(&'a self, engine: &'a MemoryEngine) -> Option<(&'a String, &'a BTreeIndex)> {
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
                if self.matches_all_filters(&row) {
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

    /// 使用复合索引执行查询
    fn execute_with_composite_index(&self, engine: &MemoryEngine, _meta: &IndexMeta, index: &BTreeIndex, key_values: &[DbValue]) -> DbResult<Vec<Row>> {
        // 从复合索引中获取匹配的 row_id
        let row_ids = index.search_composite(key_values);

        // 根据 row_ids 回表查询完整行数据
        let mut results = Vec::new();
        for row_id in row_ids {
            if let Some(row) = engine.get(&self.table, row_id)? {
                // 验证所有过滤条件
                if self.matches_all_filters(&row) {
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
            .collect::<Row>()
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

    /// 从 qualified field name 提取列名
    /// "table.column" -> "column"
    /// "column" -> "column"
    fn extract_column_name(field: &str) -> &str {
        if let Some(pos) = field.rfind('.') {
            &field[pos + 1..]
        } else {
            field
        }
    }

    /// 优化 JOIN 顺序
    /// 策略：选择最小的表作为驱动表，减少中间结果集
    fn optimize_join_order(&self, engine: &MemoryEngine) -> DbResult<Vec<JoinCondition>> {
        // 1. 收集所有涉及的表及其大小
        let mut tables: Vec<(String, usize)> = Vec::new();

        // 主表
        let main_count = engine.get_row_count(&self.table)?;
        tables.push((self.table.clone(), main_count));

        // JOIN 表
        for join in &self.joins {
            if !tables.iter().any(|(name, _)| name == &join.right_table) {
                let count = engine.get_row_count(&join.right_table)?;
                tables.push((join.right_table.clone(), count));
            }
        }

        // 2. 按表大小排序（从小到大）
        tables.sort_by_key(|(_, count)| *count);

        // 3. 找出最小的右表
        let smallest_table = tables.first()
            .map(|(name, _)| name.clone());

        // 4. 重排 JOIN 顺序：将最小表对应的 JOIN 交换到前面
        let mut optimized_joins = self.joins.clone();

        if let Some(smallest) = smallest_table {
            // 找到包含最小表的 JOIN
            if let Some(min_idx) = optimized_joins.iter().position(|j| j.right_table == smallest) {
                optimized_joins.swap(0, min_idx);
            }
        }

        Ok(optimized_joins)
    }

    /// 执行 JOIN 查询（嵌套循环连接）
    fn execute_join(&self, engine: &MemoryEngine) -> DbResult<Vec<Row>> {
        if self.joins.is_empty() {
            return self.execute_simple_scan(engine);
        }

        // 检查是否有 RIGHT JOIN 或 FULL OUTER JOIN
        let has_right_join = self.joins.iter().any(|j| matches!(j.join_type, JoinType::Right));
        let has_full_join = self.joins.iter().any(|j| matches!(j.join_type, JoinType::Full));

        // 从第一个 JOIN 开始，逐步连接所有表
        let mut results: Vec<Row> = Vec::new();

        if has_right_join || has_full_join {
            // RIGHT JOIN 和 FULL OUTER JOIN 需要特殊处理：以右表为驱动表
            self.execute_right_or_full_join(engine, &mut results)?;
        } else {
            // 优化 JOIN 顺序：选择最小的表作为驱动表
            let optimized_joins = self.optimize_join_order(engine)?;

            // 扫描主表
            let main_table_rows = engine.scan(&self.table)?;

            for (_row_id, main_row) in main_table_rows {
                let main_prefixed = self.prefix_row(&main_row, &self.table);

                // 使用优化后的 JOIN 顺序
                self.process_joins_with_order(engine, main_prefixed, 0, &mut results, &optimized_joins)?;
            }
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

    /// 执行 RIGHT JOIN 或 FULL OUTER JOIN（以右表为驱动表）
    fn execute_right_or_full_join(&self, engine: &MemoryEngine, results: &mut Vec<Row>) -> DbResult<()> {
        // 找到第一个 RIGHT JOIN 或 FULL OUTER JOIN
        let right_or_full_join_index = self.joins.iter().position(|j| {
            matches!(j.join_type, JoinType::Right | JoinType::Full)
        });

        if let Some(join_idx) = right_or_full_join_index {
            let join = &self.joins[join_idx];
            let is_full = matches!(join.join_type, JoinType::Full);

            // 扫描右表
            let right_table_rows = engine.scan(&join.right_table)?;

            // 扫描左表用于匹配
            let left_table_rows = if join_idx == 0 {
                engine.scan(&self.table)?
            } else {
                // 多表 JOIN 场景：左表是前面 JOIN 的结果
                // 简化处理：先不支持复杂的链式 RIGHT/FULL JOIN
                Vec::new()
            };

            // 跟踪已匹配的左表行（用于 FULL OUTER JOIN）
            let mut matched_left_row_ids: std::collections::HashSet<RowId> = std::collections::HashSet::new();

            // 处理右表的每一行
            for (_right_id, right_row) in right_table_rows {
                let right_prefixed = self.prefix_row(&right_row, &join.right_table);

                // 为每个右表行查找左表匹配
                let mut has_match = false;

                for (left_id, left_row) in &left_table_rows {
                    let left_prefixed = self.prefix_row(&left_row, &self.table);

                    if self.match_join_condition(&left_prefixed, &right_prefixed, join) {
                        has_match = true;
                        matched_left_row_ids.insert(*left_id);
                        let mut merged = left_prefixed.clone();
                        merged.extend(right_prefixed.clone());

                        // 处理后续 JOIN（如果有）
                        self.process_joins_right_full(engine, merged, join_idx + 1, results, is_full)?;
                    }
                }

                // 无匹配：左表为 NULL，右表保留
                if !has_match {
                    let null_row = self.create_null_row(engine, &self.table)?;
                    let mut merged = null_row;
                    merged.extend(right_prefixed);

                    // 处理后续 JOIN
                    self.process_joins_right_full(engine, merged, join_idx + 1, results, is_full)?;
                }
            }

            // FULL OUTER JOIN：处理左表中未匹配的行
            if is_full {
                for (left_id, left_row) in &left_table_rows {
                    if !matched_left_row_ids.contains(left_id) {
                        let left_prefixed = self.prefix_row(&left_row, &self.table);
                        let null_row = self.create_null_row(engine, &join.right_table)?;
                        let mut merged = left_prefixed;
                        merged.extend(null_row);

                        // 处理后续 JOIN
                        self.process_joins_right_full(engine, merged, join_idx + 1, results, is_full)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// 递归处理 JOIN 链（用于 RIGHT/FULL JOIN）
    fn process_joins_right_full(
        &self,
        engine: &MemoryEngine,
        current_row: Row,
        join_index: usize,
        results: &mut Vec<Row>,
        is_full_context: bool,
    ) -> DbResult<()> {
        if join_index >= self.joins.len() {
            results.push(current_row);
            return Ok(());
        }

        let join = &self.joins[join_index];

        // 如果是 FULL OUTER JOIN 上下文，且当前也是 FULL OUTER JOIN
        if is_full_context && matches!(join.join_type, JoinType::Full) {
            // 简化处理：将 FULL OUTER JOIN 视为 LEFT JOIN 处理后续
            // 完整的实现需要更复杂的状态跟踪
        }

        // 尝试使用索引优化
        let right_column = Self::extract_column_name(&join.right_field);
        if let Some(index) = engine.get_index(&join.right_table, right_column) {
            // 有索引，使用索引优化
            self.process_join_with_index_impl(engine, current_row, join_index, results, join, right_column, index)
        } else {
            // 无索引，降级为全表扫描
            self.process_join_scan_impl(engine, current_row, join_index, results, join)
        }
    }

    /// 递归处理 JOIN 链（使用优化后的顺序）
    fn process_joins_with_order(
        &self,
        engine: &MemoryEngine,
        current_row: Row,
        join_index: usize,
        results: &mut Vec<Row>,
        optimized_joins: &[JoinCondition],
    ) -> DbResult<()> {
        if join_index >= optimized_joins.len() {
            // 所有 JOIN 处理完毕，添加结果
            results.push(current_row);
            return Ok(());
        }

        let join = &optimized_joins[join_index];

        // RIGHT JOIN 需要特殊处理
        if matches!(join.join_type, JoinType::Right) {
            return self.process_joins_with_order(engine, current_row, join_index + 1, results, optimized_joins);
        }

        // 尝试使用索引优化
        let right_column = Self::extract_column_name(&join.right_field);
        if let Some(index) = engine.get_index(&join.right_table, right_column) {
            self.process_join_with_index_impl_optimized(engine, current_row, join_index, results, join, right_column, index, optimized_joins)
        } else {
            self.process_join_scan_impl_optimized(engine, current_row, join_index, results, join, optimized_joins)
        }
    }

    /// 使用全表扫描执行 JOIN（优化版本，支持自定义 JOIN 顺序）
    fn process_join_scan_impl_optimized(
        &self,
        engine: &MemoryEngine,
        current_row: Row,
        join_index: usize,
        results: &mut Vec<Row>,
        join: &JoinCondition,
        optimized_joins: &[JoinCondition],
    ) -> DbResult<()> {
        let right_rows = engine.scan(&join.right_table)?;

        let mut has_match = false;

        for (_right_id, right_row) in right_rows {
            let right_prefixed = self.prefix_row(&right_row, &join.right_table);

            // 检查 JOIN 条件
            if self.match_join_condition(&current_row, &right_prefixed, join) {
                has_match = true;
                // 合并行
                let mut merged = current_row.clone();
                merged.extend(right_prefixed);

                // 递归处理下一个 JOIN（使用优化后的顺序）
                self.process_joins_with_order(engine, merged, join_index + 1, results, optimized_joins)?;
            }
        }

        // 处理 LEFT JOIN 无匹配情况
        if !has_match && matches!(join.join_type, JoinType::Left) {
            let null_row = self.create_null_row(engine, &join.right_table)?;
            let mut merged = current_row.clone();
            merged.extend(null_row);

            // 递归处理下一个 JOIN（使用优化后的顺序）
            self.process_joins_with_order(engine, merged, join_index + 1, results, optimized_joins)?;
        }

        Ok(())
    }

    /// 使用索引执行 JOIN（优化版本，支持自定义 JOIN 顺序）
    fn process_join_with_index_impl_optimized(
        &self,
        engine: &MemoryEngine,
        current_row: Row,
        join_index: usize,
        results: &mut Vec<Row>,
        join: &JoinCondition,
        _right_column: &str,
        index: &BTreeIndex,
        optimized_joins: &[JoinCondition],
    ) -> DbResult<()> {
        // 从左表获取 JOIN 字段值
        let join_value = match current_row.get(&join.left_field) {
            Some(v) => v,
            None => return Ok(()),  // 左表字段为 NULL，无法匹配
        };

        // 使用索引查找匹配的 RowId（O(log n)）
        let matching_row_ids = index.search(join_value);

        let mut has_match = false;

        for row_id in matching_row_ids {
            has_match = true;
            if let Some(right_row) = engine.get(&join.right_table, row_id)? {
                let right_prefixed = self.prefix_row(&right_row, &join.right_table);
                let mut merged = current_row.clone();
                merged.extend(right_prefixed);

                // 递归处理下一个 JOIN（使用优化后的顺序）
                self.process_joins_with_order(engine, merged, join_index + 1, results, optimized_joins)?;
            }
        }

        // LEFT JOIN 无匹配处理
        if !has_match && matches!(join.join_type, JoinType::Left) {
            let null_row = self.create_null_row(engine, &join.right_table)?;
            let mut merged = current_row.clone();
            merged.extend(null_row);

            // 递归处理下一个 JOIN（使用优化后的顺序）
            self.process_joins_with_order(engine, merged, join_index + 1, results, optimized_joins)?;
        }

        Ok(())
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

        // RIGHT JOIN 需要特殊处理（在 execute_right_join 中已处理）
        // 这里处理 INNER JOIN 和 LEFT JOIN
        if matches!(join.join_type, JoinType::Right) {
            // RIGHT JOIN 已在 execute_right_join 中处理，这里只需要处理后续的非 RIGHT JOIN
            // 递归处理下一个 JOIN
            return self.process_joins(engine, current_row, join_index + 1, results);
        }

        // 尝试使用索引优化
        let right_column = Self::extract_column_name(&join.right_field);
        if let Some(index) = engine.get_index(&join.right_table, right_column) {
            // 有索引，使用索引优化
            self.process_join_with_index_impl(engine, current_row, join_index, results, join, right_column, index)
        } else {
            // 无索引，降级为全表扫描
            self.process_join_scan_impl(engine, current_row, join_index, results, join)
        }
    }

    /// 使用全表扫描执行 JOIN（降级方案）
    fn process_join_scan_impl(
        &self,
        engine: &MemoryEngine,
        current_row: Row,
        join_index: usize,
        results: &mut Vec<Row>,
        join: &JoinCondition,
    ) -> DbResult<()> {
        let right_rows = engine.scan(&join.right_table)?;

        let mut has_match = false;

        for (_right_id, right_row) in right_rows {
            let right_prefixed = self.prefix_row(&right_row, &join.right_table);

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

        Ok(())
    }

    /// 使用索引执行 JOIN（优化方案）
    fn process_join_with_index_impl(
        &self,
        engine: &MemoryEngine,
        current_row: Row,
        join_index: usize,
        results: &mut Vec<Row>,
        join: &JoinCondition,
        _right_column: &str,
        index: &BTreeIndex,
    ) -> DbResult<()> {
        // 从左表获取 JOIN 字段值
        let join_value = match current_row.get(&join.left_field) {
            Some(v) => v,
            None => return Ok(()),  // 左表字段为 NULL，无法匹配
        };

        // 使用索引查找匹配的 RowId（O(log n)）
        let matching_row_ids = index.search(join_value);

        let mut has_match = false;

        for row_id in matching_row_ids {
            has_match = true;
            if let Some(right_row) = engine.get(&join.right_table, row_id)? {
                let right_prefixed = self.prefix_row(&right_row, &join.right_table);
                let mut merged = current_row.clone();
                for (k, v) in right_prefixed {
                    merged.insert(k, v);
                }

                // 递归处理下一个 JOIN
                self.process_joins(engine, merged, join_index + 1, results)?;
            }
        }

        // LEFT JOIN 无匹配处理
        if !has_match && matches!(join.join_type, JoinType::Left) {
            let null_row = self.create_null_row(engine, &join.right_table)?;
            let mut merged = current_row.clone();
            for (k, v) in null_row {
                merged.insert(k, v);
            }

            // 递归处理下一个 JOIN
            self.process_joins(engine, merged, join_index + 1, results)?;
        }

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

    /// OR 条件：将两个过滤条件用 OR 连接
    ///
    /// # 示例
    ///
    /// 忽略编译的示例，完整代码请参见测试文件：
    /// ```rust,no_run
    /// # use regulus_db::{Database, DbValue, FilterExpr};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Database::new();
    /// // 查询 age > 18 OR status = 'vip' 的用户
    /// db.query("users")
    ///     .or(
    ///         FilterExpr::Gt { field: "age".to_string(), value: DbValue::integer(18) },
    ///         FilterExpr::Eq { field: "status".to_string(), value: DbValue::text("vip") }
    ///     )
    ///     .execute()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn or(mut self, left: FilterExpr, right: FilterExpr) -> Self {
        self.filters.push(FilterExpr::Or(Box::new(left), Box::new(right)));
        self
    }

    /// NOT 条件：对一个过滤条件取反
    ///
    /// # 示例
    ///
    /// 忽略编译的示例，完整代码请参见测试文件：
    /// ```rust,no_run
    /// # use regulus_db::{Database, DbValue, FilterExpr};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Database::new();
    /// // 查询 status != 'deleted' 的用户
    /// db.query("users")
    ///     .not(FilterExpr::Eq { field: "status".to_string(), value: DbValue::text("deleted") })
    ///     .execute()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn not(mut self, expr: FilterExpr) -> Self {
        self.filters.push(FilterExpr::Not(Box::new(expr)));
        self
    }

    /// 添加任意过滤表达式
    ///
    /// # 示例
    ///
    /// 忽略编译的示例，完整代码请参见测试文件：
    /// ```rust,no_run
    /// # use regulus_db::{Database, DbValue, FilterExpr};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Database::new();
    /// // 添加 OR 条件
    /// db.query("users")
    ///     .where_expr(FilterExpr::Or(
    ///         Box::new(FilterExpr::Gt { field: "age".to_string(), value: DbValue::integer(18) }),
    ///         Box::new(FilterExpr::Eq { field: "status".to_string(), value: DbValue::text("vip") })
    ///     ))
    ///     .execute()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn where_expr(mut self, expr: FilterExpr) -> Self {
        self.filters.push(expr);
        self
    }

    /// 构建 OR 条件（便捷方法）
    ///
    /// # 示例
    ///
    /// 忽略编译的示例，完整代码请参见测试文件：
    /// ```rust,no_run
    /// # use regulus_db::{Database, DbValue};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Database::new();
    /// // 查询 age > 18 OR age < 10 的用户
    /// db.query("users")
    ///     .or_simple(|q| q.gt("age", DbValue::integer(18)), |q| q.lt("age", DbValue::integer(10)))
    ///     .execute()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn or_simple<F1, F2>(self, left_builder: F1, right_builder: F2) -> Self
    where
        F1: FnOnce(QueryBuilder) -> QueryBuilder,
        F2: FnOnce(QueryBuilder) -> QueryBuilder,
    {
        // 构建左边的条件
        let left_qb = left_builder(QueryBuilder::new(self.table.clone(), Arc::clone(&self.engine)));
        // 构建右边的条件
        let right_qb = right_builder(QueryBuilder::new(self.table.clone(), Arc::clone(&self.engine)));

        // 合并左右条件为 OR
        let left_filters: Vec<FilterExpr> = left_qb.filters;
        let right_filters: Vec<FilterExpr> = right_qb.filters;

        // 将多个 filters 用 AND 连接
        let left_expr = left_filters.into_iter().reduce(|a, b| FilterExpr::And(Box::new(a), Box::new(b)));
        let right_expr = right_filters.into_iter().reduce(|a, b| FilterExpr::And(Box::new(a), Box::new(b)));

        match (left_expr, right_expr) {
            (Some(left), Some(right)) => {
                let mut new_self = self;
                new_self.filters.push(FilterExpr::Or(Box::new(left), Box::new(right)));
                new_self
            }
            (Some(left), None) => {
                let mut new_self = self;
                new_self.filters.push(left);
                new_self
            }
            (None, Some(right)) => {
                let mut new_self = self;
                new_self.filters.push(right);
                new_self
            }
            (None, None) => self,
        }
    }

    /// 构建 NOT 条件（便捷方法）
    ///
    /// # 示例
    ///
    /// 忽略编译的示例，完整代码请参见测试文件：
    /// ```rust,no_run
    /// # use regulus_db::{Database, DbValue};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Database::new();
    /// // 查询 status != 'deleted' 的用户
    /// db.query("users")
    ///     .not_simple(|q| q.eq("status", DbValue::text("deleted")))
    ///     .execute()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn not_simple<F>(self, builder: F) -> Self
    where
        F: FnOnce(QueryBuilder) -> QueryBuilder,
    {
        // 构建内部条件
        let inner_qb = builder(QueryBuilder::new(self.table.clone(), Arc::clone(&self.engine)));
        let inner_filters: Vec<FilterExpr> = inner_qb.filters;

        // 将多个 filter 用 AND 连接
        let inner_expr = inner_filters.into_iter().reduce(|a, b| FilterExpr::And(Box::new(a), Box::new(b)));

        match inner_expr {
            Some(expr) => {
                let mut new_self = self;
                new_self.filters.push(FilterExpr::Not(Box::new(expr)));
                new_self
            }
            None => self,
        }
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

    pub fn full_join(mut self, right_table: &str, left_field: &str, right_field: &str) -> Self {
        self.joins.push(JoinCondition {
            join_type: JoinType::Full,
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

    // ==================== 聚合函数方法 ====================

    /// COUNT(*) - 统计行数
    pub fn count(mut self) -> Self {
        self.aggregate = Some(AggregateExpr {
            func: AggregateFunction::Count,
            column: None,  // COUNT(*) 不需要列名
            alias: None,
        });
        self
    }

    /// COUNT(column) - 统计非 NULL 行数
    pub fn count_column(mut self, column: &str) -> Self {
        self.aggregate = Some(AggregateExpr {
            func: AggregateFunction::Count,
            column: Some(column.to_string()),
            alias: None,
        });
        self
    }

    /// SUM(column) - 求和
    pub fn sum(mut self, column: &str) -> Self {
        self.aggregate = Some(AggregateExpr {
            func: AggregateFunction::Sum,
            column: Some(column.to_string()),
            alias: None,
        });
        self
    }

    /// AVG(column) - 平均值
    pub fn avg(mut self, column: &str) -> Self {
        self.aggregate = Some(AggregateExpr {
            func: AggregateFunction::Avg,
            column: Some(column.to_string()),
            alias: None,
        });
        self
    }

    /// MAX(column) - 最大值
    pub fn max(mut self, column: &str) -> Self {
        self.aggregate = Some(AggregateExpr {
            func: AggregateFunction::Max,
            column: Some(column.to_string()),
            alias: None,
        });
        self
    }

    /// MIN(column) - 最小值
    pub fn min(mut self, column: &str) -> Self {
        self.aggregate = Some(AggregateExpr {
            func: AggregateFunction::Min,
            column: Some(column.to_string()),
            alias: None,
        });
        self
    }

    /// 设置聚合结果别名
    pub fn alias(mut self, name: &str) -> Self {
        if let Some(ref mut agg) = self.aggregate {
            agg.alias = Some(name.to_string());
        }
        self
    }

    // ==================== GROUP BY 和 HAVING ====================

    /// GROUP BY 子句
    pub fn group_by(mut self, columns: &[&str]) -> Self {
        self.group_by = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    /// HAVING 子句 - 等于
    pub fn having_eq(mut self, value: DbValue) -> Self {
        self.having = Some(HavingExpr::Eq { value });
        self
    }

    /// HAVING 子句 - 不等于
    pub fn having_ne(mut self, value: DbValue) -> Self {
        self.having = Some(HavingExpr::Ne { value });
        self
    }

    /// HAVING 子句 - 大于
    pub fn having_gt(mut self, value: DbValue) -> Self {
        self.having = Some(HavingExpr::Gt { value });
        self
    }

    /// HAVING 子句 - 大于等于
    pub fn having_ge(mut self, value: DbValue) -> Self {
        self.having = Some(HavingExpr::Ge { value });
        self
    }

    /// HAVING 子句 - 小于
    pub fn having_lt(mut self, value: DbValue) -> Self {
        self.having = Some(HavingExpr::Lt { value });
        self
    }

    /// HAVING 子句 - 小于等于
    pub fn having_le(mut self, value: DbValue) -> Self {
        self.having = Some(HavingExpr::Le { value });
        self
    }

    /// 执行查询
    pub fn execute(self) -> DbResult<Vec<Row>> {
        let engine = self.engine.read().unwrap();

        // 检查是否有聚合函数
        if self.aggregate.is_some() || !self.group_by.is_empty() {
            return self.execute_aggregate(&engine);
        }

        // 检查是否有 JOIN
        if !self.joins.is_empty() {
            return self.execute_join(&engine);
        }

        // 尝试查找最佳复合索引
        if let Some((meta, index, key_values)) = self.find_best_index(&engine) {
            // 使用复合索引优化查询
            return self.execute_with_composite_index(&engine, meta, index, &key_values);
        }

        // 尝试查找最佳单列索引（向后兼容）
        if let Some((field, index)) = self.find_best_single_index(&engine) {
            // 使用单列索引优化查询
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

    /// 执行聚合查询
    fn execute_aggregate(&self, engine: &MemoryEngine) -> DbResult<Vec<Row>> {
        // 获取数据（考虑 JOIN 和过滤）
        let rows: Vec<Row> = if !self.joins.is_empty() {
            // 有 JOIN 时，先执行 JOIN
            let join_rows = self.execute_join(engine)?;
            // 应用过滤条件
            join_rows.into_iter()
                .filter(|row| self.filters.iter().all(|expr| evaluate_filter(expr, row)))
                .collect()
        } else {
            // 无 JOIN 时，直接扫描并过滤
            let scanned = engine.scan(&self.table)?;
            scanned.into_iter()
                .filter(|(_, row)| self.filters.iter().all(|expr| evaluate_filter(expr, row)))
                .map(|(_, row)| row.clone())
                .collect()
        };

        // 处理 GROUP BY
        if !self.group_by.is_empty() {
            self.execute_grouped_aggregate(rows)
        } else {
            // 无 GROUP BY，全局聚合
            self.execute_simple_aggregate(rows)
        }
    }

    /// 执行简单聚合（无 GROUP BY）
    fn execute_simple_aggregate(&self, rows: Vec<Row>) -> DbResult<Vec<Row>> {
        let agg = match &self.aggregate {
            Some(a) => a,
            None => {
                // 没有聚合函数，返回原始数据（不应该发生）
                return Ok(rows);
            }
        };

        let result_value = match agg.func {
            AggregateFunction::Count => {
                let count = match &agg.column {
                    Some(col) => {
                        // COUNT(column) - 统计非 NULL 值
                        rows.iter()
                            .filter(|r| r.get(col).map(|v| !v.is_null()).unwrap_or(false))
                            .count() as i64
                    }
                    None => {
                        // COUNT(*) - 统计所有行
                        rows.len() as i64
                    }
                };
                DbValue::Integer(count)
            }
            AggregateFunction::Sum => {
                match &agg.column {
                    Some(col) => self.sum_column(&rows, col),
                    None => DbValue::Integer(0),
                }
            }
            AggregateFunction::Avg => {
                match &agg.column {
                    Some(col) => self.avg_column(&rows, col),
                    None => DbValue::Null,
                }
            }
            AggregateFunction::Max => {
                match &agg.column {
                    Some(col) => self.max_column(&rows, col),
                    None => DbValue::Null,
                }
            }
            AggregateFunction::Min => {
                match &agg.column {
                    Some(col) => self.min_column(&rows, col),
                    None => DbValue::Null,
                }
            }
        };

        // 构建结果行
        let mut result_row = Row::new();
        let column_name = match &agg.alias {
            Some(alias) => alias.clone(),
            None => {
                let func_name = match agg.func {
                    AggregateFunction::Count => "COUNT",
                    AggregateFunction::Sum => "SUM",
                    AggregateFunction::Avg => "AVG",
                    AggregateFunction::Max => "MAX",
                    AggregateFunction::Min => "MIN",
                };
                match &agg.column {
                    Some(col) => format!("{}({})", func_name, col),
                    None => "COUNT(*)".to_string(),
                }
            }
        };

        // 将 i64 转换为 DbValue
        let db_value = match result_value {
            DbValue::Integer(i) => DbValue::Integer(i),
            DbValue::Real(r) => DbValue::Real(r),
            DbValue::Null => DbValue::Null,
            _ => DbValue::Null,
        };
        result_row.insert(column_name, db_value);

        Ok(vec![result_row])
    }

    /// 执行分组聚合（有 GROUP BY）
    fn execute_grouped_aggregate(&self, rows: Vec<Row>) -> DbResult<Vec<Row>> {
        let agg = match &self.aggregate {
            Some(a) => a,
            None => {
                // 没有聚合函数，只返回 GROUP BY 列的不同组合
                return self.execute_group_by_only(rows);
            }
        };

        // 按 GROUP BY 列分组
        use std::collections::HashMap;
        let mut groups: HashMap<Vec<DbValue>, Vec<Row>> = HashMap::new();

        for row in rows {
            // 构建分组键
            let mut key = Vec::new();
            for col in &self.group_by {
                if let Some(val) = row.get(col) {
                    key.push(val.clone());
                } else {
                    key.push(DbValue::Null);
                }
            }

            groups.entry(key).or_insert_with(Vec::new).push(row);
        }

        // 对每组计算聚合函数
        let mut results = Vec::new();
        for (key, group_rows) in groups {
            // 计算聚合值
            let agg_value = match agg.func {
                AggregateFunction::Count => {
                    let count = match &agg.column {
                        Some(col) => {
                            group_rows.iter()
                                .filter(|r| r.get(col).map(|v| !v.is_null()).unwrap_or(false))
                                .count() as i64
                        }
                        None => group_rows.len() as i64,
                    };
                    DbValue::Integer(count)
                }
                AggregateFunction::Sum => {
                    match &agg.column {
                        Some(col) => self.sum_column(&group_rows, col),
                        None => DbValue::Integer(0),
                    }
                }
                AggregateFunction::Avg => {
                    match &agg.column {
                        Some(col) => self.avg_column(&group_rows, col),
                        None => DbValue::Null,
                    }
                }
                AggregateFunction::Max => {
                    match &agg.column {
                        Some(col) => self.max_column(&group_rows, col),
                        None => DbValue::Null,
                    }
                }
                AggregateFunction::Min => {
                    match &agg.column {
                        Some(col) => self.min_column(&group_rows, col),
                        None => DbValue::Null,
                    }
                }
            };

            // 应用 HAVING 过滤
            if let Some(ref having) = self.having {
                if !self.evaluate_having(&agg_value, having) {
                    continue;
                }
            }

            // 构建结果行
            let mut result_row = Row::new();

            // 添加 GROUP BY 列
            for (i, col) in self.group_by.iter().enumerate() {
                result_row.insert(col.clone(), key[i].clone());
            }

            // 添加聚合结果
            let column_name = match &agg.alias {
                Some(alias) => alias.clone(),
                None => {
                    let func_name = match agg.func {
                        AggregateFunction::Count => "COUNT",
                        AggregateFunction::Sum => "SUM",
                        AggregateFunction::Avg => "AVG",
                        AggregateFunction::Max => "MAX",
                        AggregateFunction::Min => "MIN",
                    };
                    match &agg.column {
                        Some(col) => format!("{}({})", func_name, col),
                        None => "COUNT(*)".to_string(),
                    }
                }
            };

            let db_value = match agg_value {
                DbValue::Integer(i) => DbValue::Integer(i),
                DbValue::Real(r) => DbValue::Real(r),
                DbValue::Null => DbValue::Null,
                _ => DbValue::Null,
            };
            result_row.insert(column_name, db_value);

            results.push(result_row);
        }

        Ok(results)
    }

    /// 仅执行 GROUP BY（没有聚合函数）
    fn execute_group_by_only(&self, rows: Vec<Row>) -> DbResult<Vec<Row>> {
        use std::collections::HashSet;

        let mut seen: HashSet<Vec<DbValue>> = HashSet::new();
        let mut results = Vec::new();

        for row in rows {
            let mut key = Vec::new();
            for col in &self.group_by {
                if let Some(val) = row.get(col) {
                    key.push(val.clone());
                } else {
                    key.push(DbValue::Null);
                }
            }

            if !seen.contains(&key) {
                seen.insert(key.clone());

                let mut result_row = Row::new();
                for (i, col) in self.group_by.iter().enumerate() {
                    result_row.insert(col.clone(), key[i].clone());
                }
                results.push(result_row);
            }
        }

        Ok(results)
    }

    /// 计算列的总和
    fn sum_column(&self, rows: &[Row], column: &str) -> DbValue {
        let mut sum: f64 = 0.0;
        let mut has_value = false;

        for row in rows {
            if let Some(val) = row.get(column) {
                match val {
                    DbValue::Integer(i) => {
                        sum += *i as f64;
                        has_value = true;
                    }
                    DbValue::Real(r) => {
                        sum += *r;
                        has_value = true;
                    }
                    _ => {}
                }
            }
        }

        if has_value {
            // 如果所有值都是整数，返回整数
            if rows.iter().filter_map(|r| r.get(column)).all(|v| matches!(v, DbValue::Integer(_))) {
                DbValue::Integer(sum as i64)
            } else {
                DbValue::Real(sum)
            }
        } else {
            DbValue::Null
        }
    }

    /// 计算列的平均值
    fn avg_column(&self, rows: &[Row], column: &str) -> DbValue {
        let mut sum: f64 = 0.0;
        let mut count: i64 = 0;

        for row in rows {
            if let Some(val) = row.get(column) {
                match val {
                    DbValue::Integer(i) => {
                        sum += *i as f64;
                        count += 1;
                    }
                    DbValue::Real(r) => {
                        sum += *r;
                        count += 1;
                    }
                    _ => {}
                }
            }
        }

        if count > 0 {
            DbValue::Real(sum / count as f64)
        } else {
            DbValue::Null
        }
    }

    /// 计算列的最大值
    fn max_column(&self, rows: &[Row], column: &str) -> DbValue {
        let mut max_val: Option<DbValue> = None;

        for row in rows {
            if let Some(val) = row.get(column) {
                if !val.is_null() {
                    match &max_val {
                        None => max_val = Some(val.clone()),
                        Some(current) => {
                            if self.compare_values(val, current) > 0 {
                                max_val = Some(val.clone());
                            }
                        }
                    }
                }
            }
        }

        max_val.unwrap_or(DbValue::Null)
    }

    /// 计算列的最小值
    fn min_column(&self, rows: &[Row], column: &str) -> DbValue {
        let mut min_val: Option<DbValue> = None;

        for row in rows {
            if let Some(val) = row.get(column) {
                if !val.is_null() {
                    match &min_val {
                        None => min_val = Some(val.clone()),
                        Some(current) => {
                            if self.compare_values(val, current) < 0 {
                                min_val = Some(val.clone());
                            }
                        }
                    }
                }
            }
        }

        min_val.unwrap_or(DbValue::Null)
    }

    /// 比较两个 DbValue
    fn compare_values(&self, a: &DbValue, b: &DbValue) -> i32 {
        match (a, b) {
            (DbValue::Integer(a), DbValue::Integer(b)) => a.cmp(b) as i32,
            (DbValue::Integer(a), DbValue::Real(b)) => (*a as f64).partial_cmp(b).map(|o| o as i32).unwrap_or(0),
            (DbValue::Real(a), DbValue::Integer(b)) => a.partial_cmp(&(*b as f64)).map(|o| o as i32).unwrap_or(0),
            (DbValue::Real(a), DbValue::Real(b)) => a.partial_cmp(b).map(|o| o as i32).unwrap_or(0),
            (DbValue::Text(a), DbValue::Text(b)) => a.cmp(b) as i32,
            _ => 0,
        }
    }

    /// 评估 HAVING 子句
    fn evaluate_having(&self, agg_value: &DbValue, having: &HavingExpr) -> bool {
        match having {
            HavingExpr::Eq { value } => agg_value == value,
            HavingExpr::Ne { value } => agg_value != value,
            HavingExpr::Gt { value } => self.compare_values(agg_value, value) > 0,
            HavingExpr::Ge { value } => self.compare_values(agg_value, value) >= 0,
            HavingExpr::Lt { value } => self.compare_values(agg_value, value) < 0,
            HavingExpr::Le { value } => self.compare_values(agg_value, value) <= 0,
        }
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
            let matches = self.filters.iter().all(|expr| evaluate_filter(expr, &row));
            if matches {
                to_update.push(row_id);
            }
        }

        // 执行更新
        let mut count = 0;
        for row_id in to_update {
            let mut update_row: Row = self.values.iter().cloned().collect::<Row>();
            // 保留原有行的其他字段
            if let Some(existing) = engine.get(&self.table, row_id)? {
                for (key, value) in existing.iter() {
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
            let matches = self.filters.iter().all(|expr| evaluate_filter(expr, &row));
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
