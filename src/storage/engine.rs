use crate::types::{DbValue, TableSchema, DbResult, DbError, SchemaError};
use crate::index::IndexManager;
use crate::index::manager::IndexMeta;
use crate::index::btree::BTreeIndex;
use indexmap::IndexMap;
use std::collections::HashMap;

/// 行 ID 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId(pub u64);

impl RowId {
    pub fn new(id: u64) -> Self {
        RowId(id)
    }
}

/// 行数据 - 使用 IndexMap 保持列顺序
pub type Row = IndexMap<String, DbValue>;

/// 表数据容器
#[derive(Debug)]
pub struct Table {
    pub schema: TableSchema,
    pub rows: HashMap<RowId, Row>,
    pub next_row_id: u64,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        Table {
            schema,
            rows: HashMap::new(),
            next_row_id: 0,
        }
    }

    pub fn insert(&mut self, row: Row) -> RowId {
        let row_id = RowId(self.next_row_id);
        self.next_row_id += 1;
        self.rows.insert(row_id, row);
        row_id
    }

    pub fn get(&self, row_id: RowId) -> Option<&Row> {
        self.rows.get(&row_id)
    }

    pub fn get_mut(&mut self, row_id: RowId) -> Option<&mut Row> {
        self.rows.get_mut(&row_id)
    }

    pub fn remove(&mut self, row_id: RowId) -> Option<Row> {
        self.rows.remove(&row_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (RowId, &Row)> {
        self.rows.iter().map(|(k, v)| (*k, v))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (RowId, &mut Row)> {
        self.rows.iter_mut().map(|(k, v)| (*k, v))
    }
}

/// 存储引擎 trait
pub trait StorageEngine {
    /// 创建表
    fn create_table(&mut self, schema: TableSchema) -> DbResult<()>;

    /// 删除表
    fn drop_table(&mut self, name: &str) -> DbResult<()>;

    /// 检查表是否存在
    fn has_table(&self, name: &str) -> bool;

    /// 获取表 schema
    fn get_schema(&self, name: &str) -> DbResult<&TableSchema>;

    /// 插入行
    fn insert(&mut self, table: &str, row: Row) -> DbResult<RowId>;

    /// 获取行
    fn get(&self, table: &str, row_id: RowId) -> DbResult<Option<&Row>>;

    /// 更新行
    fn update(&mut self, table: &str, row_id: RowId, values: Row) -> DbResult<()>;

    /// 删除行
    fn delete(&mut self, table: &str, row_id: RowId) -> DbResult<Option<Row>>;

    /// 扫描全表
    fn scan(&self, table: &str) -> DbResult<Vec<(RowId, &Row)>>;
}

/// 内存存储引擎实现
pub struct MemoryEngine {
    tables: HashMap<String, Table>,
    /// 索引管理器
    indexes: IndexManager,
}

impl MemoryEngine {
    pub fn new() -> Self {
        MemoryEngine {
            tables: HashMap::new(),
            indexes: IndexManager::new(),
        }
    }

    /// 为表列创建索引（单列，向后兼容）
    pub fn create_index(&mut self, table: &str, column: &str) -> DbResult<()> {
        self.create_composite_index(table, &[column])
    }

    /// 为表列创建复合索引
    pub fn create_composite_index(&mut self, table: &str, columns: &[&str]) -> DbResult<()> {
        // 检查表是否存在
        if !self.has_table(table) {
            return Err(DbError::TableNotFound(table.to_string()));
        }

        // 检查列是否存在
        let schema = self.get_schema(table)?;
        for &column in columns {
            if !schema.columns.iter().any(|c| c.name == column) {
                return Err(DbError::SchemaError(SchemaError::UnknownColumn {
                    table: table.to_string(),
                    column: column.to_string(),
                }));
            }
        }

        // 创建索引
        self.indexes.create_index(table, columns);

        // 构建索引：扫描现有数据
        if let Some(index) = self.indexes.get_composite_index_mut(table, columns) {
            if let Some(tbl) = self.tables.get(table) {
                for (row_id, row) in tbl.iter() {
                    let mut composite_key = Vec::new();
                    for &column in columns {
                        if let Some(value) = row.get(column) {
                            composite_key.push(value.clone());
                        } else {
                            composite_key.push(DbValue::Null);
                        }
                    }
                    index.insert_composite(composite_key, row_id);
                }
            }
        }

        Ok(())
    }

    /// 创建唯一复合索引
    pub fn create_unique_index(&mut self, table: &str, columns: &[&str]) -> DbResult<()> {
        // 检查表是否存在
        if !self.has_table(table) {
            return Err(DbError::TableNotFound(table.to_string()));
        }

        // 检查列是否存在
        let schema = self.get_schema(table)?;
        for &column in columns {
            if !schema.columns.iter().any(|c| c.name == column) {
                return Err(DbError::SchemaError(SchemaError::UnknownColumn {
                    table: table.to_string(),
                    column: column.to_string(),
                }));
            }
        }

        // 创建唯一索引
        self.indexes.create_unique_index(table, columns);

        // 构建索引：扫描现有数据
        if let Some(index) = self.indexes.get_composite_index_mut(table, columns) {
            if let Some(tbl) = self.tables.get(table) {
                for (row_id, row) in tbl.iter() {
                    let mut composite_key = Vec::new();
                    for &column in columns {
                        if let Some(value) = row.get(column) {
                            composite_key.push(value.clone());
                        } else {
                            composite_key.push(DbValue::Null);
                        }
                    }
                    index.insert_composite(composite_key, row_id);
                }
            }
        }

        Ok(())
    }

    /// 删除索引
    pub fn drop_index(&mut self, table: &str, column: &str) -> DbResult<bool> {
        Ok(self.indexes.drop_index(table, &[column]))
    }

    /// 删除复合索引
    pub fn drop_composite_index(&mut self, table: &str, columns: &[&str]) -> DbResult<bool> {
        Ok(self.indexes.drop_index(table, columns))
    }

    /// 检查列是否有索引
    pub fn has_index(&self, table: &str, column: &str) -> bool {
        self.indexes.has_index(table, column)
    }

    /// 检查复合索引是否存在
    pub fn has_composite_index(&self, table: &str, columns: &[&str]) -> bool {
        self.indexes.has_composite_index(table, columns)
    }

    /// 获取索引（不可变引用，单列）
    pub fn get_index(&self, table: &str, column: &str) -> Option<&crate::index::BTreeIndex> {
        self.indexes.get_index(table, column)
    }

    /// 获取复合索引（不可变引用）
    pub fn get_composite_index(&self, table: &str, columns: &[&str]) -> Option<&crate::index::BTreeIndex> {
        self.indexes.get_composite_index(table, columns)
    }

    /// 获取索引（可变引用，单列）
    pub fn get_index_mut(&mut self, table: &str, column: &str) -> Option<&mut crate::index::BTreeIndex> {
        self.indexes.get_index_mut(table, column)
    }

    /// 获取复合索引（可变引用）
    pub fn get_composite_index_mut(&mut self, table: &str, columns: &[&str]) -> Option<&mut crate::index::BTreeIndex> {
        self.indexes.get_composite_index_mut(table, columns)
    }

    /// 删除表的所有索引
    pub fn drop_table_indexes(&mut self, table: &str) {
        self.indexes.drop_table_indexes(table);
    }

    /// 从索引中删除键值对（用于 delete 操作，单列）
    pub fn remove_from_index(&mut self, table: &str, column: &str, key: &DbValue, row_id: RowId) {
        if let Some(index) = self.indexes.get_index_mut(table, column) {
            index.remove(key, row_id);
        }
    }

    /// 从复合索引中删除键值对
    pub fn remove_from_composite_index(&mut self, table: &str, columns: &[&str], key: &[DbValue], row_id: RowId) {
        if let Some(index) = self.indexes.get_composite_index_mut(table, columns) {
            index.remove_composite(key, row_id);
        }
    }

    /// 向索引添加键值对（用于 insert 操作，单列）
    pub fn add_to_index(&mut self, table: &str, column: &str, key: DbValue, row_id: RowId) {
        if let Some(index) = self.indexes.get_index_mut(table, column) {
            index.insert(key, row_id);
        }
    }

    /// 向复合索引添加键值对
    pub fn add_to_composite_index(&mut self, table: &str, columns: &[&str], key: Vec<DbValue>, row_id: RowId) {
        if let Some(index) = self.indexes.get_composite_index_mut(table, columns) {
            index.insert_composite(key, row_id);
        }
    }

    /// 查找最佳索引
    pub fn find_best_index(&self, table: &str, filter_columns: &[&str]) -> Option<(&IndexMeta, &BTreeIndex)> {
        self.indexes.find_best_index(table, filter_columns)
    }
}

impl Default for MemoryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine for MemoryEngine {
    fn create_table(&mut self, schema: TableSchema) -> DbResult<()> {
        if self.tables.contains_key(&schema.name) {
            return Err(DbError::TableAlreadyExists(schema.name.clone()));
        }
        self.tables.insert(schema.name.clone(), Table::new(schema));
        Ok(())
    }

    fn drop_table(&mut self, name: &str) -> DbResult<()> {
        self.tables
            .remove(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))?;
        // 删除表的所有索引
        self.drop_table_indexes(name);
        Ok(())
    }

    fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    fn get_schema(&self, name: &str) -> DbResult<&TableSchema> {
        self.tables
            .get(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
            .map(|table| &table.schema)
    }

    fn insert(&mut self, table: &str, row: Row) -> DbResult<RowId> {
        let tbl = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;

        // 验证 schema
        let values: Vec<(String, DbValue)> = row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        tbl.schema.validate(&values)?;

        // 先获取需要索引的列和值（在插入之前）
        let index_metas: Vec<&IndexMeta> = self.indexes.get_table_indexes(table);

        // 收集单列索引条目
        let index_entries: Vec<(String, DbValue)> = index_metas.iter()
            .filter(|m| m.columns.len() == 1)
            .flat_map(|m| m.columns.iter())
            .filter_map(|column| row.get(column).map(|v| (column.clone(), v.clone())))
            .collect();

        // 收集复合索引条目（存储列名而不是引用）
        let composite_index_entries: Vec<(Vec<String>, Vec<DbValue>)> = index_metas.iter()
            .filter(|m| m.columns.len() > 1)
            .filter_map(|meta| {
                let key: Vec<DbValue> = meta.columns.iter()
                    .filter_map(|col| row.get(col).cloned())
                    .collect();
                if key.len() == meta.columns.len() {
                    Some((meta.columns.clone(), key))
                } else {
                    None
                }
            })
            .collect();

        drop(index_metas); // 提前释放借用

        let row_id = tbl.insert(row);

        // 更新单列索引
        for (column, value) in index_entries {
            self.add_to_index(table, &column, value, row_id);
        }

        // 更新复合索引
        for (columns, key) in composite_index_entries {
            let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            self.add_to_composite_index(table, &columns_ref, key, row_id);
        }

        Ok(row_id)
    }

    fn get(&self, table: &str, row_id: RowId) -> DbResult<Option<&Row>> {
        let tbl = self
            .tables
            .get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
        Ok(tbl.get(row_id))
    }

    fn update(&mut self, table: &str, row_id: RowId, values: Row) -> DbResult<()> {
        // 获取旧值（用于从索引中删除）
        let old_row_data: Option<Row> = {
            let tbl = self.tables.get(table)
                .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
            tbl.get(row_id).cloned()
        };

        // 验证 schema 并更新行
        let tbl = self.tables.get_mut(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
        let values_ref: Vec<(String, DbValue)> = values.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        tbl.schema.validate(&values_ref)?;

        let row = tbl.get_mut(row_id).ok_or_else(|| DbError::RowNotFound)?;
        for (key, value) in values {
            row.insert(key, value);
        }

        // 获取新值（用于添加到索引）
        let new_row_data: Option<Row> = {
            let tbl = self.tables.get(table)
                .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
            tbl.get(row_id).cloned()
        };

        // 提取索引元数据信息（避免借用冲突）
        // 单列索引：列名列表
        let single_index_columns: Vec<String> = self.indexes.get_table_indexes(table)
            .into_iter()
            .filter(|m| m.columns.len() == 1)
            .filter_map(|m| m.columns.first().cloned())
            .collect();

        // 复合索引：(列名列表，is_unique)
        let composite_index_infos: Vec<(Vec<String>, bool)> = self.indexes.get_table_indexes(table)
            .into_iter()
            .filter(|m| m.columns.len() > 1)
            .map(|m| (m.columns.clone(), m.is_unique))
            .collect();

        // 更新索引：先删除旧值
        if let Some(ref old_row) = old_row_data {
            // 删除单列索引
            for col in &single_index_columns {
                if let Some(value) = old_row.get(col) {
                    self.remove_from_index(table, col, value, row_id);
                }
            }

            // 删除复合索引
            for (columns, _is_unique) in &composite_index_infos {
                let key: Vec<DbValue> = columns.iter()
                    .filter_map(|col| old_row.get(col).cloned())
                    .collect();
                if key.len() == columns.len() {
                    let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                    self.remove_from_composite_index(table, &columns_ref, &key, row_id);
                }
            }
        }

        // 添加新值到索引
        if let Some(ref new_row) = new_row_data {
            // 添加单列索引
            for col in &single_index_columns {
                if let Some(value) = new_row.get(col) {
                    self.add_to_index(table, col, value.clone(), row_id);
                }
            }

            // 添加复合索引
            for (columns, _is_unique) in &composite_index_infos {
                let key: Vec<DbValue> = columns.iter()
                    .filter_map(|col| new_row.get(col).cloned())
                    .collect();
                if key.len() == columns.len() {
                    let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                    self.add_to_composite_index(table, &columns_ref, key, row_id);
                }
            }
        }

        Ok(())
    }

    fn delete(&mut self, table: &str, row_id: RowId) -> DbResult<Option<Row>> {
        // 获取要删除的行数据
        let row_data = {
            let tbl = self.tables.get(table)
                .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
            tbl.get(row_id).cloned()
        };

        // 提取索引元数据信息（避免借用冲突）
        // 单列索引：列名列表
        let single_index_columns: Vec<String> = self.indexes.get_table_indexes(table)
            .into_iter()
            .filter(|m| m.columns.len() == 1)
            .filter_map(|m| m.columns.first().cloned())
            .collect();

        // 复合索引：列名列表
        let composite_index_infos: Vec<Vec<String>> = self.indexes.get_table_indexes(table)
            .into_iter()
            .filter(|m| m.columns.len() > 1)
            .map(|m| m.columns.clone())
            .collect();

        // 从索引中删除
        if let Some(ref row_data) = row_data {
            // 删除单列索引
            for col in &single_index_columns {
                if let Some(value) = row_data.get(col) {
                    self.remove_from_index(table, col, value, row_id);
                }
            }

            // 删除复合索引
            for columns in &composite_index_infos {
                let key: Vec<DbValue> = columns.iter()
                    .filter_map(|col| row_data.get(col).cloned())
                    .collect();
                if key.len() == columns.len() {
                    let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                    self.remove_from_composite_index(table, &columns_ref, &key, row_id);
                }
            }
        }

        // 删除行
        let tbl = self.tables.get_mut(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
        Ok(tbl.remove(row_id))
    }

    fn scan(&self, table: &str) -> DbResult<Vec<(RowId, &Row)>> {
        let tbl = self
            .tables
            .get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
        Ok(tbl.iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Column};

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
    fn test_create_table() {
        let mut engine = MemoryEngine::new();
        let schema = create_test_schema();

        assert!(engine.create_table(schema).is_ok());
        assert!(engine.has_table("users"));

        // 重复创建应该失败
        let schema2 = create_test_schema();
        assert!(matches!(
            engine.create_table(schema2),
            Err(DbError::TableAlreadyExists(_))
        ));
    }

    #[test]
    fn test_insert_and_get() {
        let mut engine = MemoryEngine::new();
        engine.create_table(create_test_schema()).unwrap();

        let row = create_test_row();
        let row_id = engine.insert("users", row).unwrap();

        let retrieved = engine.get("users", row_id).unwrap().unwrap();
        assert_eq!(retrieved.get("name").unwrap().as_text(), Some("Alice"));
    }

    #[test]
    fn test_update() {
        let mut engine = MemoryEngine::new();
        engine.create_table(create_test_schema()).unwrap();

        let row = create_test_row();
        let row_id = engine.insert("users", row).unwrap();

        let mut update_values = Row::new();
        update_values.insert("age".to_string(), DbValue::integer(26));

        engine.update("users", row_id, update_values).unwrap();

        let retrieved = engine.get("users", row_id).unwrap().unwrap();
        assert_eq!(retrieved.get("age").unwrap().as_integer(), Some(26));
    }

    #[test]
    fn test_delete() {
        let mut engine = MemoryEngine::new();
        engine.create_table(create_test_schema()).unwrap();

        let row = create_test_row();
        let row_id = engine.insert("users", row).unwrap();

        let deleted = engine.delete("users", row_id).unwrap().unwrap();
        assert_eq!(deleted.get("name").unwrap().as_text(), Some("Alice"));

        assert!(engine.get("users", row_id).unwrap().is_none());
    }

    #[test]
    fn test_scan() {
        let mut engine = MemoryEngine::new();
        engine.create_table(create_test_schema()).unwrap();

        for i in 0..3 {
            let mut row = Row::new();
            row.insert("id".to_string(), DbValue::integer(i));
            row.insert("name".to_string(), DbValue::text(format!("User{}", i)));
            row.insert("age".to_string(), DbValue::integer(20 + i));
            engine.insert("users", row).unwrap();
        }

        let rows = engine.scan("users").unwrap();
        assert_eq!(rows.len(), 3);
    }
}
