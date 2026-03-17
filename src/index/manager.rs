use std::collections::HashMap;
use crate::index::btree::BTreeIndex;

/// 索引元数据
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub table: String,
    pub columns: Vec<String>,  // 支持多列
    pub is_unique: bool,       // 是否唯一索引
}

/// 索引管理器
#[derive(Debug)]
pub struct IndexManager {
    /// 索引键 "table:col1,col2" -> BTreeIndex
    indexes: HashMap<String, BTreeIndex>,
    /// 索引键 "table:col1,col2" -> IndexMeta
    index_meta: HashMap<String, IndexMeta>,
    /// 表 -> 索引键列表 (用于快速查找表的所有索引)
    table_indexes: HashMap<String, Vec<String>>,
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            indexes: HashMap::new(),
            index_meta: HashMap::new(),
            table_indexes: HashMap::new(),
        }
    }

    /// 生成索引键
    fn make_index_key(table: &str, columns: &[&str]) -> String {
        format!("{}:{}", table, columns.join(","))
    }

    /// 为表列创建索引（支持复合索引）
    pub fn create_index(&mut self, table: &str, columns: &[&str]) {
        let key = Self::make_index_key(table, columns);

        if !self.indexes.contains_key(&key) {
            let index = BTreeIndex::new_composite(4, columns);
            let meta = IndexMeta {
                table: table.to_string(),
                columns: columns.iter().map(|s| s.to_string()).collect(),
                is_unique: false,
            };

            self.indexes.insert(key.clone(), index);
            self.index_meta.insert(key.clone(), meta);

            // 更新表的索引列表
            self.table_indexes
                .entry(table.to_string())
                .or_insert_with(Vec::new)
                .push(key);
        }
    }

    /// 创建唯一索引
    pub fn create_unique_index(&mut self, table: &str, columns: &[&str]) {
        let key = Self::make_index_key(table, columns);

        if !self.indexes.contains_key(&key) {
            let index = BTreeIndex::new_composite(4, columns);
            let meta = IndexMeta {
                table: table.to_string(),
                columns: columns.iter().map(|s| s.to_string()).collect(),
                is_unique: true,
            };

            self.indexes.insert(key.clone(), index);
            self.index_meta.insert(key.clone(), meta);

            self.table_indexes
                .entry(table.to_string())
                .or_insert_with(Vec::new)
                .push(key);
        }
    }

    /// 删除索引
    pub fn drop_index(&mut self, table: &str, columns: &[&str]) -> bool {
        let key = Self::make_index_key(table, columns);

        if self.indexes.remove(&key).is_some() {
            self.index_meta.remove(&key);
            if let Some(index_list) = self.table_indexes.get_mut(table) {
                if let Some(pos) = index_list.iter().position(|k| k == &key) {
                    index_list.remove(pos);
                }
            }
            return true;
        }
        false
    }

    /// 检查列是否有索引（单列检查）
    pub fn has_index(&self, table: &str, column: &str) -> bool {
        let key = Self::make_index_key(table, &[column]);
        self.indexes.contains_key(&key)
    }

    /// 检查复合索引是否存在
    pub fn has_composite_index(&self, table: &str, columns: &[&str]) -> bool {
        let key = Self::make_index_key(table, columns);
        self.indexes.contains_key(&key)
    }

    /// 获取索引（不可变引用，单列）
    pub fn get_index(&self, table: &str, column: &str) -> Option<&BTreeIndex> {
        let key = Self::make_index_key(table, &[column]);
        self.indexes.get(&key)
    }

    /// 获取复合索引（不可变引用）
    pub fn get_composite_index(&self, table: &str, columns: &[&str]) -> Option<&BTreeIndex> {
        let key = Self::make_index_key(table, columns);
        self.indexes.get(&key)
    }

    /// 获取索引（可变引用，单列）
    pub fn get_index_mut(&mut self, table: &str, column: &str) -> Option<&mut BTreeIndex> {
        let key = Self::make_index_key(table, &[column]);
        self.indexes.get_mut(&key)
    }

    /// 获取复合索引（可变引用）
    pub fn get_composite_index_mut(&mut self, table: &str, columns: &[&str]) -> Option<&mut BTreeIndex> {
        let key = Self::make_index_key(table, columns);
        self.indexes.get_mut(&key)
    }

    /// 获取表的所有索引元数据
    pub fn get_table_indexes(&self, table: &str) -> Vec<&IndexMeta> {
        self.table_indexes
            .get(table)
            .map(|keys| keys.iter().filter_map(|k| self.index_meta.get(k)).collect())
            .unwrap_or_default()
    }

    /// 删除表的所有索引
    pub fn drop_table_indexes(&mut self, table: &str) {
        if let Some(keys) = self.table_indexes.remove(table) {
            for key in &keys {
                self.indexes.remove(key);
                self.index_meta.remove(key);
            }
        }
    }

    /// 查找适合给定列的最佳索引（用于查询优化）
    pub fn find_best_index(&self, table: &str, filter_columns: &[&str]) -> Option<(&IndexMeta, &BTreeIndex)> {
        if let Some(keys) = self.table_indexes.get(table) {
            for key in keys {
                if let Some(meta) = self.index_meta.get(key) {
                    if let Some(index) = self.indexes.get(key) {
                        // 检查索引列是否匹配过滤列（前缀匹配）
                        if meta.columns.len() >= filter_columns.len() {
                            let matches = filter_columns.iter().enumerate().all(|(i, col)| {
                                meta.columns.get(i).map(|s| s.as_str()) == Some(*col)
                            });
                            if matches {
                                return Some((meta, index));
                            }
                        }
                    }
                }
            }
        }
        None
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_index() {
        let mut manager = IndexManager::new();

        manager.create_index("users", &["email"]);

        assert!(manager.has_index("users", "email"));
        assert!(!manager.has_index("users", "name"));
    }

    #[test]
    fn test_drop_index() {
        let mut manager = IndexManager::new();

        manager.create_index("users", &["email"]);
        assert!(manager.has_index("users", "email"));

        manager.drop_index("users", &["email"]);
        assert!(!manager.has_index("users", "email"));
    }

    #[test]
    fn test_drop_table_indexes() {
        let mut manager = IndexManager::new();

        manager.create_index("users", &["email"]);
        manager.create_index("users", &["name"]);
        manager.create_index("orders", &["user_id"]);

        manager.drop_table_indexes("users");

        assert!(!manager.has_index("users", "email"));
        assert!(!manager.has_index("users", "name"));
        assert!(manager.has_index("orders", "user_id"));
    }

    #[test]
    fn test_get_table_indexes() {
        let mut manager = IndexManager::new();

        manager.create_index("users", &["email"]);
        manager.create_index("users", &["name"]);

        let indexes = manager.get_table_indexes("users");
        assert_eq!(indexes.len(), 2);
    }

    #[test]
    fn test_composite_index_creation() {
        let mut manager = IndexManager::new();

        manager.create_index("users", &["age", "city"]);

        assert!(manager.has_composite_index("users", &["age", "city"]));
        assert!(!manager.has_index("users", "age")); // 单列检查应该返回 false
    }

    #[test]
    fn test_unique_index_creation() {
        let mut manager = IndexManager::new();

        manager.create_unique_index("orders", &["user_id", "product_id"]);

        assert!(manager.has_composite_index("orders", &["user_id", "product_id"]));

        let indexes = manager.get_table_indexes("orders");
        assert_eq!(indexes.len(), 1);
        assert!(indexes[0].is_unique);
    }

    #[test]
    fn test_find_best_index() {
        let mut manager = IndexManager::new();

        // 创建复合索引 (age, city)
        manager.create_index("users", &["age", "city"]);
        // 创建单列索引 (email)
        manager.create_index("users", &["email"]);

        // 查找 age, city 的最佳索引
        let result = manager.find_best_index("users", &["age", "city"]);
        assert!(result.is_some());

        // 查找 age 的最佳索引（前缀匹配）
        let result = manager.find_best_index("users", &["age"]);
        assert!(result.is_some());

        // 查找 email 的最佳索引
        let result = manager.find_best_index("users", &["email"]);
        assert!(result.is_some());

        // 查找不存在的索引
        let result = manager.find_best_index("users", &["name"]);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_composite_index() {
        let mut manager = IndexManager::new();

        manager.create_index("users", &["age", "city"]);

        let index = manager.get_composite_index("users", &["age", "city"]);
        assert!(index.is_some());

        let index = manager.get_composite_index("users", &["age"]);
        assert!(index.is_none()); // 复合索引不能用单列获取
    }
}
