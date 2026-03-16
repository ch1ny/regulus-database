use std::collections::HashMap;
use crate::index::btree::BTreeIndex;

/// 索引信息
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub table: String,
    pub column: String,
}

/// 索引管理器
#[derive(Debug)]
pub struct IndexManager {
    /// 表名 + 列名 -> B+ 树索引
    indexes: HashMap<(String, String), BTreeIndex>,
    /// 表 -> 列 -> 索引键 (用于快速查找哪些列有索引)
    table_indexes: HashMap<String, HashMap<String, BTreeIndex>>,
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            indexes: HashMap::new(),
            table_indexes: HashMap::new(),
        }
    }

    /// 为指定表列创建索引
    pub fn create_index(&mut self, table: &str, column: &str) {
        let key = (table.to_string(), column.to_string());

        if !self.indexes.contains_key(&key) {
            let index = BTreeIndex::new(4); // 使用默认阶数 4

            // 如果是第一次为该表创建索引，初始化 HashMap
            self.table_indexes
                .entry(table.to_string())
                .or_insert_with(HashMap::new)
                .insert(column.to_string(), index.clone());

            self.indexes.insert(key, index);
        }
    }

    /// 删除索引
    pub fn drop_index(&mut self, table: &str, column: &str) -> bool {
        let key = (table.to_string(), column.to_string());

        if self.indexes.remove(&key).is_some() {
            if let Some(table_map) = self.table_indexes.get_mut(table) {
                table_map.remove(column);
            }
            return true;
        }
        false
    }

    /// 检查列是否有索引
    pub fn has_index(&self, table: &str, column: &str) -> bool {
        let key = (table.to_string(), column.to_string());
        self.indexes.contains_key(&key)
    }

    /// 获取索引（不可变引用）
    pub fn get_index(&self, table: &str, column: &str) -> Option<&BTreeIndex> {
        let key = (table.to_string(), column.to_string());
        self.indexes.get(&key)
    }

    /// 获取索引（可变引用）
    pub fn get_index_mut(&mut self, table: &str, column: &str) -> Option<&mut BTreeIndex> {
        let key = (table.to_string(), column.to_string());
        self.indexes.get_mut(&key)
    }

    /// 获取表的所有索引列
    pub fn get_table_indexes(&self, table: &str) -> Vec<&String> {
        self.table_indexes
            .get(table)
            .map(|m| m.keys().collect())
            .unwrap_or_default()
    }

    /// 删除表的所有索引
    pub fn drop_table_indexes(&mut self, table: &str) {
        if let Some(table_map) = self.table_indexes.remove(table) {
            for column in table_map.keys() {
                let key = (table.to_string(), column.to_string());
                self.indexes.remove(&key);
            }
        }
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

        manager.create_index("users", "email");

        assert!(manager.has_index("users", "email"));
        assert!(!manager.has_index("users", "name"));
    }

    #[test]
    fn test_drop_index() {
        let mut manager = IndexManager::new();

        manager.create_index("users", "email");
        assert!(manager.has_index("users", "email"));

        manager.drop_index("users", "email");
        assert!(!manager.has_index("users", "email"));
    }

    #[test]
    fn test_drop_table_indexes() {
        let mut manager = IndexManager::new();

        manager.create_index("users", "email");
        manager.create_index("users", "name");
        manager.create_index("orders", "user_id");

        manager.drop_table_indexes("users");

        assert!(!manager.has_index("users", "email"));
        assert!(!manager.has_index("users", "name"));
        assert!(manager.has_index("orders", "user_id"));
    }

    #[test]
    fn test_get_table_indexes() {
        let mut manager = IndexManager::new();

        manager.create_index("users", "email");
        manager.create_index("users", "name");

        let indexes = manager.get_table_indexes("users");
        assert_eq!(indexes.len(), 2);
    }
}
