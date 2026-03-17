use crate::types::DbValue;
use crate::storage::RowId;
use std::collections::BTreeMap;

/// 节点 ID 类型
pub type NodeId = u64;

/// B+ 树的阶数
pub const DEFAULT_ORDER: usize = 4;

/// B+ 树节点（保留结构定义，用于未来扩展）
#[derive(Debug, Clone)]
pub enum BTreeNode {
    Internal(BTreeInternalNode),
    Leaf(BTreeLeafNode),
}

#[derive(Debug, Clone)]
pub struct BTreeInternalNode {
    pub keys: Vec<DbValue>,
    pub children: Vec<NodeId>,
}

impl BTreeInternalNode {
    pub fn new() -> Self {
        BTreeInternalNode {
            keys: Vec::new(),
            children: Vec::new(),
        }
    }
}

impl Default for BTreeInternalNode {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct BTreeLeafNode {
    pub keys: Vec<DbValue>,
    pub values: Vec<RowId>,
    pub next: Option<NodeId>,
}

impl BTreeLeafNode {
    pub fn new() -> Self {
        BTreeLeafNode {
            keys: Vec::new(),
            values: Vec::new(),
            next: None,
        }
    }
}

impl Default for BTreeLeafNode {
    fn default() -> Self {
        Self::new()
    }
}

/// B+ 树索引
/// 当前使用 BTreeMap 实现，提供 O(log n) 的查找性能
/// 支持单列索引和复合索引
#[derive(Debug, Clone)]
pub struct BTreeIndex {
    pub order: usize,
    pub root: NodeId,
    pub first_leaf: Option<NodeId>,
    pub last_leaf: Option<NodeId>,
    pub nodes: HashMap<NodeId, BTreeNode>,
    pub next_node_id: NodeId,
    /// 索引列名列表（单列为 1 个元素，复合索引为多个元素）
    pub columns: Vec<String>,
    /// 是否为复合索引
    pub is_composite: bool,
    /// 实际存储数据的 BTreeMap：键 -> RowId 列表（支持重复键）
    /// 单列索引：Vec 长度为 1
    /// 复合索引：Vec 长度为列数
    data: BTreeMap<Vec<DbValue>, Vec<RowId>>,
}

use std::collections::HashMap;

impl BTreeIndex {
    /// 创建单列索引
    pub fn new(order: usize) -> Self {
        Self::new_composite(order, &["column"])
    }

    /// 创建复合索引
    pub fn new_composite(order: usize, columns: &[&str]) -> Self {
        let mut index = BTreeIndex {
            order,
            root: 0,
            first_leaf: None,
            last_leaf: None,
            nodes: HashMap::new(),
            next_node_id: 0,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            is_composite: columns.len() > 1,
            data: BTreeMap::new(),
        };

        // 初始化空树
        let leaf_id = index.allocate_node();
        let leaf = BTreeLeafNode::new();
        index.nodes.insert(leaf_id, BTreeNode::Leaf(leaf));
        index.root = leaf_id;
        index.first_leaf = Some(leaf_id);
        index.last_leaf = Some(leaf_id);

        index
    }

    fn allocate_node(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    /// 插入键值对（单列索引）- O(log n)
    pub fn insert(&mut self, key: DbValue, value: RowId) {
        self.insert_composite(vec![key], value);
    }

    /// 插入复合键值对 - O(log n)
    pub fn insert_composite(&mut self, key: Vec<DbValue>, value: RowId) {
        self.data.entry(key).or_insert_with(Vec::new).push(value);
    }

    /// 精确查找（单列索引）- O(log n)
    pub fn search(&self, key: &DbValue) -> Vec<RowId> {
        self.search_composite(&[key.clone()])
    }

    /// 复合键精确查找 - O(log n)
    pub fn search_composite(&self, key: &[DbValue]) -> Vec<RowId> {
        self.data.get(key).cloned().unwrap_or_default()
    }

    /// 范围查找（单列索引）[start, end) - O(log n + k)，k 为结果数
    pub fn range(&self, start: &DbValue, end: &DbValue) -> Vec<RowId> {
        self.range_composite(&[start.clone()], &[end.clone()])
    }

    /// 复合键范围查找 [start, end) - O(log n + k)
    pub fn range_composite(&self, start: &[DbValue], end: &[DbValue]) -> Vec<RowId> {
        let mut results = Vec::new();

        // 使用 Vec<DbValue> 作为 range 的边界
        let start_vec = start.to_vec();
        let end_vec = end.to_vec();
        for (_key, values) in self.data.range(start_vec..end_vec) {
            results.extend(values.iter().copied());
        }

        results
    }

    /// 范围查找 >= start（单列索引）
    pub fn range_from(&self, start: &DbValue) -> Vec<RowId> {
        let mut results = Vec::new();

        for (_key, values) in self.data.range([start.clone()].to_vec()..) {
            results.extend(values.iter().copied());
        }

        results
    }

    /// 复合键范围查找 >= start
    pub fn range_from_composite(&self, start: &[DbValue]) -> Vec<RowId> {
        let mut results = Vec::new();

        let start_vec = start.to_vec();
        for (_key, values) in self.data.range(start_vec..) {
            results.extend(values.iter().copied());
        }

        results
    }

    /// 删除（单列索引）- O(log n)
    pub fn remove(&mut self, key: &DbValue, value: RowId) -> bool {
        self.remove_composite(&[key.clone()], value)
    }

    /// 复合键删除 - O(log n)
    pub fn remove_composite(&mut self, key: &[DbValue], value: RowId) -> bool {
        if let Some(values) = self.data.get_mut(key) {
            if let Some(pos) = values.iter().position(|&v| v == value) {
                values.remove(pos);
                if values.is_empty() {
                    self.data.remove(key);
                }
                return true;
            }
        }
        false
    }

    pub fn len(&self) -> usize {
        self.data.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Default for BTreeIndex {
    fn default() -> Self {
        Self::new(DEFAULT_ORDER)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_insert_search() {
        let mut btree = BTreeIndex::new(4);

        btree.insert(DbValue::integer(10), RowId::new(1));
        btree.insert(DbValue::integer(20), RowId::new(2));
        btree.insert(DbValue::integer(30), RowId::new(3));

        let results = btree.search(&DbValue::integer(20));
        assert_eq!(results, vec![RowId::new(2)]);
    }

    #[test]
    fn test_btree_duplicate_keys() {
        let mut btree = BTreeIndex::new(4);

        btree.insert(DbValue::integer(10), RowId::new(1));
        btree.insert(DbValue::integer(10), RowId::new(2));

        let results = btree.search(&DbValue::integer(10));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_btree_range() {
        let mut btree = BTreeIndex::new(4);

        for i in 1..=10 {
            btree.insert(DbValue::integer(i), RowId::new(i as u64));
        }

        let results = btree.range(&DbValue::integer(3), &DbValue::integer(7));
        assert_eq!(results.len(), 4); // 3, 4, 5, 6
    }

    #[test]
    fn test_btree_large_insert() {
        let mut btree = BTreeIndex::new(4);

        for i in 0..1000 {
            btree.insert(DbValue::integer(i), RowId::new(i as u64));
        }

        assert_eq!(btree.len(), 1000);

        for i in 0..1000 {
            let results = btree.search(&DbValue::integer(i));
            assert_eq!(results, vec![RowId::new(i as u64)]);
        }
    }

    #[test]
    fn test_btree_remove() {
        let mut btree = BTreeIndex::new(4);

        btree.insert(DbValue::integer(10), RowId::new(1));
        btree.insert(DbValue::integer(10), RowId::new(2));

        assert_eq!(btree.search(&DbValue::integer(10)).len(), 2);

        btree.remove(&DbValue::integer(10), RowId::new(1));
        assert_eq!(btree.search(&DbValue::integer(10)).len(), 1);
    }

    #[test]
    fn test_composite_index_insert_search() {
        let mut btree = BTreeIndex::new_composite(4, &["age", "city"]);
        assert!(btree.is_composite);
        assert_eq!(btree.columns, vec!["age", "city"]);

        // 插入复合键 (age=25, city="Beijing")
        btree.insert_composite(
            vec![DbValue::integer(25), DbValue::text("Beijing")],
            RowId::new(1)
        );
        btree.insert_composite(
            vec![DbValue::integer(25), DbValue::text("Shanghai")],
            RowId::new(2)
        );
        btree.insert_composite(
            vec![DbValue::integer(30), DbValue::text("Beijing")],
            RowId::new(3)
        );

        // 精确查找
        let results = btree.search_composite(&[
            DbValue::integer(25),
            DbValue::text("Beijing")
        ]);
        assert_eq!(results, vec![RowId::new(1)]);
    }

    #[test]
    fn test_composite_index_range() {
        let mut btree = BTreeIndex::new_composite(4, &["age", "city"]);

        // 插入数据
        for age in 20..30 {
            for city in &["Beijing", "Shanghai"] {
                btree.insert_composite(
                    vec![DbValue::integer(age), DbValue::text(*city)],
                    RowId::new((age * 10) as u64)
                );
            }
        }

        // 范围查找：age >= 25
        let results = btree.range_from_composite(&[DbValue::integer(25)]);
        assert_eq!(results.len(), 10); // 5 ages * 2 cities
    }

    #[test]
    fn test_composite_index_partial_match() {
        let mut btree = BTreeIndex::new_composite(4, &["a", "b", "c"]);

        // 插入数据
        btree.insert_composite(vec![DbValue::integer(1), DbValue::integer(2), DbValue::integer(3)], RowId::new(1));
        btree.insert_composite(vec![DbValue::integer(1), DbValue::integer(2), DbValue::integer(4)], RowId::new(2));
        btree.insert_composite(vec![DbValue::integer(1), DbValue::integer(3), DbValue::integer(5)], RowId::new(3));

        // 前缀匹配：a=1, b=2
        let results = btree.search_composite(&[
            DbValue::integer(1),
            DbValue::integer(2)
        ]);
        // 注意：复合索引精确匹配需要完整键，这里应该返回空
        // 前缀匹配需要使用 range
        assert!(results.is_empty());
    }
}
