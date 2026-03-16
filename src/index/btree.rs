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
/// 未来可以替换为手写的 B+ 树以支持更多特性
#[derive(Debug, Clone)]
pub struct BTreeIndex {
    pub order: usize,
    pub root: NodeId,
    pub first_leaf: Option<NodeId>,
    pub last_leaf: Option<NodeId>,
    pub nodes: HashMap<NodeId, BTreeNode>,
    pub next_node_id: NodeId,
    /// 实际存储数据的 BTreeMap：键 -> RowId 列表（支持重复键）
    data: BTreeMap<DbValue, Vec<RowId>>,
}

use std::collections::HashMap;

impl BTreeIndex {
    pub fn new(order: usize) -> Self {
        let mut index = BTreeIndex {
            order,
            root: 0,
            first_leaf: None,
            last_leaf: None,
            nodes: HashMap::new(),
            next_node_id: 0,
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

    /// 插入键值对 - O(log n)
    pub fn insert(&mut self, key: DbValue, value: RowId) {
        self.data.entry(key).or_insert_with(Vec::new).push(value);
    }

    /// 精确查找 - O(log n)
    pub fn search(&self, key: &DbValue) -> Vec<RowId> {
        self.data.get(key).cloned().unwrap_or_default()
    }

    /// 范围查找 [start, end) - O(log n + k)，k 为结果数
    pub fn range(&self, start: &DbValue, end: &DbValue) -> Vec<RowId> {
        let mut results = Vec::new();

        for (_key, values) in self.data.range((start)..(end)) {
            results.extend(values.iter().copied());
        }

        results
    }

    /// 范围查找 >= start
    pub fn range_from(&self, start: &DbValue) -> Vec<RowId> {
        let mut results = Vec::new();

        for (_key, values) in self.data.range((start)..) {
            results.extend(values.iter().copied());
        }

        results
    }

    /// 删除 - O(log n)
    pub fn remove(&mut self, key: &DbValue, value: RowId) -> bool {
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
}
