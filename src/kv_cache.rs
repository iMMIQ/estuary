use std::{
    collections::{HashMap, HashSet},
    mem::{size_of, size_of_val},
    sync::Arc,
};

use anyhow::{Result, bail};
use parking_lot::RwLock;

const MAX_BLOCK_TOKENS: usize = 4_096;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum BlockHash {
    Bytes(Vec<u8>),
    Integer(u64),
}

#[derive(Debug)]
pub enum CacheMutation {
    Store {
        hashes: Vec<BlockHash>,
        parent: Option<BlockHash>,
        token_ids: Vec<u64>,
        block_size: usize,
        group: i64,
    },
    Remove {
        hashes: Vec<BlockHash>,
        group: i64,
    },
    Clear,
}

#[derive(Debug, Default)]
pub struct ExactPrefixMatch {
    pub matched_tokens: HashMap<String, usize>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NodeCacheSnapshot {
    pub authoritative: bool,
    pub blocks: usize,
    pub bytes: usize,
}

#[derive(Debug, Default)]
pub struct ExactCacheDirectory {
    nodes: RwLock<HashMap<String, Arc<RwLock<NodeCache>>>>,
}

impl ExactCacheDirectory {
    pub fn configure_node(&self, node_id: &str, max_blocks: usize) {
        self.configure_node_owned(node_id, max_blocks, usize::MAX, 0);
    }

    pub fn configure_node_owned(
        &self,
        node_id: &str,
        max_blocks: usize,
        max_bytes: usize,
        owner: u64,
    ) {
        let mut nodes = self.nodes.write();
        if let Some(existing) = nodes.get(node_id) {
            let mut node = existing.write();
            if node.owner == owner {
                node.max_blocks = max_blocks;
                node.max_bytes = max_bytes;
                return;
            }
        }
        nodes.insert(
            node_id.to_owned(),
            Arc::new(RwLock::new(NodeCache::new(max_blocks, max_bytes, owner))),
        );
    }

    pub fn remove_node_owned(&self, node_id: &str, owner: u64) {
        let mut nodes = self.nodes.write();
        if nodes
            .get(node_id)
            .is_some_and(|node| node.read().owner == owner)
        {
            nodes.remove(node_id);
        }
    }

    pub fn apply(&self, node_id: &str, mutations: Vec<CacheMutation>) -> Result<()> {
        self.apply_owned(node_id, 0, mutations)
    }

    pub fn apply_owned(
        &self,
        node_id: &str,
        owner: u64,
        mutations: Vec<CacheMutation>,
    ) -> Result<()> {
        let node_lock = {
            let nodes = self.nodes.read();
            let Some(node) = nodes.get(node_id) else {
                bail!("KV cache node {node_id} was not configured");
            };
            Arc::clone(node)
        };
        let mut node = node_lock.write();
        if node.owner != owner {
            return Ok(());
        }
        for mutation in mutations {
            let result = match mutation {
                CacheMutation::Store {
                    hashes,
                    parent,
                    token_ids,
                    block_size,
                    group,
                } => node.store(group, hashes, parent.as_ref(), &token_ids, block_size),
                CacheMutation::Remove { hashes, group } => {
                    node.remove(group, &hashes);
                    Ok(())
                }
                CacheMutation::Clear => {
                    node.clear_authoritative();
                    Ok(())
                }
            };
            if let Err(error) = result {
                node.invalidate();
                return Err(error);
            }
        }
        node.authoritative = true;
        Ok(())
    }

    fn with_owned_node(&self, node_id: &str, owner: u64, f: impl FnOnce(&mut NodeCache)) {
        if let Some(node_lock) = self.nodes.read().get(node_id).cloned() {
            let mut node = node_lock.write();
            if node.owner == owner {
                f(&mut node);
            }
        }
    }

    #[cfg(test)]
    pub fn invalidate_node(&self, node_id: &str) {
        self.invalidate_node_owned(node_id, 0);
    }

    pub fn invalidate_node_owned(&self, node_id: &str, owner: u64) {
        self.with_owned_node(node_id, owner, NodeCache::invalidate);
    }

    #[cfg(test)]
    pub fn suspend_node(&self, node_id: &str) {
        self.suspend_node_owned(node_id, 0);
    }

    pub fn suspend_node_owned(&self, node_id: &str, owner: u64) {
        self.with_owned_node(node_id, owner, |node| node.authoritative = false);
    }

    pub fn resume_node_owned(&self, node_id: &str, owner: u64) {
        self.with_owned_node(node_id, owner, |node| node.authoritative = true);
    }

    pub fn matches(&self, token_ids: &[u64]) -> ExactPrefixMatch {
        let entries: Vec<(String, Arc<RwLock<NodeCache>>)> = self
            .nodes
            .read()
            .iter()
            .map(|(id, lock)| (id.clone(), Arc::clone(lock)))
            .collect();
        let mut matched_tokens = HashMap::new();
        for (node_id, node_lock) in entries {
            let node = node_lock.read();
            if !node.authoritative {
                continue;
            }
            let matched = node.longest_match(token_ids);
            drop(node);
            matched_tokens.insert(node_id, matched);
        }
        ExactPrefixMatch { matched_tokens }
    }

    pub fn snapshot(&self, node_id: &str) -> NodeCacheSnapshot {
        self.nodes.read().get(node_id).cloned().map_or_else(
            NodeCacheSnapshot::default,
            |node_lock| {
                let node = node_lock.read();
                NodeCacheSnapshot {
                    authoritative: node.authoritative,
                    blocks: node.block_count,
                    bytes: node.directory_bytes,
                }
            },
        )
    }
}

#[derive(Debug)]
struct NodeCache {
    owner: u64,
    groups: HashMap<i64, TokenTrie>,
    authoritative: bool,
    block_count: usize,
    max_blocks: usize,
    directory_bytes: usize,
    max_bytes: usize,
}

impl NodeCache {
    fn new(max_blocks: usize, max_bytes: usize, owner: u64) -> Self {
        Self {
            owner,
            groups: HashMap::new(),
            authoritative: false,
            block_count: 0,
            max_blocks,
            directory_bytes: 0,
            max_bytes,
        }
    }

    fn store(
        &mut self,
        group: i64,
        hashes: Vec<BlockHash>,
        parent: Option<&BlockHash>,
        token_ids: &[u64],
        block_size: usize,
    ) -> Result<()> {
        if hashes.is_empty() {
            return Ok(());
        }
        if block_size == 0 || block_size > MAX_BLOCK_TOKENS {
            bail!("invalid vLLM KV block size {block_size}");
        }
        if token_ids.len() != hashes.len().saturating_mul(block_size) {
            bail!(
                "vLLM KV event has {} tokens for {} blocks of size {block_size}",
                token_ids.len(),
                hashes.len()
            );
        }

        let additions = hashes
            .iter()
            .filter(|hash| {
                self.groups
                    .get(&group)
                    .is_none_or(|trie| !trie.hashes.contains_key(*hash))
            })
            .collect::<HashSet<_>>()
            .len();
        if self.block_count.saturating_add(additions) > self.max_blocks {
            bail!("vLLM KV directory exceeded its configured block limit");
        }
        let new_group = !self.groups.contains_key(&group);
        let group_overhead = usize::from(new_group) * size_of::<(i64, TokenTrie)>();
        let trie = self.groups.entry(group).or_default();
        let before = if new_group { 0 } else { trie.memory_bytes };
        let byte_budget = self
            .max_bytes
            .saturating_sub(self.directory_bytes.saturating_add(group_overhead))
            .saturating_add(before);
        if trie.memory_bytes > byte_budget {
            bail!("vLLM KV directory exceeded its configured byte limit");
        }
        trie.store(hashes, parent, token_ids, block_size, byte_budget)?;
        self.directory_bytes = self
            .directory_bytes
            .saturating_add(trie.memory_bytes.saturating_sub(before))
            .saturating_add(group_overhead);
        self.block_count = self.block_count.saturating_add(additions);
        Ok(())
    }

    fn remove(&mut self, group: i64, hashes: &[BlockHash]) {
        let Some(trie) = self.groups.get_mut(&group) else {
            return;
        };
        for hash in hashes {
            if let Some(removed_bytes) = trie.remove(hash) {
                self.block_count = self.block_count.saturating_sub(1);
                self.directory_bytes = self.directory_bytes.saturating_sub(removed_bytes);
            }
        }
    }

    fn longest_match(&self, token_ids: &[u64]) -> usize {
        self.groups
            .values()
            .map(|group| group.longest_match(token_ids))
            .min()
            .unwrap_or_default()
    }

    fn clear_authoritative(&mut self) {
        self.groups.clear();
        self.block_count = 0;
        self.directory_bytes = 0;
        self.authoritative = true;
    }

    fn invalidate(&mut self) {
        self.groups.clear();
        self.block_count = 0;
        self.directory_bytes = 0;
        self.authoritative = false;
    }
}

#[derive(Debug)]
struct BlockNode {
    parent: Option<usize>,
    edge: Box<[u64]>,
    children: HashMap<Box<[u64]>, usize>,
    terminals: usize,
    depth: usize,
}

impl BlockNode {
    fn root() -> Self {
        Self {
            parent: None,
            edge: Box::default(),
            children: HashMap::new(),
            terminals: 0,
            depth: 0,
        }
    }
}

#[derive(Debug)]
struct TokenTrie {
    nodes: Vec<Option<BlockNode>>,
    free: Vec<usize>,
    hashes: HashMap<BlockHash, usize>,
    block_size: Option<usize>,
    memory_bytes: usize,
}

impl Default for TokenTrie {
    fn default() -> Self {
        Self {
            nodes: vec![Some(BlockNode::root())],
            free: Vec::new(),
            hashes: HashMap::new(),
            block_size: None,
            memory_bytes: size_of::<BlockNode>(),
        }
    }
}

impl TokenTrie {
    fn store(
        &mut self,
        hashes: Vec<BlockHash>,
        parent: Option<&BlockHash>,
        token_ids: &[u64],
        block_size: usize,
        max_bytes: usize,
    ) -> Result<()> {
        if self
            .block_size
            .is_some_and(|existing| existing != block_size)
        {
            bail!("vLLM KV cache group changed block size");
        }
        self.block_size = Some(block_size);
        let mut current = match parent {
            Some(hash) => self
                .hashes
                .get(hash)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("vLLM KV parent hash is unknown"))?,
            None => 0,
        };

        for (index, hash) in hashes.into_iter().enumerate() {
            let start = index * block_size;
            current =
                self.child_or_insert(current, &token_ids[start..start + block_size], max_bytes)?;
            if let Some(existing) = self.hashes.get(&hash) {
                if *existing != current {
                    bail!("vLLM KV hash mapped to conflicting token prefixes");
                }
                continue;
            }
            let next_bytes = self.memory_bytes.saturating_add(hash_memory_bytes(&hash));
            if next_bytes > max_bytes {
                bail!("vLLM KV directory exceeded its configured byte limit");
            }
            self.memory_bytes = next_bytes;
            self.hashes.insert(hash, current);
            self.node_mut(current).terminals = self.node(current).terminals.saturating_add(1);
        }
        Ok(())
    }

    fn child_or_insert(&mut self, parent: usize, edge: &[u64], max_bytes: usize) -> Result<usize> {
        if let Some(&child) = self.node(parent).children.get(edge) {
            return Ok(child);
        }
        let next_bytes = self
            .memory_bytes
            .saturating_add(block_node_memory_bytes(edge));
        if next_bytes > max_bytes {
            bail!("vLLM KV directory exceeded its configured byte limit");
        }
        let depth = self.node(parent).depth.saturating_add(edge.len());
        let node = BlockNode {
            parent: Some(parent),
            edge: edge.into(),
            children: HashMap::new(),
            terminals: 0,
            depth,
        };
        let index = if let Some(index) = self.free.pop() {
            self.nodes[index] = Some(node);
            index
        } else {
            self.nodes.push(Some(node));
            self.nodes.len() - 1
        };
        self.node_mut(parent).children.insert(edge.into(), index);
        self.memory_bytes = next_bytes;
        Ok(index)
    }

    fn remove(&mut self, hash: &BlockHash) -> Option<usize> {
        let mut index = self.hashes.remove(hash)?;
        let before = self.memory_bytes;
        self.memory_bytes = self.memory_bytes.saturating_sub(hash_memory_bytes(hash));
        self.node_mut(index).terminals = self.node(index).terminals.saturating_sub(1);
        while index != 0 && self.node(index).terminals == 0 && self.node(index).children.is_empty()
        {
            let removed = self.nodes[index]
                .take()
                .expect("active trie index contains a node");
            let parent = removed.parent.expect("non-root trie node has a parent");
            let removed_bytes = block_node_memory_bytes(&removed.edge);
            self.free.push(index);
            self.node_mut(parent).children.remove(removed.edge.as_ref());
            self.memory_bytes = self.memory_bytes.saturating_sub(removed_bytes);
            index = parent;
        }
        Some(before.saturating_sub(self.memory_bytes))
    }

    fn longest_match(&self, token_ids: &[u64]) -> usize {
        let mut current = 0;
        let mut offset = 0usize;
        let mut matched = 0;
        let Some(block_size) = self.block_size else {
            return 0;
        };
        while offset.saturating_add(block_size) <= token_ids.len() {
            let edge = &token_ids[offset..offset + block_size];
            let Some(&child) = self.node(current).children.get(edge) else {
                break;
            };
            current = child;
            offset += block_size;
            if self.node(current).terminals > 0 {
                matched = self.node(current).depth;
            }
        }
        matched
    }

    fn node(&self, index: usize) -> &BlockNode {
        self.nodes[index]
            .as_ref()
            .expect("active trie index contains a node")
    }

    fn node_mut(&mut self, index: usize) -> &mut BlockNode {
        self.nodes[index]
            .as_mut()
            .expect("active trie index contains a node")
    }
}

fn hash_memory_bytes(hash: &BlockHash) -> usize {
    size_of::<(BlockHash, usize)>()
        + match hash {
            BlockHash::Bytes(bytes) => bytes.len(),
            BlockHash::Integer(_) => 0,
        }
}

fn block_node_memory_bytes(edge: &[u64]) -> usize {
    size_of::<BlockNode>()
        .saturating_add(size_of_val(edge).saturating_mul(2))
        .saturating_add(size_of::<(Box<[u64]>, usize)>())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer(value: u64) -> BlockHash {
        BlockHash::Integer(value)
    }

    #[test]
    fn stores_matches_and_removes_chained_blocks() {
        let directory = ExactCacheDirectory::default();
        directory.configure_node("a", 10);
        directory
            .apply(
                "a",
                vec![CacheMutation::Store {
                    hashes: vec![integer(1), integer(2)],
                    parent: None,
                    token_ids: vec![10, 11, 12, 13],
                    block_size: 2,
                    group: 0,
                }],
            )
            .unwrap();
        assert_eq!(
            directory.matches(&[10, 11, 12, 13, 14]).matched_tokens["a"],
            4
        );

        directory
            .apply(
                "a",
                vec![CacheMutation::Remove {
                    hashes: vec![integer(2)],
                    group: 0,
                }],
            )
            .unwrap();
        assert_eq!(directory.matches(&[10, 11, 12, 13]).matched_tokens["a"], 2);
    }

    #[test]
    fn stores_one_compact_node_per_kv_block_and_tracks_bytes() {
        let mut trie = TokenTrie::default();
        trie.store(
            vec![integer(1), integer(2)],
            None,
            &[10, 11, 12, 13, 14, 15, 16, 17],
            4,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(trie.nodes.iter().flatten().count(), 3);
        let before_remove = trie.memory_bytes;
        assert!(trie.remove(&integer(2)).is_some());
        assert!(trie.memory_bytes < before_remove);
    }

    #[test]
    fn byte_limit_rejects_and_invalidates_the_directory() {
        let directory = ExactCacheDirectory::default();
        directory.configure_node_owned("a", 10, 1, 0);
        let result = directory.apply_owned(
            "a",
            0,
            vec![CacheMutation::Store {
                hashes: vec![integer(1)],
                parent: None,
                token_ids: vec![1, 2],
                block_size: 2,
                group: 0,
            }],
        );

        assert!(result.is_err());
        assert_eq!(directory.snapshot("a").blocks, 0);
        assert_eq!(directory.snapshot("a").bytes, 0);
        assert!(!directory.snapshot("a").authoritative);
    }

    #[test]
    fn requires_all_learned_cache_groups_to_match() {
        let directory = ExactCacheDirectory::default();
        directory.configure_node("a", 10);
        directory
            .apply(
                "a",
                vec![
                    CacheMutation::Store {
                        hashes: vec![integer(1)],
                        parent: None,
                        token_ids: vec![1, 2, 3, 4],
                        block_size: 4,
                        group: 0,
                    },
                    CacheMutation::Store {
                        hashes: vec![integer(2)],
                        parent: None,
                        token_ids: vec![1, 2],
                        block_size: 2,
                        group: 1,
                    },
                ],
            )
            .unwrap();
        assert_eq!(directory.matches(&[1, 2, 3, 4]).matched_tokens["a"], 2);
    }

    #[test]
    fn invalidation_discards_authority() {
        let directory = ExactCacheDirectory::default();
        directory.configure_node("a", 10);
        directory.apply("a", vec![CacheMutation::Clear]).unwrap();
        assert!(directory.matches(&[1]).matched_tokens.contains_key("a"));
        directory.invalidate_node("a");
        assert!(!directory.matches(&[1]).matched_tokens.contains_key("a"));
    }

    #[test]
    fn stale_runtime_cannot_mutate_a_replacement_directory() {
        let directory = ExactCacheDirectory::default();
        directory.configure_node_owned("a", 10, usize::MAX, 1);
        directory.configure_node_owned("a", 10, usize::MAX, 2);
        directory
            .apply_owned(
                "a",
                1,
                vec![CacheMutation::Store {
                    hashes: vec![integer(1)],
                    parent: None,
                    token_ids: vec![1, 2],
                    block_size: 2,
                    group: 0,
                }],
            )
            .unwrap();
        assert_eq!(directory.snapshot("a").blocks, 0);
        assert!(!directory.snapshot("a").authoritative);
    }
}
