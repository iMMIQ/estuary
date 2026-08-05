use std::collections::{HashMap, HashSet};

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
    pub authoritative_nodes: HashSet<String>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NodeCacheSnapshot {
    pub authoritative: bool,
    pub blocks: usize,
}

#[derive(Debug, Default)]
pub struct ExactCacheDirectory {
    nodes: RwLock<HashMap<String, NodeCache>>,
}

impl ExactCacheDirectory {
    pub fn configure_node(&self, node_id: &str, max_blocks: usize) {
        self.configure_node_owned(node_id, max_blocks, 0);
    }

    pub fn remove_node(&self, node_id: &str) {
        self.nodes.write().remove(node_id);
    }

    pub fn configure_node_owned(&self, node_id: &str, max_blocks: usize, owner: u64) {
        let mut nodes = self.nodes.write();
        match nodes.get_mut(node_id) {
            Some(node) if node.owner == owner => node.max_blocks = max_blocks,
            _ => {
                nodes.insert(node_id.to_owned(), NodeCache::new(max_blocks, owner));
            }
        }
    }

    pub fn remove_node_owned(&self, node_id: &str, owner: u64) {
        let mut nodes = self.nodes.write();
        if nodes.get(node_id).is_some_and(|node| node.owner == owner) {
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
        let mut nodes = self.nodes.write();
        let Some(node) = nodes.get_mut(node_id) else {
            bail!("KV cache node {node_id} was not configured");
        };
        if node.owner != owner {
            return Ok(());
        }
        for mutation in mutations {
            match mutation {
                CacheMutation::Store {
                    hashes,
                    parent,
                    token_ids,
                    block_size,
                    group,
                } => node.store(group, hashes, parent.as_ref(), &token_ids, block_size)?,
                CacheMutation::Remove { hashes, group } => node.remove(group, &hashes),
                CacheMutation::Clear => {
                    node.clear_authoritative();
                }
            }
        }
        node.authoritative = true;
        Ok(())
    }

    pub fn invalidate_node(&self, node_id: &str) {
        self.invalidate_node_owned(node_id, 0);
    }

    pub fn invalidate_node_owned(&self, node_id: &str, owner: u64) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            if node.owner == owner {
                node.invalidate();
            }
        }
    }

    pub fn suspend_node(&self, node_id: &str) {
        self.suspend_node_owned(node_id, 0);
    }

    pub fn suspend_node_owned(&self, node_id: &str, owner: u64) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            if node.owner == owner {
                node.authoritative = false;
            }
        }
    }

    pub fn resume_node(&self, node_id: &str) {
        self.resume_node_owned(node_id, 0);
    }

    pub fn resume_node_owned(&self, node_id: &str, owner: u64) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            if node.owner == owner {
                node.authoritative = true;
            }
        }
    }

    pub fn matches(&self, token_ids: &[u64]) -> ExactPrefixMatch {
        let nodes = self.nodes.read();
        let mut result = ExactPrefixMatch::default();
        for (node_id, node) in &*nodes {
            if !node.authoritative {
                continue;
            }
            result.authoritative_nodes.insert(node_id.clone());
            result
                .matched_tokens
                .insert(node_id.clone(), node.longest_match(token_ids));
        }
        result
    }

    pub fn snapshot(&self, node_id: &str) -> NodeCacheSnapshot {
        self.nodes
            .read()
            .get(node_id)
            .map_or_else(NodeCacheSnapshot::default, |node| NodeCacheSnapshot {
                authoritative: node.authoritative,
                blocks: node.block_count,
            })
    }
}

#[derive(Debug)]
struct NodeCache {
    owner: u64,
    groups: HashMap<i64, TokenTrie>,
    authoritative: bool,
    block_count: usize,
    max_blocks: usize,
}

impl NodeCache {
    fn new(max_blocks: usize, owner: u64) -> Self {
        Self {
            owner,
            groups: HashMap::new(),
            authoritative: false,
            block_count: 0,
            max_blocks,
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

        let trie = self.groups.entry(group).or_default();
        let additions = hashes
            .iter()
            .filter(|hash| !trie.hashes.contains_key(*hash))
            .count();
        if self.block_count.saturating_add(additions) > self.max_blocks {
            bail!("vLLM KV directory exceeded its configured block limit");
        }
        trie.store(hashes, parent, token_ids, block_size)?;
        self.block_count = self.block_count.saturating_add(additions);
        Ok(())
    }

    fn remove(&mut self, group: i64, hashes: &[BlockHash]) {
        let Some(trie) = self.groups.get_mut(&group) else {
            return;
        };
        for hash in hashes {
            if trie.remove(hash) {
                self.block_count = self.block_count.saturating_sub(1);
            }
        }
    }

    fn longest_match(&self, token_ids: &[u64]) -> usize {
        if self.groups.is_empty() {
            return 0;
        }
        self.groups
            .values()
            .map(|group| group.longest_match(token_ids))
            .min()
            .unwrap_or_default()
    }

    fn clear_authoritative(&mut self) {
        self.groups.clear();
        self.block_count = 0;
        self.authoritative = true;
    }

    fn invalidate(&mut self) {
        self.groups.clear();
        self.block_count = 0;
        self.authoritative = false;
    }
}

#[derive(Debug)]
struct TrieNode {
    parent: Option<(usize, u64)>,
    children: HashMap<u64, usize>,
    terminals: usize,
    depth: usize,
}

impl TrieNode {
    fn root() -> Self {
        Self {
            parent: None,
            children: HashMap::new(),
            terminals: 0,
            depth: 0,
        }
    }
}

#[derive(Debug)]
struct TokenTrie {
    nodes: Vec<Option<TrieNode>>,
    free: Vec<usize>,
    hashes: HashMap<BlockHash, usize>,
}

impl Default for TokenTrie {
    fn default() -> Self {
        Self {
            nodes: vec![Some(TrieNode::root())],
            free: Vec::new(),
            hashes: HashMap::new(),
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
    ) -> Result<()> {
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
            for token in &token_ids[start..start + block_size] {
                current = self.child_or_insert(current, *token);
            }
            if let Some(existing) = self.hashes.get(&hash) {
                if *existing != current {
                    bail!("vLLM KV hash mapped to conflicting token prefixes");
                }
                continue;
            }
            self.hashes.insert(hash, current);
            self.node_mut(current).terminals = self.node(current).terminals.saturating_add(1);
        }
        Ok(())
    }

    fn child_or_insert(&mut self, parent: usize, token: u64) -> usize {
        if let Some(child) = self.node(parent).children.get(&token) {
            return *child;
        }
        let depth = self.node(parent).depth.saturating_add(1);
        let node = TrieNode {
            parent: Some((parent, token)),
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
        self.node_mut(parent).children.insert(token, index);
        index
    }

    fn remove(&mut self, hash: &BlockHash) -> bool {
        let Some(mut index) = self.hashes.remove(hash) else {
            return false;
        };
        self.node_mut(index).terminals = self.node(index).terminals.saturating_sub(1);
        while index != 0 && self.node(index).terminals == 0 && self.node(index).children.is_empty()
        {
            let (parent, token) = self
                .node(index)
                .parent
                .expect("non-root trie node has a parent");
            self.nodes[index] = None;
            self.free.push(index);
            self.node_mut(parent).children.remove(&token);
            index = parent;
        }
        true
    }

    fn longest_match(&self, token_ids: &[u64]) -> usize {
        let mut current = 0;
        let mut matched = 0;
        for token in token_ids {
            let Some(child) = self.node(current).children.get(token) else {
                break;
            };
            current = *child;
            if self.node(current).terminals > 0 {
                matched = self.node(current).depth;
            }
        }
        matched
    }

    fn node(&self, index: usize) -> &TrieNode {
        self.nodes[index]
            .as_ref()
            .expect("active trie index contains a node")
    }

    fn node_mut(&mut self, index: usize) -> &mut TrieNode {
        self.nodes[index]
            .as_mut()
            .expect("active trie index contains a node")
    }
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
        assert!(directory.matches(&[1]).authoritative_nodes.contains("a"));
        directory.invalidate_node("a");
        assert!(!directory.matches(&[1]).authoritative_nodes.contains("a"));
    }

    #[test]
    fn stale_runtime_cannot_mutate_a_replacement_directory() {
        let directory = ExactCacheDirectory::default();
        directory.configure_node_owned("a", 10, 1);
        directory.configure_node_owned("a", 10, 2);
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
