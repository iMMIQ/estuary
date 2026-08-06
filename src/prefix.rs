use std::{
    collections::HashMap,
    io::{self, Write},
    sync::Arc,
};

use parking_lot::RwLock;
use serde_json::Value;

use crate::config::PrefixConfig;

const MAX_TREE_KEY_BYTES: usize = 1_024;

#[derive(Clone, Debug, Default)]
pub struct PrefixInput {
    tree_key: String,
    text: String,
    char_count: usize,
    token_ids: Option<Arc<[u64]>>,
}

impl PrefixInput {
    pub fn char_count(&self) -> usize {
        self.char_count
    }

    pub fn set_token_ids(&mut self, token_ids: Vec<u64>) {
        self.token_ids = Some(token_ids.into());
    }

    pub fn token_ids(&self) -> Option<&[u64]> {
        self.token_ids.as_deref()
    }
}

#[derive(Debug, Default)]
pub struct PrefixMatch {
    pub node_ids: Vec<String>,
    pub matched_chars: usize,
    pub input_chars: usize,
}

#[derive(Debug, Default)]
struct RadixNode {
    text: String,
    char_count: usize,
    children: HashMap<char, RadixNode>,
    tenant_last_access: HashMap<String, u64>,
    tenant_oldest_leaf: HashMap<String, u64>,
}

impl RadixNode {
    fn with_text(text: String, tenant: &str, epoch: u64) -> Self {
        let char_count = text.chars().count();
        Self {
            text,
            char_count,
            children: HashMap::new(),
            tenant_last_access: HashMap::from([(tenant.to_owned(), epoch)]),
            tenant_oldest_leaf: HashMap::from([(tenant.to_owned(), epoch)]),
        }
    }
}

#[derive(Debug, Default)]
struct RadixTree {
    root: RadixNode,
    tenant_char_count: HashMap<String, usize>,
    epoch: u64,
}

impl RadixTree {
    fn insert(&mut self, text: &str, tenant: &str, max_chars: usize) {
        if text.is_empty() {
            return;
        }
        self.epoch = self.epoch.wrapping_add(1);
        let epoch = self.epoch;
        self.root
            .tenant_last_access
            .insert(tenant.to_owned(), epoch);
        let count = self.tenant_char_count.entry(tenant.to_owned()).or_default();
        insert_at(&mut self.root, text, tenant, epoch, count);
        refresh_oldest_leaf(&mut self.root, tenant);
        self.evict_tenant(tenant, max_chars);
    }

    fn prefix_match(&self, text: &str, input_chars: usize) -> PrefixMatch {
        if text.is_empty() {
            return PrefixMatch::default();
        }

        let mut current = &self.root;
        let mut remaining = text;
        let mut matched_chars = 0;
        let mut tenants = Vec::new();

        while let Some(first) = remaining.chars().next() {
            let Some(child) = current.children.get(&first) else {
                break;
            };
            let shared = shared_prefix_chars(remaining, &child.text);
            matched_chars += shared;
            tenants = child.tenant_last_access.keys().cloned().collect();
            if shared != child.char_count {
                break;
            }
            remaining = advance_chars(remaining, shared);
            current = child;
        }

        tenants.sort_unstable();
        PrefixMatch {
            node_ids: tenants,
            matched_chars,
            input_chars,
        }
    }

    fn evict_tenant(&mut self, tenant: &str, max_chars: usize) {
        while self
            .tenant_char_count
            .get(tenant)
            .copied()
            .unwrap_or_default()
            > max_chars
        {
            let Some((_, path)) = oldest_leaf(&self.root, tenant, &mut Vec::new()) else {
                break;
            };
            let removed = remove_tenant_at_path(&mut self.root, tenant, &path, 0);
            let count = self.tenant_char_count.entry(tenant.to_owned()).or_default();
            *count = count.saturating_sub(removed);
        }

        if self.tenant_char_count.get(tenant).copied() == Some(0) {
            self.tenant_char_count.remove(tenant);
            self.root.tenant_last_access.remove(tenant);
        }
    }

    fn clear_tenant(&mut self, tenant: &str) {
        clear_tenant_from_node(&mut self.root, tenant);
        self.root.tenant_last_access.remove(tenant);
        self.tenant_char_count.remove(tenant);
    }
}

fn clear_tenant_from_node(node: &mut RadixNode, tenant: &str) {
    node.tenant_last_access.remove(tenant);
    node.tenant_oldest_leaf.remove(tenant);
    node.children.retain(|_, child| {
        clear_tenant_from_node(child, tenant);
        !child.children.is_empty() || !child.tenant_last_access.is_empty()
    });
}

fn insert_at(node: &mut RadixNode, remaining: &str, tenant: &str, epoch: u64, count: &mut usize) {
    if remaining.is_empty() {
        node.tenant_last_access.insert(tenant.to_owned(), epoch);
        refresh_oldest_leaf(node, tenant);
        return;
    }

    let first = remaining
        .chars()
        .next()
        .expect("remaining text is not empty");
    let Some(mut child) = node.children.remove(&first) else {
        let leaf = RadixNode::with_text(remaining.to_owned(), tenant, epoch);
        *count = count.saturating_add(leaf.char_count);
        node.children.insert(first, leaf);
        refresh_oldest_leaf(node, tenant);
        return;
    };

    let shared = shared_prefix_chars(remaining, &child.text);
    if shared == child.char_count {
        if !child.tenant_last_access.contains_key(tenant) {
            *count = count.saturating_add(child.char_count);
            child.tenant_last_access.insert(tenant.to_owned(), 0);
        }
        insert_at(
            &mut child,
            advance_chars(remaining, shared),
            tenant,
            epoch,
            count,
        );
        node.children.insert(first, child);
        refresh_oldest_leaf(node, tenant);
        return;
    }

    let split_byte = byte_index_at_char(&child.text, shared);
    let suffix = child.text.split_off(split_byte);
    child.text = suffix;
    child.char_count -= shared;
    let child_key = child
        .text
        .chars()
        .next()
        .expect("radix suffix is not empty");
    let inherited_tenants = child.tenant_last_access.clone();
    let inherited_oldest = child.tenant_oldest_leaf.clone();

    let mut branch = RadixNode {
        text: take_chars(remaining, shared).to_owned(),
        char_count: shared,
        children: HashMap::from([(child_key, child)]),
        tenant_last_access: inherited_tenants,
        tenant_oldest_leaf: inherited_oldest,
    };
    if !branch.tenant_last_access.contains_key(tenant) {
        *count = count.saturating_add(shared);
        branch.tenant_last_access.insert(tenant.to_owned(), 0);
    }
    insert_at(
        &mut branch,
        advance_chars(remaining, shared),
        tenant,
        epoch,
        count,
    );
    node.children.insert(first, branch);
    refresh_oldest_leaf(node, tenant);
}

fn oldest_leaf(node: &RadixNode, tenant: &str, path: &mut Vec<char>) -> Option<(u64, Vec<char>)> {
    let epoch = *node.tenant_oldest_leaf.get(tenant)?;
    let mut current = node;
    while let Some((key, child)) = current
        .children
        .iter()
        .find(|(_, child)| child.tenant_oldest_leaf.get(tenant).copied() == Some(epoch))
    {
        path.push(*key);
        current = child;
    }
    (!path.is_empty()).then(|| (epoch, path.clone()))
}

fn refresh_oldest_leaf(node: &mut RadixNode, tenant: &str) {
    let oldest = node
        .children
        .values()
        .filter_map(|child| child.tenant_oldest_leaf.get(tenant).copied())
        .min()
        .or_else(|| node.tenant_last_access.get(tenant).copied());
    if let Some(epoch) = oldest {
        node.tenant_oldest_leaf.insert(tenant.to_owned(), epoch);
    } else {
        node.tenant_oldest_leaf.remove(tenant);
    }
}

fn remove_tenant_at_path(node: &mut RadixNode, tenant: &str, path: &[char], depth: usize) -> usize {
    if depth == path.len() {
        if node.tenant_last_access.remove(tenant).is_some() {
            node.tenant_oldest_leaf.remove(tenant);
            return node.char_count;
        }
        return 0;
    }

    let key = path[depth];
    let Some(child) = node.children.get_mut(&key) else {
        return 0;
    };
    let removed = remove_tenant_at_path(child, tenant, path, depth + 1);
    if child.children.is_empty() && child.tenant_last_access.is_empty() {
        node.children.remove(&key);
    }
    refresh_oldest_leaf(node, tenant);
    removed
}

#[derive(Debug)]
pub struct PrefixDirectory {
    enabled: bool,
    max_tree_chars_per_node: usize,
    max_trees: usize,
    max_directory_chars: usize,
    trees: RwLock<PrefixTrees>,
}

#[derive(Debug, Default)]
struct PrefixTrees {
    values: HashMap<String, TreeEntry>,
    epoch: u64,
}

#[derive(Debug)]
struct TreeEntry {
    tree: Arc<RwLock<RadixTree>>,
    last_recorded: u64,
    accounted_chars: usize,
}

impl PrefixDirectory {
    pub fn new(config: &PrefixConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_tree_chars_per_node: config.max_tree_chars_per_node,
            max_trees: config.max_trees,
            max_directory_chars: config.max_directory_chars,
            trees: RwLock::new(PrefixTrees::default()),
        }
    }

    pub fn record(&self, node_id: &str, input: &PrefixInput) {
        if !self.enabled || input.text.is_empty() || input.tree_key.len() > MAX_TREE_KEY_BYTES {
            return;
        }
        let mut trees = self.trees.write();
        trees.epoch = trees.epoch.wrapping_add(1);
        let epoch = trees.epoch;
        if !trees.values.contains_key(&input.tree_key) && trees.values.len() >= self.max_trees {
            evict_oldest_tree(&mut trees.values);
        }
        let entry = trees
            .values
            .entry(input.tree_key.clone())
            .or_insert_with(|| TreeEntry {
                tree: Arc::new(RwLock::new(RadixTree::default())),
                last_recorded: epoch,
                accounted_chars: 0,
            });
        entry.last_recorded = epoch;
        let mut tree = entry.tree.write();
        tree.insert(&input.text, node_id, self.max_tree_chars_per_node);
        entry.accounted_chars = tree
            .tenant_char_count
            .values()
            .fold(0_usize, |total, chars| total.saturating_add(*chars));
        drop(tree);
        while trees.values.values().fold(0_usize, |total, entry| {
            total.saturating_add(entry.accounted_chars)
        }) > self.max_directory_chars
        {
            if !evict_oldest_tree(&mut trees.values) {
                break;
            }
        }
    }

    pub fn best_match(&self, input: &PrefixInput) -> PrefixMatch {
        if !self.enabled || input.text.is_empty() || input.tree_key.len() > MAX_TREE_KEY_BYTES {
            return PrefixMatch::default();
        }
        let tree = self
            .trees
            .read()
            .values
            .get(&input.tree_key)
            .map(|entry| Arc::clone(&entry.tree));
        tree.map_or_else(PrefixMatch::default, |tree| {
            tree.read().prefix_match(&input.text, input.char_count())
        })
    }

    pub fn clear_node(&self, node_id: &str) {
        self.trees.write().values.retain(|_, entry| {
            let mut tree = entry.tree.write();
            tree.clear_tenant(node_id);
            entry.accounted_chars = tree
                .tenant_char_count
                .values()
                .fold(0_usize, |total, chars| total.saturating_add(*chars));
            entry.accounted_chars > 0
        });
    }
}

fn evict_oldest_tree(trees: &mut HashMap<String, TreeEntry>) -> bool {
    let Some(oldest) = trees
        .iter()
        .min_by_key(|(_, entry)| entry.last_recorded)
        .map(|(key, _)| key.clone())
    else {
        return false;
    };
    trees.remove(&oldest);
    true
}

pub fn routing_text(
    endpoint: &str,
    model: Option<&str>,
    body: Option<&Value>,
    config: &PrefixConfig,
) -> PrefixInput {
    if !config.enabled {
        return PrefixInput::default();
    }

    let tree_key = format!("{endpoint}\u{1f}{}", model.unwrap_or("<unspecified>"));
    let mut canonical = BoundedCanonical::new(config.max_request_chars);
    let Some(body) = body else {
        return PrefixInput {
            tree_key,
            text: String::new(),
            char_count: 0,
            token_ids: None,
        };
    };

    for key in [
        "system",
        "instructions",
        "tools",
        "tool_choice",
        "response_format",
        "parallel_tool_calls",
        "reasoning",
    ] {
        if let Some(value) = body.get(key) {
            canonical.append_segment(key, value);
            if canonical.is_full() {
                break;
            }
        }
    }

    let input_key = match endpoint {
        "chat/completions" | "messages" => "messages",
        "completions" => "prompt",
        _ => "input",
    };
    if !canonical.is_full() {
        if let Some(input) = body.get(input_key) {
            if let Some(items) = input.as_array() {
                for item in items {
                    canonical.append_segment(input_key, item);
                    if canonical.is_full() {
                        break;
                    }
                }
            } else {
                canonical.append_segment(input_key, input);
            }
        } else {
            canonical.append_segment("body", body);
        }
    }

    let (text, char_count) = canonical.finish();
    PrefixInput {
        tree_key,
        char_count,
        text,
        token_ids: None,
    }
}

struct BoundedCanonical {
    output: String,
    max_chars: usize,
    chars: usize,
    full: bool,
}

impl BoundedCanonical {
    fn new(max_chars: usize) -> Self {
        Self {
            output: String::new(),
            max_chars,
            chars: 0,
            full: false,
        }
    }

    fn append_segment(&mut self, name: &str, value: &Value) {
        if self.full {
            return;
        }
        self.push_str("\u{1e}");
        self.push_str(name);
        self.push_str("\u{1f}");
        if self.full {
            return;
        }
        let _ = serde_json::to_writer(self, value);
    }

    fn push_str(&mut self, value: &str) {
        if self.full {
            return;
        }
        if value.is_ascii() {
            let written = value.len().min(self.max_chars.saturating_sub(self.chars));
            self.output.push_str(&value[..written]);
            self.chars += written;
            self.full = self.chars == self.max_chars;
            return;
        }
        for character in value.chars() {
            if self.chars == self.max_chars {
                self.full = true;
                break;
            }
            self.output.push(character);
            self.chars += 1;
        }
        self.full = self.chars == self.max_chars;
    }

    fn is_full(&self) -> bool {
        self.full
    }

    fn finish(self) -> (String, usize) {
        (self.output, self.chars)
    }
}

impl Write for BoundedCanonical {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let value = std::str::from_utf8(buffer)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let before = self.output.len();
        self.push_str(value);
        let written = self.output.len() - before;
        if written < buffer.len() {
            Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "prefix canonicalization limit reached",
            ))
        } else {
            Ok(written)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn shared_prefix_chars(left: &str, right: &str) -> usize {
    left.chars()
        .zip(right.chars())
        .take_while(|(left, right)| left == right)
        .count()
}

fn advance_chars(text: &str, count: usize) -> &str {
    let byte = byte_index_at_char(text, count);
    &text[byte..]
}

fn take_chars(text: &str, count: usize) -> &str {
    let byte = byte_index_at_char(text, count);
    &text[..byte]
}

fn byte_index_at_char(text: &str, count: usize) -> usize {
    text.char_indices()
        .nth(count)
        .map_or(text.len(), |(index, _)| index)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn shared_messages_produce_a_longest_prefix_match() {
        let config = PrefixConfig::default();
        let first = json!({
            "messages": [
                {"role": "system", "content": "be precise"},
                {"role": "user", "content": "one"}
            ]
        });
        let second = json!({
            "messages": [
                {"content": "be precise", "role": "system"},
                {"role": "user", "content": "two"}
            ]
        });
        let first = routing_text("chat/completions", Some("m"), Some(&first), &config);
        let second = routing_text("chat/completions", Some("m"), Some(&second), &config);
        let directory = PrefixDirectory::new(&config);
        directory.record("node-a", &first);

        let result = directory.best_match(&second);
        assert_eq!(result.node_ids, ["node-a"]);
        assert!(result.matched_chars > 20);
        assert!(result.matched_chars < result.input_chars);
    }

    #[test]
    fn anthropic_routing_ignores_generation_only_fields() {
        let config = PrefixConfig::default();
        let first = json!({
            "model": "claude",
            "max_tokens": 128,
            "stream": false,
            "system": "be precise",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let second = json!({
            "model": "claude",
            "max_tokens": 4096,
            "stream": true,
            "system": "be precise",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let first = routing_text("messages", Some("claude"), Some(&first), &config);
        let second = routing_text("messages", Some("claude"), Some(&second), &config);
        let directory = PrefixDirectory::new(&config);
        directory.record("node-a", &first);

        let matched = directory.best_match(&second);
        assert_eq!(matched.node_ids, ["node-a"]);
        assert_eq!(matched.matched_chars, matched.input_chars);
    }

    #[test]
    fn selects_tenants_at_the_deepest_shared_node() {
        let config = PrefixConfig::default();
        let directory = PrefixDirectory::new(&config);
        let a = routing_text(
            "completions",
            Some("m"),
            Some(&json!({"prompt": "shared alpha"})),
            &config,
        );
        let b = routing_text(
            "completions",
            Some("m"),
            Some(&json!({"prompt": "shared beta"})),
            &config,
        );
        directory.record("node-a", &a);
        directory.record("node-b", &b);

        let result = directory.best_match(&routing_text(
            "completions",
            Some("m"),
            Some(&json!({"prompt": "shared alphabet"})),
            &config,
        ));
        assert_eq!(result.node_ids, ["node-a"]);
    }

    #[test]
    fn tree_isolated_by_model_and_endpoint() {
        let config = PrefixConfig::default();
        let directory = PrefixDirectory::new(&config);
        let input = routing_text(
            "completions",
            Some("a"),
            Some(&json!({"prompt": "same"})),
            &config,
        );
        directory.record("node-a", &input);

        let other_model = routing_text(
            "completions",
            Some("b"),
            Some(&json!({"prompt": "same"})),
            &config,
        );
        assert!(directory.best_match(&other_model).node_ids.is_empty());
    }

    #[test]
    fn directory_evicts_old_model_trees_and_removes_empty_trees() {
        let config = PrefixConfig {
            max_trees: 2,
            ..PrefixConfig::default()
        };
        let directory = PrefixDirectory::new(&config);
        for model in ["a", "b", "c"] {
            let input = routing_text(
                "completions",
                Some(model),
                Some(&json!({"prompt": model})),
                &config,
            );
            directory.record("node-a", &input);
        }
        assert_eq!(directory.trees.read().values.len(), 2);
        let evicted = routing_text(
            "completions",
            Some("a"),
            Some(&json!({"prompt": "a"})),
            &config,
        );
        assert!(directory.best_match(&evicted).node_ids.is_empty());

        directory.clear_node("node-a");
        assert!(directory.trees.read().values.is_empty());
    }

    #[test]
    fn directory_enforces_global_character_budget_and_tree_key_limit() {
        let config = PrefixConfig {
            max_trees: 10,
            max_directory_chars: 80,
            ..PrefixConfig::default()
        };
        let directory = PrefixDirectory::new(&config);
        for model in ["a", "b", "c"] {
            directory.record(
                "node-a",
                &routing_text(
                    "completions",
                    Some(model),
                    Some(&json!({"prompt": "a moderately long prompt value"})),
                    &config,
                ),
            );
        }
        let trees = directory.trees.read();
        assert!(
            trees
                .values
                .values()
                .map(|entry| entry.accounted_chars)
                .sum::<usize>()
                <= config.max_directory_chars
        );
        drop(trees);

        let before = directory.trees.read().values.len();
        let long_model = "m".repeat(MAX_TREE_KEY_BYTES + 1);
        directory.record(
            "node-a",
            &routing_text(
                "completions",
                Some(&long_model),
                Some(&json!({"prompt": "ignored"})),
                &config,
            ),
        );
        assert_eq!(directory.trees.read().values.len(), before);
    }

    #[test]
    fn eviction_bounds_each_tenant_tree() {
        let config = PrefixConfig {
            max_tree_chars_per_node: 24,
            ..PrefixConfig::default()
        };
        let directory = PrefixDirectory::new(&config);
        for suffix in ["aaaaaaaa", "bbbbbbbb", "cccccccc", "dddddddd"] {
            let input = routing_text(
                "completions",
                Some("m"),
                Some(&json!({"prompt": format!("shared-{suffix}")})),
                &config,
            );
            directory.record("node-a", &input);
        }
        let trees = directory.trees.read();
        let tree = trees
            .values
            .values()
            .next()
            .expect("one model tree")
            .tree
            .read();
        assert!(
            tree.tenant_char_count
                .get("node-a")
                .copied()
                .unwrap_or_default()
                <= 24
        );
    }

    #[test]
    fn eviction_index_respects_a_refreshed_leaf_epoch() {
        let mut tree = RadixTree::default();
        tree.insert("aaaa", "node-a", 8);
        tree.insert("bbbb", "node-a", 8);
        tree.insert("aaaa", "node-a", 8);
        tree.insert("cccc", "node-a", 8);

        assert_eq!(tree.prefix_match("aaaa", 4).node_ids, ["node-a"]);
        assert!(tree.prefix_match("bbbb", 4).node_ids.is_empty());
        assert_eq!(tree.prefix_match("cccc", 4).node_ids, ["node-a"]);
    }

    #[test]
    fn canonicalization_stops_at_the_configured_character_limit() {
        let config = PrefixConfig {
            max_request_chars: 32,
            ..PrefixConfig::default()
        };
        let body = json!({"prompt": "é".repeat(100_000)});
        let input = routing_text("completions", Some("m"), Some(&body), &config);

        assert_eq!(input.char_count(), 32);
        assert_eq!(input.text.chars().count(), 32);
        assert!(input.text.len() <= 64);
    }

    #[test]
    fn routing_text_is_canonical_across_json_key_order() {
        let config = PrefixConfig::default();
        let first: Value = serde_json::from_str(
            r#"{"model":"model","messages":[{"role":"user","content":{"b":2,"a":1}}]}"#,
        )
        .unwrap();
        let second: Value = serde_json::from_str(
            r#"{"messages":[{"content":{"a":1,"b":2},"role":"user"}],"model":"model"}"#,
        )
        .unwrap();
        let first = routing_text("chat/completions", Some("model"), Some(&first), &config);
        let second = routing_text("chat/completions", Some("model"), Some(&second), &config);
        assert_eq!(first.text, second.text);
    }
}
