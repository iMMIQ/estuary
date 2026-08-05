use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use serde_json::Value;

use crate::config::PrefixConfig;

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
}

impl RadixNode {
    fn with_text(text: String, tenant: &str, epoch: u64) -> Self {
        let char_count = text.chars().count();
        Self {
            text,
            char_count,
            children: HashMap::new(),
            tenant_last_access: HashMap::from([(tenant.to_owned(), epoch)]),
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
    node.children.retain(|_, child| {
        clear_tenant_from_node(child, tenant);
        !child.children.is_empty() || !child.tenant_last_access.is_empty()
    });
}

fn insert_at(node: &mut RadixNode, remaining: &str, tenant: &str, epoch: u64, count: &mut usize) {
    if remaining.is_empty() {
        node.tenant_last_access.insert(tenant.to_owned(), epoch);
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

    let mut branch = RadixNode {
        text: take_chars(remaining, shared).to_owned(),
        char_count: shared,
        children: HashMap::from([(child_key, child)]),
        tenant_last_access: inherited_tenants,
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
}

fn oldest_leaf(node: &RadixNode, tenant: &str, path: &mut Vec<char>) -> Option<(u64, Vec<char>)> {
    let mut best: Option<(u64, Vec<char>)> = None;
    let mut has_tenant_child = false;

    for (key, child) in &node.children {
        if !child.tenant_last_access.contains_key(tenant) {
            continue;
        }
        has_tenant_child = true;
        path.push(*key);
        if let Some(candidate) = oldest_leaf(child, tenant, path) {
            if best.as_ref().is_none_or(|(epoch, _)| candidate.0 < *epoch) {
                best = Some(candidate);
            }
        }
        path.pop();
    }

    if !has_tenant_child && !path.is_empty() {
        return node
            .tenant_last_access
            .get(tenant)
            .map(|epoch| (*epoch, path.clone()));
    }
    best
}

fn remove_tenant_at_path(node: &mut RadixNode, tenant: &str, path: &[char], depth: usize) -> usize {
    if depth == path.len() {
        if node.tenant_last_access.remove(tenant).is_some() {
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
    removed
}

#[derive(Debug)]
pub struct PrefixDirectory {
    enabled: bool,
    max_tree_chars_per_node: usize,
    trees: RwLock<HashMap<String, Arc<RwLock<RadixTree>>>>,
}

impl PrefixDirectory {
    pub fn new(config: &PrefixConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_tree_chars_per_node: config.max_tree_chars_per_node,
            trees: RwLock::new(HashMap::new()),
        }
    }

    pub fn record(&self, node_id: &str, input: &PrefixInput) {
        if !self.enabled || input.text.is_empty() {
            return;
        }
        let tree = self
            .trees
            .write()
            .entry(input.tree_key.clone())
            .or_insert_with(|| Arc::new(RwLock::new(RadixTree::default())))
            .clone();
        tree.write()
            .insert(&input.text, node_id, self.max_tree_chars_per_node);
    }

    pub fn best_match(&self, input: &PrefixInput) -> PrefixMatch {
        if !self.enabled || input.text.is_empty() {
            return PrefixMatch::default();
        }
        let tree = self.trees.read().get(&input.tree_key).cloned();
        tree.map_or_else(PrefixMatch::default, |tree| {
            tree.read().prefix_match(&input.text, input.char_count())
        })
    }

    pub fn clear_node(&self, node_id: &str) {
        for tree in self.trees.read().values() {
            tree.write().clear_tenant(node_id);
        }
    }
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
    let mut text = String::new();
    let Some(body) = body else {
        return PrefixInput {
            tree_key,
            text,
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
            append_segment(&mut text, key, value);
        }
    }

    let input_key = match endpoint {
        "chat/completions" => "messages",
        "completions" => "prompt",
        _ => "input",
    };
    if let Some(input) = body.get(input_key) {
        if let Some(items) = input.as_array() {
            for item in items {
                append_segment(&mut text, input_key, item);
            }
        } else {
            append_segment(&mut text, input_key, input);
        }
    } else {
        append_segment(&mut text, "body", body);
    }

    let char_count = text.chars().count();
    if char_count > config.max_request_chars {
        text = take_chars(&text, config.max_request_chars).to_owned();
    }
    PrefixInput {
        tree_key,
        char_count: char_count.min(config.max_request_chars),
        text,
        token_ids: None,
    }
}

fn append_segment(output: &mut String, name: &str, value: &Value) {
    output.push('\u{1e}');
    output.push_str(name);
    output.push('\u{1f}');
    if let Ok(encoded) = serde_json::to_string(value) {
        output.push_str(&encoded);
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
        let tree = trees.values().next().expect("one model tree").read();
        assert!(
            tree.tenant_char_count
                .get("node-a")
                .copied()
                .unwrap_or_default()
                <= 24
        );
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
