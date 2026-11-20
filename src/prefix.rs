use std::{
    collections::HashMap,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
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
    token_node_ids: Option<Arc<[String]>>,
    estimated_tokens: usize,
    estimated_affinity_tokens: usize,
    output_limit: Option<usize>,
    generations: usize,
}

impl PrefixInput {
    pub(crate) fn generations(&self) -> usize {
        self.generations.max(1)
    }
    /// Input work, using exact tokenization when available and a text estimate otherwise.
    pub fn input_tokens(&self) -> usize {
        self.token_ids
            .as_ref()
            .map_or(self.estimated_tokens, |ids| ids.len())
    }

    pub(crate) fn affinity_tokens(&self) -> usize {
        self.estimated_affinity_tokens.min(self.input_tokens())
    }

    pub(crate) fn output_tokens(&self, typical_output: usize) -> usize {
        typical_output
            .min(self.output_limit.unwrap_or(usize::MAX))
            .saturating_mul(self.generations)
    }

    fn with_workload(mut self, endpoint: &str, body: Option<&Value>) -> Self {
        let Some(body) = body else {
            return self;
        };
        // Count the full prompt, even when the bounded affinity key is truncated or disabled.
        let input_key = match endpoint {
            "chat/completions" | "messages" => "messages",
            "completions" => "prompt",
            _ => "input",
        };
        for key in [
            "system",
            "instructions",
            "tools",
            "response_format",
            input_key,
        ] {
            if let Some(value) = body.get(key) {
                self.estimated_tokens =
                    self.estimated_tokens.saturating_add(estimate_tokens(value));
            }
        }
        self.estimated_affinity_tokens =
            estimate_text_tokens(&self.text).min(self.estimated_tokens);
        if matches!(
            endpoint,
            "chat/completions" | "completions" | "messages" | "responses"
        ) {
            self.generations = body
                .get("n")
                .and_then(Value::as_u64)
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(1)
                .max(1);
            if endpoint == "completions"
                && let Some(prompts) = body.get("prompt").and_then(Value::as_array)
                && prompts
                    .first()
                    .is_some_and(|prompt| prompt.is_string() || prompt.is_array())
            {
                self.generations = self.generations.saturating_mul(prompts.len());
            }
            self.output_limit = ["max_completion_tokens", "max_output_tokens", "max_tokens"]
                .into_iter()
                .find_map(|key| body.get(key).and_then(Value::as_u64))
                .and_then(|n| usize::try_from(n).ok());
        }
        self
    }
    pub fn char_count(&self) -> usize {
        self.char_count
    }

    pub fn set_token_ids(&mut self, token_ids: Vec<u64>) {
        self.token_ids = Some(token_ids.into());
        self.token_node_ids = None;
    }

    pub(crate) fn set_scoped_token_ids(
        &mut self,
        token_ids: Vec<u64>,
        node_ids: Option<Vec<String>>,
    ) {
        self.set_token_ids(token_ids);
        self.token_node_ids = node_ids.map(Into::into);
    }

    pub(crate) fn tokens_apply_to_node(&self, node_id: &str) -> bool {
        self.token_node_ids
            .as_ref()
            .is_none_or(|nodes| nodes.iter().any(|id| id == node_id))
    }

    pub fn token_ids(&self) -> Option<&[u64]> {
        self.token_ids.as_deref()
    }
}

pub(crate) fn is_media_object(items: &serde_json::Map<String, Value>) -> bool {
    matches!(
        items.get("type").and_then(Value::as_str),
        Some(
            "image"
                | "image_url"
                | "input_image"
                | "audio"
                | "input_audio"
                | "audio_url"
                | "video"
                | "video_url"
                | "input_video"
                | "file"
                | "input_file"
        )
    )
}

pub(crate) fn has_media(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().any(has_media),
        Value::Object(object) => is_media_object(object) || object.values().any(has_media),
        _ => false,
    }
}

fn estimate_tokens(value: &Value) -> usize {
    match value {
        Value::String(text) => estimate_text_tokens(text),
        Value::Array(items) => items.iter().fold(0usize, |sum, item| {
            sum.saturating_add(estimate_tokens(item))
        }),
        Value::Object(items) => {
            // Encoded media length is not a text-token or GPU-work estimate.
            if is_media_object(items) {
                return 0;
            }
            items.values().fold(0usize, |sum, item| {
                sum.saturating_add(estimate_tokens(item))
            })
        }
        Value::Number(_) => 1,
        _ => 0,
    }
}

fn estimate_text_tokens(text: &str) -> usize {
    if text.is_ascii() {
        return text.len().div_ceil(4);
    }
    let (ascii, non_ascii) = text
        .chars()
        .fold((0usize, 0usize), |(ascii, non_ascii), ch| {
            if ch.is_ascii() {
                (ascii + 1, non_ascii)
            } else {
                (ascii, non_ascii + 1)
            }
        });
    ascii.div_ceil(4).saturating_add(non_ascii)
}

#[derive(Clone, Copy, Debug)]
pub struct NodeMatch {
    pub chars: usize,
    pub age_seconds: f64,
}

#[derive(Debug, Default)]
pub struct PrefixMatch {
    pub matched_chars: usize,
    pub input_chars: usize,
    pub node_matches: HashMap<String, NodeMatch>,
}

impl PrefixMatch {
    pub fn best_nodes(&self) -> Vec<String> {
        if self.matched_chars == 0 {
            return Vec::new();
        }
        let mut nodes = self
            .node_matches
            .iter()
            .filter(|&(_, matched)| matched.chars == self.matched_chars)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes
    }
}

#[derive(Clone, Copy, Debug)]
struct TenantState {
    last_access: u64,
    oldest_leaf: u64,
    last_seen: Instant,
}

#[derive(Debug, Default)]
struct RadixNode {
    text: String,
    char_count: usize,
    children: HashMap<char, RadixNode>,
    tenants: HashMap<String, TenantState>,
}

impl RadixNode {
    fn with_text(text: String, tenant: &str, epoch: u64) -> Self {
        let char_count = text.chars().count();
        Self {
            text,
            char_count,
            children: HashMap::new(),
            tenants: HashMap::from([(
                tenant.to_owned(),
                TenantState {
                    last_access: epoch,
                    oldest_leaf: epoch,
                    last_seen: Instant::now(),
                },
            )]),
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
        let now = Instant::now();
        self.root
            .tenants
            .entry(tenant.to_owned())
            .and_modify(|state| {
                state.last_access = epoch;
                state.last_seen = now;
            })
            .or_insert(TenantState {
                last_access: epoch,
                oldest_leaf: epoch,
                last_seen: now,
            });
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
        let mut node_matches = HashMap::new();
        let now = Instant::now();

        while let Some(first) = remaining.chars().next() {
            let Some(child) = current.children.get(&first) else {
                break;
            };
            let full_edge = remaining.starts_with(&child.text);
            let shared = if full_edge {
                child.char_count
            } else {
                shared_prefix_chars(remaining, &child.text)
            };
            matched_chars += shared;
            for (tenant, state) in &child.tenants {
                let entry = NodeMatch {
                    chars: matched_chars,
                    age_seconds: now.saturating_duration_since(state.last_seen).as_secs_f64(),
                };
                if let Some(existing) = node_matches.get_mut(tenant) {
                    *existing = entry;
                } else {
                    node_matches.insert(tenant.clone(), entry);
                }
            }
            if !full_edge {
                break;
            }
            remaining = &remaining[child.text.len()..];
            current = child;
        }

        PrefixMatch {
            matched_chars,
            input_chars,
            node_matches,
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
            self.root.tenants.remove(tenant);
        }
    }

    fn clear_tenant(&mut self, tenant: &str) {
        clear_tenant_from_node(&mut self.root, tenant);
        self.root.tenants.remove(tenant);
        self.tenant_char_count.remove(tenant);
    }
}

fn clear_tenant_from_node(node: &mut RadixNode, tenant: &str) {
    node.tenants.remove(tenant);
    node.children.retain(|_, child| {
        clear_tenant_from_node(child, tenant);
        !child.children.is_empty() || !child.tenants.is_empty()
    });
}

fn touch_branch_tenant(node: &mut RadixNode, tenant: &str, count: &mut usize, added_chars: usize) {
    let now = Instant::now();
    if let Some(state) = node.tenants.get_mut(tenant) {
        state.last_seen = now;
    } else {
        *count = count.saturating_add(added_chars);
        node.tenants.insert(
            tenant.to_owned(),
            TenantState {
                last_access: 0,
                oldest_leaf: 0,
                last_seen: now,
            },
        );
    }
}

fn insert_at(node: &mut RadixNode, remaining: &str, tenant: &str, epoch: u64, count: &mut usize) {
    if remaining.is_empty() {
        let now = Instant::now();
        node.tenants
            .entry(tenant.to_owned())
            .and_modify(|state| {
                state.last_access = epoch;
                state.last_seen = now;
            })
            .or_insert(TenantState {
                last_access: epoch,
                oldest_leaf: epoch,
                last_seen: now,
            });
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

    if let Some(suffix) = remaining.strip_prefix(&child.text) {
        let chars = child.char_count;
        touch_branch_tenant(&mut child, tenant, count, chars);
        insert_at(&mut child, suffix, tenant, epoch, count);
        node.children.insert(first, child);
        refresh_oldest_leaf(node, tenant);
        return;
    }

    let shared = shared_prefix_chars(remaining, &child.text);
    let split_byte = byte_index_at_char(&child.text, shared);
    let suffix = child.text.split_off(split_byte);
    child.text = suffix;
    child.char_count -= shared;
    let child_key = child
        .text
        .chars()
        .next()
        .expect("radix suffix is not empty");
    let inherited_tenants = child.tenants.clone();

    let mut branch = RadixNode {
        text: take_chars(remaining, shared).to_owned(),
        char_count: shared,
        children: HashMap::from([(child_key, child)]),
        tenants: inherited_tenants,
    };
    touch_branch_tenant(&mut branch, tenant, count, shared);
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
    let epoch = node.tenants.get(tenant)?.oldest_leaf;
    let mut current = node;
    while let Some((key, child)) = current
        .children
        .iter()
        .find(|(_, child)| child.tenants.get(tenant).map(|s| s.oldest_leaf) == Some(epoch))
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
        .filter_map(|child| child.tenants.get(tenant).map(|s| s.oldest_leaf))
        .min()
        .or_else(|| node.tenants.get(tenant).map(|s| s.last_access));
    if let Some(epoch) = oldest {
        if let Some(state) = node.tenants.get_mut(tenant) {
            state.oldest_leaf = epoch;
        }
    } else {
        node.tenants.remove(tenant);
    }
}

fn remove_tenant_at_path(node: &mut RadixNode, tenant: &str, path: &[char], depth: usize) -> usize {
    if depth == path.len() {
        return if node.tenants.remove(tenant).is_some() {
            node.char_count
        } else {
            0
        };
    }

    let key = path[depth];
    let Some(child) = node.children.get_mut(&key) else {
        return 0;
    };
    let removed = remove_tenant_at_path(child, tenant, path, depth + 1);
    if child.children.is_empty() && child.tenants.is_empty() {
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
    epoch: AtomicU64,
    total_chars: AtomicUsize,
    trees: RwLock<HashMap<String, Arc<TreeEntry>>>,
}

#[derive(Debug)]
struct TreeEntry {
    tree: RwLock<RadixTree>,
    last_recorded: AtomicU64,
    accounted_chars: AtomicUsize,
}

impl PrefixDirectory {
    pub fn new(config: &PrefixConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_tree_chars_per_node: config.max_tree_chars_per_node,
            max_trees: config.max_trees,
            max_directory_chars: config.max_directory_chars,
            epoch: AtomicU64::new(0),
            total_chars: AtomicUsize::new(0),
            trees: RwLock::new(HashMap::new()),
        }
    }

    pub fn record(&self, node_id: &str, input: &PrefixInput) {
        self.record_if(node_id, input, || true);
    }

    pub(crate) fn record_for_node(&self, node: &crate::node::Node, input: &PrefixInput) {
        // Check retirement under the same per-tree lock as clear_node: an old response
        // must not repopulate affinity after its node configuration was replaced.
        self.record_if(node.id(), input, || !node.is_retired());
    }

    fn record_if(&self, node_id: &str, input: &PrefixInput, current: impl FnOnce() -> bool) {
        if !self.enabled || input.text.is_empty() || input.tree_key.len() > MAX_TREE_KEY_BYTES {
            return;
        }
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        // Fast path: look up an existing tree under a shared directory read lock
        // so inserts for different models/endpoints do not contend globally.
        let entry = if let Some(existing) = self.trees.read().get(&input.tree_key).cloned() {
            existing
        } else {
            let mut trees = self.trees.write();
            if let Some(existing) = trees.get(&input.tree_key).cloned() {
                existing
            } else {
                if trees.len() >= self.max_trees {
                    evict_oldest_tree(&mut trees, &self.total_chars);
                }
                let created = Arc::new(TreeEntry {
                    tree: RwLock::new(RadixTree::default()),
                    last_recorded: AtomicU64::new(epoch),
                    accounted_chars: AtomicUsize::new(0),
                });
                trees.insert(input.tree_key.clone(), Arc::clone(&created));
                created
            }
        };

        let mut tree = entry.tree.write();
        if !current() {
            return;
        }
        entry.last_recorded.store(epoch, Ordering::Relaxed);
        let previous_tenant_chars = tree
            .tenant_char_count
            .get(node_id)
            .copied()
            .unwrap_or_default();
        tree.insert(&input.text, node_id, self.max_tree_chars_per_node);
        let tenant_chars = tree
            .tenant_char_count
            .get(node_id)
            .copied()
            .unwrap_or_default();
        drop(tree);

        if tenant_chars >= previous_tenant_chars {
            let delta = tenant_chars - previous_tenant_chars;
            entry.accounted_chars.fetch_add(delta, Ordering::Relaxed);
            self.total_chars.fetch_add(delta, Ordering::Relaxed);
        } else {
            let delta = previous_tenant_chars - tenant_chars;
            saturating_fetch_sub(&entry.accounted_chars, delta);
            saturating_fetch_sub(&self.total_chars, delta);
        }

        if self.total_chars.load(Ordering::Relaxed) > self.max_directory_chars {
            let mut trees = self.trees.write();
            while self.total_chars.load(Ordering::Relaxed) > self.max_directory_chars {
                if !evict_oldest_tree(&mut trees, &self.total_chars) {
                    break;
                }
            }
        }
    }

    pub fn best_match(&self, input: &PrefixInput) -> PrefixMatch {
        if !self.enabled || input.text.is_empty() || input.tree_key.len() > MAX_TREE_KEY_BYTES {
            return PrefixMatch::default();
        }
        let entry = self.trees.read().get(&input.tree_key).cloned();
        entry.map_or_else(PrefixMatch::default, |entry| {
            entry
                .tree
                .read()
                .prefix_match(&input.text, input.char_count())
        })
    }

    pub fn clear_node(&self, node_id: &str) {
        let mut trees = self.trees.write();
        let mut total_chars = 0usize;
        trees.retain(|_, entry| {
            let mut tree = entry.tree.write();
            tree.clear_tenant(node_id);
            let accounted = tree
                .tenant_char_count
                .values()
                .fold(0_usize, |total, chars| total.saturating_add(*chars));
            entry.accounted_chars.store(accounted, Ordering::Relaxed);
            total_chars = total_chars.saturating_add(accounted);
            accounted > 0
        });
        self.total_chars.store(total_chars, Ordering::Relaxed);
    }
}

fn saturating_fetch_sub(counter: &AtomicUsize, delta: usize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(delta))
    });
}

fn evict_oldest_tree(
    trees: &mut HashMap<String, Arc<TreeEntry>>,
    total_chars: &AtomicUsize,
) -> bool {
    let Some(oldest) = trees
        .iter()
        .min_by_key(|(_, entry)| entry.last_recorded.load(Ordering::Relaxed))
        .map(|(key, _)| key.clone())
    else {
        return false;
    };
    if let Some(removed) = trees.remove(&oldest) {
        let removed_chars = removed.accounted_chars.load(Ordering::Relaxed);
        saturating_fetch_sub(total_chars, removed_chars);
    }
    true
}

pub fn routing_text(
    endpoint: &str,
    model: Option<&str>,
    body: Option<&Value>,
    config: &PrefixConfig,
) -> PrefixInput {
    if !config.enabled {
        return PrefixInput::default().with_workload(endpoint, body);
    }

    let tree_key = format!("{endpoint}\u{1f}{}", model.unwrap_or("<unspecified>"));
    let mut canonical = BoundedCanonical::new(config.max_request_chars);
    let Some(body) = body else {
        return PrefixInput {
            tree_key,
            text: String::new(),
            char_count: 0,
            token_ids: None,
            ..PrefixInput::default()
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
        "reasoning_effort",
        "chat_template",
        "chat_template_kwargs",
        "add_generation_prompt",
        "continue_final_message",
        "add_special_tokens",
        "documents",
        "cache_salt",
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
        ..PrefixInput::default()
    }
    .with_workload(endpoint, Some(body))
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
        let _ = serde_json::to_writer(self, &CanonicalValue(value));
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

// Routing keys must remain independent of the client's JSON object insertion
// order, including when dependencies enable serde_json/preserve_order.
struct CanonicalValue<'a>(&'a Value);

impl serde::Serialize for CanonicalValue<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self.0 {
            Value::Object(object) => {
                let mut entries = object.iter().collect::<Vec<_>>();
                entries.sort_unstable_by_key(|(left, _)| *left);
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, &CanonicalValue(value))?;
                }
                map.end()
            }
            Value::Array(values) => serializer.collect_seq(values.iter().map(CanonicalValue)),
            value => value.serialize(serializer),
        }
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
    fn workload_estimation_uses_the_full_input_even_without_affinity() {
        let body = json!({"prompt": "x".repeat(16_384), "max_tokens": 32, "n": 2});
        for config in [
            PrefixConfig {
                enabled: false,
                ..PrefixConfig::default()
            },
            PrefixConfig {
                max_request_chars: 32,
                ..PrefixConfig::default()
            },
        ] {
            let input = routing_text("completions", Some("model"), Some(&body), &config);
            assert_eq!(input.input_tokens(), 4_096);
            assert_eq!(input.output_tokens(256), 64);
        }
        let body = json!({"prompt": [1, 2, 3, 4], "max_tokens": 100});
        let mut input = routing_text(
            "completions",
            Some("model"),
            Some(&body),
            &PrefixConfig::default(),
        );
        assert_eq!(input.input_tokens(), 4);
        input.set_token_ids(vec![1, 2]);
        assert_eq!(input.input_tokens(), 2);
        let input = routing_text(
            "embeddings",
            Some("model"),
            Some(&json!({"input":"text"})),
            &PrefixConfig::default(),
        );
        assert_eq!(input.output_tokens(256), 0);
        let batch = routing_text(
            "completions",
            Some("model"),
            Some(&json!({"prompt":["first","second"],"n":2,"max_tokens":32})),
            &PrefixConfig::default(),
        );
        assert_eq!(batch.output_tokens(256), 128);
        let media = json!({"messages":[{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image_url","image_url":{"url":"x".repeat(100_000)}}]}]});
        let input = routing_text(
            "chat/completions",
            Some("model"),
            Some(&media),
            &PrefixConfig::default(),
        );
        assert!(input.input_tokens() < 10);
    }

    #[test]
    fn per_worker_prefix_matches_keep_shorter_owners_and_age_separately() {
        fn age(node: &mut RadixNode, timestamp: Instant) {
            if let Some(state) = node.tenants.get_mut("short") {
                state.last_seen = timestamp;
            }
            for child in node.children.values_mut() {
                age(child, timestamp);
            }
        }
        let mut tree = RadixTree::default();
        tree.insert("shared short", "short", 1_000);
        tree.insert("shared short and long", "long", 1_000);
        let old = Instant::now()
            .checked_sub(std::time::Duration::from_secs(60))
            .unwrap();
        age(&mut tree.root, old);
        let matched = tree.prefix_match("shared short and long", 21);
        assert_eq!(matched.node_matches["short"].chars, 12);
        assert_eq!(matched.node_matches["long"].chars, 21);
        assert!(matched.node_matches["short"].age_seconds >= 60.0);
        assert!(matched.node_matches["long"].age_seconds < 1.0);
    }

    #[test]
    fn radix_matches_keep_unicode_character_counts_across_complete_and_partial_edges() {
        let mut tree = RadixTree::default();
        let recorded = [
            ("short", "你好🙂café"),
            ("long", "你好🙂café 后续"),
            ("other", "你好🙃different"),
        ];
        for (tenant, text) in recorded {
            tree.insert(text, tenant, 1_000);
        }
        // Repeated insertion takes the same complete-edge path as a cache hit.
        tree.insert(recorded[0].1, recorded[0].0, 1_000);
        for text in [
            "你好🙂café",
            "你好🙂café 后续",
            "你好🙂café 后续更多",
            "你好🙂cafè",
            "你好🙃different",
            "你好🙄",
            "你",
        ] {
            let matched = tree.prefix_match(text, text.chars().count());
            for (tenant, cached) in recorded {
                let expected = shared_prefix_chars(text, cached);
                assert_eq!(
                    matched
                        .node_matches
                        .get(tenant)
                        .map_or(0, |item| item.chars),
                    expected,
                    "{tenant}: {text}"
                );
            }
            assert_eq!(
                matched.matched_chars,
                recorded
                    .iter()
                    .map(|(_, cached)| shared_prefix_chars(text, cached))
                    .max()
                    .unwrap()
            );
        }
    }

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
        assert_eq!(result.best_nodes(), ["node-a"]);
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
        assert_eq!(matched.best_nodes(), ["node-a"]);
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
        assert_eq!(result.best_nodes(), ["node-a"]);
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
        assert_eq!(
            directory.best_match(&other_model).best_nodes(),
            Vec::<String>::new()
        );
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
        assert_eq!(directory.trees.read().len(), 2);
        let evicted = routing_text(
            "completions",
            Some("a"),
            Some(&json!({"prompt": "a"})),
            &config,
        );
        assert_eq!(
            directory.best_match(&evicted).best_nodes(),
            Vec::<String>::new()
        );

        directory.clear_node("node-a");
        assert!(directory.trees.read().is_empty());
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
                .values()
                .map(|entry| entry.accounted_chars.load(Ordering::Relaxed))
                .sum::<usize>()
                <= config.max_directory_chars
        );
        drop(trees);

        let before = directory.trees.read().len();
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
        assert_eq!(directory.trees.read().len(), before);
    }

    #[test]
    fn directory_character_accounting_tracks_updates_eviction_and_node_removal() {
        let config = PrefixConfig {
            max_trees: 3,
            max_tree_chars_per_node: 32,
            max_directory_chars: 140,
            ..PrefixConfig::default()
        };
        let directory = PrefixDirectory::new(&config);
        for index in 0..150 {
            let node_id = format!("node-{}", index % 4);
            let input = PrefixInput {
                tree_key: format!("model-{}", (index / 7) % 5),
                text: format!("共享前缀-{}-{}", index % 6, "x".repeat(index % 20)),
                ..PrefixInput::default()
            };
            directory.record(&node_id, &input);
            // Updating a complete prefix must not increase the accounted size.
            let before = directory.total_chars.load(Ordering::Relaxed);
            directory.record(&node_id, &input);
            assert_eq!(directory.total_chars.load(Ordering::Relaxed), before);
            if index % 11 == 0 {
                directory.clear_node(&format!("node-{}", (index + 1) % 4));
            }
            let trees = directory.trees.read();
            let mut expected_total = 0;
            for entry in trees.values() {
                let tree = entry.tree.read();
                let expected = tree.tenant_char_count.values().sum::<usize>();
                assert_eq!(entry.accounted_chars.load(Ordering::Relaxed), expected);
                assert!(tree.tenant_char_count.values().all(|chars| *chars <= 32));
                expected_total += expected;
            }
            let total_chars = directory.total_chars.load(Ordering::Relaxed);
            assert_eq!(total_chars, expected_total);
            assert!(total_chars <= config.max_directory_chars);
            assert!(trees.len() <= config.max_trees);
        }
        for index in 0..4 {
            directory.clear_node(&format!("node-{index}"));
        }
        assert_eq!(directory.total_chars.load(Ordering::Relaxed), 0);
        assert!(directory.trees.read().is_empty());
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
        let tree = trees.values().next().expect("one model tree").tree.read();
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

        assert_eq!(tree.prefix_match("aaaa", 4).best_nodes(), ["node-a"]);
        assert_eq!(
            tree.prefix_match("bbbb", 4).best_nodes(),
            Vec::<String>::new()
        );
        assert_eq!(tree.prefix_match("cccc", 4).best_nodes(), ["node-a"]);
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

#[cfg(test)]
mod routing_tests {
    use crate::chat_bridge::prepare;
    use serde_json::json;
    #[test]
    fn equivalent_clients_share_the_final_chat_prefix_and_thinking_changes_it() {
        let messages =
            json!({"model":"m","max_tokens":64,"messages":[{"role":"user","content":"hello"}]});
        let responses = json!({"model":"m","input":"hello"});
        let chat = json!({"model":"m","messages":[{"role":"user","content":"hello"}]});
        let config = crate::config::PrefixConfig::default();
        let mut prefixes = Vec::new();
        for (endpoint, source) in [
            ("messages", messages.clone()),
            ("responses", responses),
            ("chat/completions", chat),
        ] {
            let (body, _) = prepare(endpoint, source).unwrap();
            prefixes.push(crate::prefix::routing_text(
                "chat/completions",
                Some("m"),
                Some(&body),
                &config,
            ));
        }
        assert_eq!(prefixes[0].text, prefixes[1].text);
        assert_eq!(prefixes[1].text, prefixes[2].text);
        let mut thinking = messages;
        thinking["thinking"] = json!({"type":"enabled","budget_tokens":32});
        let (body, _) = prepare("messages", thinking).unwrap();
        assert_ne!(
            prefixes[0].text,
            crate::prefix::routing_text("chat/completions", Some("m"), Some(&body), &config).text
        );
    }
}
