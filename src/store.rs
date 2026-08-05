use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;

use crate::config::{NodeConfig, validate_node_config};

const SCHEMA_VERSION: i64 = 1;

#[derive(Clone, Debug, Serialize)]
pub struct StoredNode {
    pub config: NodeConfig,
    pub revision: u64,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Debug)]
pub struct NodeStore {
    connection: Mutex<Connection>,
}

impl NodeStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create database directory {}", parent.display())
            })?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("failed to open SQLite database {}", path.display()))?;
        Self::initialize(connection)
    }

    pub fn memory() -> Result<Arc<Self>> {
        Self::initialize(Connection::open_in_memory()?)
    }

    fn initialize(mut connection: Connection) -> Result<Arc<Self>> {
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS node_configs (
                id TEXT PRIMARY KEY NOT NULL,
                config_json TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK (revision > 0),
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS control_state (
                singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
                revision INTEGER NOT NULL CHECK (revision > 0)
            );
            INSERT OR IGNORE INTO control_state (singleton, revision) VALUES (1, 1);
            CREATE TRIGGER IF NOT EXISTS node_configs_control_insert
            AFTER INSERT ON node_configs BEGIN
                UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
            END;
            CREATE TRIGGER IF NOT EXISTS node_configs_control_update
            AFTER UPDATE ON node_configs BEGIN
                UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
            END;
            CREATE TRIGGER IF NOT EXISTS node_configs_control_delete
            AFTER DELETE ON node_configs BEGIN
                UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
            END;",
        )?;
        let current: i64 =
            transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if current > SCHEMA_VERSION {
            bail!(
                "database schema version {current} is newer than supported version {SCHEMA_VERSION}"
            );
        }
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.commit()?;
        Ok(Arc::new(Self {
            connection: Mutex::new(connection),
        }))
    }

    pub fn list(&self) -> Result<Vec<StoredNode>> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT config_json, revision, created_at_unix_ms, updated_at_unix_ms
             FROM node_configs ORDER BY id",
        )?;
        let rows = statement.query_map([], decode_node_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to load node configuration")?
            .into_iter()
            .map(|node| {
                validate_node_config(&node.config)?;
                Ok(node)
            })
            .collect()
    }

    pub fn revision(&self) -> Result<u64> {
        let connection = self.connection.lock();
        connection
            .query_row(
                "SELECT revision FROM control_state WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .context("failed to load control-plane revision")
    }

    pub fn get(&self, id: &str) -> Result<Option<StoredNode>> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT config_json, revision, created_at_unix_ms, updated_at_unix_ms
             FROM node_configs WHERE id = ?1",
        )?;
        statement
            .query_row([id], decode_node_row)
            .optional()
            .context("failed to load node configuration")
    }

    pub fn insert(&self, config: &NodeConfig) -> Result<StoredNode> {
        validate_node_config(config)?;
        let encoded = serde_json::to_string(config)?;
        let now = unix_millis();
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction
            .execute(
                "INSERT INTO node_configs (
                    id, config_json, revision, created_at_unix_ms, updated_at_unix_ms
                 ) VALUES (?1, ?2, 1, ?3, ?3)",
                params![config.id, encoded, now],
            )
            .with_context(|| format!("failed to insert node {:?}", config.id))?;
        transaction.commit()?;
        Ok(StoredNode {
            config: config.clone(),
            revision: 1,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        })
    }

    pub fn update(
        &self,
        id: &str,
        expected_revision: u64,
        config: &NodeConfig,
    ) -> Result<Option<StoredNode>> {
        validate_node_config(config)?;
        if config.id != id {
            bail!("node id cannot be changed during update");
        }
        let encoded = serde_json::to_string(config)?;
        let now = unix_millis();
        let revision = expected_revision.saturating_add(1);
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE node_configs
             SET config_json = ?1, revision = ?2, updated_at_unix_ms = ?3
             WHERE id = ?4 AND revision = ?5",
            params![encoded, revision, now, id, expected_revision],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        let created_at_unix_ms = transaction.query_row(
            "SELECT created_at_unix_ms FROM node_configs WHERE id = ?1",
            [id],
            |row| row.get(0),
        )?;
        transaction.commit()?;
        Ok(Some(StoredNode {
            config: config.clone(),
            revision,
            created_at_unix_ms,
            updated_at_unix_ms: now,
        }))
    }

    pub fn delete(&self, id: &str, expected_revision: Option<u64>) -> Result<bool> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = if let Some(revision) = expected_revision {
            transaction.execute(
                "DELETE FROM node_configs WHERE id = ?1 AND revision = ?2",
                params![id, revision],
            )?
        } else {
            transaction.execute("DELETE FROM node_configs WHERE id = ?1", [id])?
        };
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn seed_if_empty(&self, nodes: &[NodeConfig]) -> Result<()> {
        if nodes.is_empty() || !self.list()?.is_empty() {
            return Ok(());
        }
        for node in nodes {
            self.insert(node)?;
        }
        Ok(())
    }
}

fn decode_node_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredNode> {
    let encoded: String = row.get(0)?;
    let config = serde_json::from_str(&encoded).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            encoded.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    Ok(StoredNode {
        config,
        revision: row.get(1)?,
        created_at_unix_ms: row.get(2)?,
        updated_at_unix_ms: row.get(3)?,
    })
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn node() -> NodeConfig {
        NodeConfig {
            id: "node-a".to_owned(),
            base_url: "http://127.0.0.1:8000/v1".to_owned(),
            models: HashMap::from([("public".to_owned(), "upstream".to_owned())]),
            ..NodeConfig::default()
        }
    }

    #[test]
    fn persists_nodes_with_optimistic_revisions() {
        let store = NodeStore::memory().unwrap();
        let mut initial = node();
        initial.api_key = Some("stored-secret".to_owned());
        let inserted = store.insert(&initial).unwrap();
        assert_eq!(inserted.revision, 1);
        assert_eq!(
            store
                .get("node-a")
                .unwrap()
                .unwrap()
                .config
                .api_key
                .as_deref(),
            Some("stored-secret")
        );

        let mut changed = node();
        changed.weight = 2.0;
        let updated = store.update("node-a", 1, &changed).unwrap().unwrap();
        assert_eq!(updated.revision, 2);
        assert!((store.get("node-a").unwrap().unwrap().config.weight - 2.0).abs() < f64::EPSILON);
        assert!(store.update("node-a", 1, &changed).unwrap().is_none());
        assert!(store.delete("node-a", Some(2)).unwrap());
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    fn survives_database_reopen() {
        let path = std::env::temp_dir().join(format!("estuary-store-{}.db", uuid::Uuid::now_v7()));
        {
            let store = NodeStore::open(&path).unwrap();
            store.insert(&node()).unwrap();
        }
        {
            let store = NodeStore::open(&path).unwrap();
            assert_eq!(store.list().unwrap()[0].config.id, "node-a");
        }
        for candidate in [
            path.clone(),
            path.with_extension("db-wal"),
            path.with_extension("db-shm"),
        ] {
            let _ = std::fs::remove_file(candidate);
        }
    }

    #[test]
    fn revision_triggers_are_visible_to_another_process_connection() {
        let path = std::env::temp_dir().join(format!("estuary-store-{}.db", uuid::Uuid::now_v7()));
        let first = NodeStore::open(&path).unwrap();
        let second = NodeStore::open(&path).unwrap();
        let initial = second.revision().unwrap();

        first.insert(&node()).unwrap();
        assert!(second.revision().unwrap() > initial);
        let after_insert = second.revision().unwrap();
        first.delete("node-a", Some(1)).unwrap();
        assert!(second.revision().unwrap() > after_insert);

        drop(first);
        drop(second);
        for candidate in [
            path.clone(),
            path.with_extension("db-wal"),
            path.with_extension("db-shm"),
        ] {
            let _ = std::fs::remove_file(candidate);
        }
    }
}
