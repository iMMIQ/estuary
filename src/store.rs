use std::{net::IpAddr, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use rusqlite_migration::{M, Migrations};
use serde::Serialize;

use crate::{
    config::{NodeConfig, validate_node_config},
    crypto,
    time::unix_millis,
};

const MIGRATION_LIST: &[M<'static>] = &[
    M::up(
        "CREATE TABLE node_configs (
            id TEXT PRIMARY KEY NOT NULL,
            config_json TEXT NOT NULL,
            revision INTEGER NOT NULL CHECK (revision > 0),
            created_at_unix_ms INTEGER NOT NULL,
            updated_at_unix_ms INTEGER NOT NULL
        );
        CREATE TABLE control_state (
            singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
            revision INTEGER NOT NULL CHECK (revision > 0)
        );
        INSERT INTO control_state (singleton, revision) VALUES (1, 1);
        CREATE TRIGGER node_configs_control_insert
        AFTER INSERT ON node_configs BEGIN
            UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
        END;
        CREATE TRIGGER node_configs_control_update
        AFTER UPDATE ON node_configs BEGIN
            UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
        END;
        CREATE TRIGGER node_configs_control_delete
        AFTER DELETE ON node_configs BEGIN
            UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
        END;",
    ),
    M::up(
        "CREATE TABLE ip_limits (
            ip TEXT PRIMARY KEY NOT NULL,
            max_connections INTEGER NOT NULL CHECK (max_connections > 0),
            updated_at_unix_ms INTEGER NOT NULL
        );
        CREATE TRIGGER ip_limits_control_insert
        AFTER INSERT ON ip_limits BEGIN
            UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
        END;
        CREATE TRIGGER ip_limits_control_update
        AFTER UPDATE ON ip_limits BEGIN
            UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
        END;
        CREATE TRIGGER ip_limits_control_delete
        AFTER DELETE ON ip_limits BEGIN
            UPDATE control_state SET revision = revision + 1 WHERE singleton = 1;
        END;",
    ),
];
const MIGRATIONS: Migrations<'static> = Migrations::from_slice(MIGRATION_LIST);

#[derive(Clone, Debug, Serialize)]
pub struct StoredNode {
    pub config: NodeConfig,
    pub revision: u64,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Debug)]
pub struct ControlSnapshot {
    pub revision: u64,
    pub nodes: Vec<StoredNode>,
    pub ip_limits: Vec<(IpAddr, usize)>,
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
        connection.pragma_update(None, "wal_autocheckpoint", 1_000)?;
        connection.pragma_update(None, "journal_size_limit", 64 * 1024 * 1024)?;
        MIGRATIONS
            .to_latest(&mut connection)
            .context("failed to migrate SQLite schema")?;
        Ok(Arc::new(Self {
            connection: Mutex::new(connection),
        }))
    }

    fn write_tx<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut connection = self.connection.lock();
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = f(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    pub fn list(&self) -> Result<Vec<StoredNode>> {
        let connection = self.connection.lock();
        list_nodes(&connection)
    }

    pub fn revision(&self) -> Result<u64> {
        let connection = self.connection.lock();
        read_revision(&connection)
    }

    pub fn get(&self, id: &str) -> Result<Option<StoredNode>> {
        let connection = self.connection.lock();
        connection
            .query_row(
                "SELECT config_json, revision, created_at_unix_ms, updated_at_unix_ms
                 FROM node_configs WHERE id = ?1",
                [id],
                decode_node_row,
            )
            .optional()
            .context("failed to load node configuration")
    }

    pub fn snapshot(&self) -> Result<ControlSnapshot> {
        let mut connection = self.connection.lock();
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        Ok(ControlSnapshot {
            revision: read_revision(&tx)?,
            nodes: list_nodes(&tx)?,
            ip_limits: list_ip_limits(&tx)?,
        })
    }

    pub fn insert(&self, config: &NodeConfig) -> Result<StoredNode> {
        self.write_tx(|tx| insert_node(tx, config))
    }

    pub fn update(
        &self,
        id: &str,
        expected_revision: u64,
        config: &NodeConfig,
    ) -> Result<Option<StoredNode>> {
        ensure!(config.id == id, "node id cannot be changed during update");
        let encoded = encode_node_config(config)?;
        let now = unix_millis();
        let revision = expected_revision.saturating_add(1);
        self.write_tx(|tx| {
            let created_at_unix_ms: Option<u64> = tx
                .query_row(
                    "UPDATE node_configs
                     SET config_json = ?1, revision = ?2, updated_at_unix_ms = ?3
                     WHERE id = ?4 AND revision = ?5
                     RETURNING created_at_unix_ms",
                    params![encoded, revision, now, id, expected_revision],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(created_at_unix_ms.map(|created_at_unix_ms| StoredNode {
                config: config.clone(),
                revision,
                created_at_unix_ms,
                updated_at_unix_ms: now,
            }))
        })
    }

    pub fn delete(&self, id: &str, expected_revision: Option<u64>) -> Result<bool> {
        self.write_tx(|tx| {
            let changed = tx.execute(
                "DELETE FROM node_configs WHERE id = ?1 AND (?2 IS NULL OR revision = ?2)",
                params![id, expected_revision],
            )?;
            Ok(changed == 1)
        })
    }

    pub fn seed_if_empty(&self, nodes: &[NodeConfig]) -> Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        self.write_tx(|tx| {
            let populated: bool =
                tx.query_row("SELECT EXISTS(SELECT 1 FROM node_configs)", [], |row| {
                    row.get(0)
                })?;
            if !populated {
                for node in nodes {
                    insert_node(tx, node)?;
                }
            }
            Ok(())
        })
    }

    pub fn list_ip_limits(&self) -> Result<Vec<(IpAddr, usize)>> {
        let connection = self.connection.lock();
        list_ip_limits(&connection)
    }

    pub fn set_ip_limit(&self, ip: IpAddr, limit: usize) -> Result<()> {
        let now = unix_millis();
        self.write_tx(|tx| {
            tx.execute(
                "INSERT INTO ip_limits (ip, max_connections, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(ip) DO UPDATE SET
                    max_connections = excluded.max_connections,
                    updated_at_unix_ms = excluded.updated_at_unix_ms",
                params![ip.to_string(), limit, now],
            )?;
            Ok(())
        })
    }

    pub fn delete_ip_limit(&self, ip: IpAddr) -> Result<bool> {
        self.write_tx(|tx| {
            let changed = tx.execute("DELETE FROM ip_limits WHERE ip = ?1", [ip.to_string()])?;
            Ok(changed == 1)
        })
    }

    pub async fn run_blocking<T, F>(self: &Arc<Self>, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Self) -> Result<T> + Send + 'static,
    {
        let store = Arc::clone(self);
        tokio::task::spawn_blocking(move || operation(&store))
            .await
            .context("SQLite worker task failed")?
    }

    pub async fn snapshot_async(self: &Arc<Self>) -> Result<ControlSnapshot> {
        self.run_blocking(Self::snapshot).await
    }

    pub async fn set_ip_limit_async(self: &Arc<Self>, ip: IpAddr, limit: usize) -> Result<()> {
        self.run_blocking(move |store| store.set_ip_limit(ip, limit))
            .await
    }

    pub async fn delete_ip_limit_async(self: &Arc<Self>, ip: IpAddr) -> Result<bool> {
        self.run_blocking(move |store| store.delete_ip_limit(ip))
            .await
    }

    pub async fn list_async(self: &Arc<Self>) -> Result<Vec<StoredNode>> {
        self.run_blocking(Self::list).await
    }

    pub async fn revision_async(self: &Arc<Self>) -> Result<u64> {
        self.run_blocking(Self::revision).await
    }

    pub async fn get_async(self: &Arc<Self>, id: &str) -> Result<Option<StoredNode>> {
        let id = id.to_owned();
        self.run_blocking(move |store| store.get(&id)).await
    }

    pub async fn insert_async(self: &Arc<Self>, config: &NodeConfig) -> Result<StoredNode> {
        let config = config.clone();
        self.run_blocking(move |store| store.insert(&config)).await
    }

    pub async fn update_async(
        self: &Arc<Self>,
        id: &str,
        expected_revision: u64,
        config: &NodeConfig,
    ) -> Result<Option<StoredNode>> {
        let id = id.to_owned();
        let config = config.clone();
        self.run_blocking(move |store| store.update(&id, expected_revision, &config))
            .await
    }

    pub async fn delete_async(
        self: &Arc<Self>,
        id: &str,
        expected_revision: Option<u64>,
    ) -> Result<bool> {
        let id = id.to_owned();
        self.run_blocking(move |store| store.delete(&id, expected_revision))
            .await
    }
}

fn read_revision(connection: &Connection) -> Result<u64> {
    connection
        .query_row(
            "SELECT revision FROM control_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .context("failed to load control-plane revision")
}

fn list_nodes(connection: &Connection) -> Result<Vec<StoredNode>> {
    let mut statement = connection.prepare(
        "SELECT config_json, revision, created_at_unix_ms, updated_at_unix_ms
         FROM node_configs ORDER BY id",
    )?;
    let rows = statement.query_map([], decode_node_row)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to load node configuration")
}

fn list_ip_limits(connection: &Connection) -> Result<Vec<(IpAddr, usize)>> {
    let mut statement =
        connection.prepare("SELECT ip, max_connections FROM ip_limits ORDER BY ip")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, usize>(1)?))
    })?;
    rows.map(|row| {
        let (ip, limit) = row.context("failed to load IP limit")?;
        let ip = ip
            .parse::<IpAddr>()
            .with_context(|| format!("invalid persisted IP limit address {ip:?}"))?;
        Ok((ip, limit))
    })
    .collect()
}

fn encode_node_config(config: &NodeConfig) -> Result<String> {
    validate_node_config(config)?;
    let mut persisted = config.clone();
    crypto::encrypt_node_secrets(&mut persisted);
    Ok(serde_json::to_string(&persisted)?)
}

fn insert_node(connection: &Connection, config: &NodeConfig) -> Result<StoredNode> {
    let encoded = encode_node_config(config)?;
    let now = unix_millis();
    connection
        .execute(
            "INSERT INTO node_configs (
                id, config_json, revision, created_at_unix_ms, updated_at_unix_ms
             ) VALUES (?1, ?2, 1, ?3, ?3)",
            params![config.id, encoded, now],
        )
        .with_context(|| format!("failed to insert node {:?}", config.id))?;
    Ok(StoredNode {
        config: config.clone(),
        revision: 1,
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    })
}

fn decode_node_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredNode> {
    let encoded: String = row.get(0)?;
    let decode = || -> anyhow::Result<NodeConfig> {
        let mut config: NodeConfig = if encoded.contains("anthropic_protocol")
            || encoded.contains("flatten_codex_namespaces")
        {
            let mut value: serde_json::Value = serde_json::from_str(&encoded)?;
            // Keep old persisted nodes readable without carrying retired switches
            // through runtime configuration or accepting them in new API requests.
            if let Some(provider) = value
                .get_mut("provider")
                .and_then(serde_json::Value::as_object_mut)
            {
                provider.remove("anthropic_protocol");
                provider.remove("flatten_codex_namespaces");
            }
            serde_json::from_value(value)?
        } else {
            serde_json::from_str(&encoded)?
        };
        crypto::decrypt_node_secrets(&mut config)?;
        validate_node_config(&config)?;
        Ok(config)
    };
    let config = decode().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            encoded.len(),
            rusqlite::types::Type::Text,
            error.into(),
        )
    })?;
    Ok(StoredNode {
        config,
        revision: row.get(1)?,
        created_at_unix_ms: row.get(2)?,
        updated_at_unix_ms: row.get(3)?,
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use super::*;

    struct TempDb(PathBuf);

    impl TempDb {
        fn new(prefix: &str) -> Self {
            Self(std::env::temp_dir().join(format!("estuary-{prefix}-{}.db", uuid::Uuid::now_v7())))
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for ext in ["db", "db-wal", "db-shm"] {
                let _ = std::fs::remove_file(self.0.with_extension(ext));
            }
        }
    }

    fn node() -> NodeConfig {
        NodeConfig {
            id: "node-a".to_owned(),
            base_url: "http://127.0.0.1:8000/v1".to_owned(),
            models: HashMap::from([("public".to_owned(), "upstream".to_owned())]),
            ..NodeConfig::default()
        }
    }

    #[test]
    fn failed_seed_rolls_back_nodes_and_control_revision() {
        let store = NodeStore::memory().unwrap();
        let revision = store.revision().unwrap();
        assert!(store.seed_if_empty(&[node(), node()]).is_err());
        assert!(store.list().unwrap().is_empty());
        assert_eq!(store.revision().unwrap(), revision);

        let mut second = node();
        second.id = "node-b".to_owned();
        store.seed_if_empty(&[node(), second]).unwrap();
        assert_eq!(store.list().unwrap().len(), 2);
        assert_eq!(store.revision().unwrap(), revision + 2);

        store.seed_if_empty(&[NodeConfig::default()]).unwrap();
        assert_eq!(store.list().unwrap().len(), 2);
        assert_eq!(store.revision().unwrap(), revision + 2);
    }

    #[test]
    fn invalid_seed_does_not_leave_partial_configuration() {
        let store = NodeStore::memory().unwrap();
        assert!(
            store
                .seed_if_empty(&[node(), NodeConfig::default()])
                .is_err()
        );
        assert!(store.list().unwrap().is_empty());
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
        assert_eq!(updated.created_at_unix_ms, inserted.created_at_unix_ms);
        assert!((store.get("node-a").unwrap().unwrap().config.weight - 2.0).abs() < f64::EPSILON);
        assert!(store.update("node-a", 1, &changed).unwrap().is_none());
        assert!(store.delete("node-a", Some(2)).unwrap());
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    fn reads_retired_family_from_persisted_records_and_drops_it_on_save() {
        let store = NodeStore::memory().unwrap();
        store.insert(&node()).unwrap();
        let mut legacy = serde_json::to_value(node()).unwrap();
        legacy["model_capabilities"] =
            serde_json::json!({"*":{"family":"deepseek","multimodal":false}});
        store
            .connection
            .lock()
            .execute(
                "UPDATE node_configs SET config_json = ?1 WHERE id = 'node-a'",
                [legacy.to_string()],
            )
            .unwrap();
        let stored = store.list().unwrap().remove(0);
        assert!(!stored.config.model_capabilities["*"].multimodal);
        store
            .update("node-a", stored.revision, &stored.config)
            .unwrap()
            .unwrap();
        let encoded: String = store
            .connection
            .lock()
            .query_row(
                "SELECT config_json FROM node_configs WHERE id = 'node-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!encoded.contains("family"));
        assert!(
            !store
                .get("node-a")
                .unwrap()
                .unwrap()
                .config
                .model_capabilities["*"]
                .multimodal
        );
    }

    #[test]
    fn retired_provider_fields_are_discarded_only_when_loading_old_records() {
        let store = NodeStore::memory().unwrap();
        store.insert(&node()).unwrap();
        let mut legacy = serde_json::to_value(node()).unwrap();
        legacy["provider"]["anthropic_protocol"] = "native".into();
        legacy["provider"]["flatten_codex_namespaces"] = true.into();
        assert!(serde_json::from_value::<NodeConfig>(legacy.clone()).is_err());
        store
            .connection
            .lock()
            .execute(
                "UPDATE node_configs SET config_json=?1 WHERE id='node-a'",
                [legacy.to_string()],
            )
            .unwrap();
        let stored = store.get("node-a").unwrap().unwrap();
        assert_eq!(stored.config.provider.kind, node().provider.kind);
        assert_eq!(store.list().unwrap().len(), 1);
        store
            .update("node-a", stored.revision, &stored.config)
            .unwrap()
            .unwrap();
        let encoded: String = store
            .connection
            .lock()
            .query_row(
                "SELECT config_json FROM node_configs WHERE id='node-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!encoded.contains("anthropic_protocol"));
        assert!(!encoded.contains("flatten_codex_namespaces"));
        legacy["provider"]["misspelled_option"] = true.into();
        store
            .connection
            .lock()
            .execute(
                "UPDATE node_configs SET config_json=?1 WHERE id='node-a'",
                [legacy.to_string()],
            )
            .unwrap();
        assert!(store.get("node-a").is_err());
    }

    #[test]
    fn survives_database_reopen() {
        let db = TempDb::new("store");
        {
            let store = NodeStore::open(&db.0).unwrap();
            store.insert(&node()).unwrap();
        }
        {
            let store = NodeStore::open(&db.0).unwrap();
            assert_eq!(store.list().unwrap()[0].config.id, "node-a");
        }
    }

    #[test]
    fn revision_triggers_are_visible_to_another_process_connection() {
        let db = TempDb::new("rev");
        let first = NodeStore::open(&db.0).unwrap();
        let second = NodeStore::open(&db.0).unwrap();
        let initial = second.revision().unwrap();

        first.insert(&node()).unwrap();
        assert!(second.revision().unwrap() > initial);
        let after_insert = second.revision().unwrap();
        first.delete("node-a", Some(1)).unwrap();
        assert!(second.revision().unwrap() > after_insert);
    }

    #[test]
    fn ip_limits_persist_and_bump_control_revision() {
        let db = TempDb::new("ip");
        let ip: IpAddr = "198.51.100.10".parse().unwrap();
        {
            let store = NodeStore::open(&db.0).unwrap();
            let initial_rev = store.revision().unwrap();
            store.set_ip_limit(ip, 42).unwrap();
            assert!(store.revision().unwrap() > initial_rev);
            assert_eq!(store.list_ip_limits().unwrap(), vec![(ip, 42)]);
        }
        {
            let store = NodeStore::open(&db.0).unwrap();
            assert_eq!(store.list_ip_limits().unwrap(), vec![(ip, 42)]);
            let rev_before_delete = store.revision().unwrap();
            assert!(store.delete_ip_limit(ip).unwrap());
            assert!(store.revision().unwrap() > rev_before_delete);
            assert!(store.list_ip_limits().unwrap().is_empty());
        }
    }
}
