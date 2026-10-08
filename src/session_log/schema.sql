CREATE TABLE sessions (
 id TEXT PRIMARY KEY NOT NULL,
 source TEXT NOT NULL
);
CREATE TABLE requests (
 id TEXT PRIMARY KEY NOT NULL,
 session_id TEXT REFERENCES sessions(id),
 started_at_ms INTEGER NOT NULL,
 ended_at_ms INTEGER,
 model TEXT,
 outcome TEXT NOT NULL,
 data TEXT NOT NULL
);
CREATE INDEX requests_time ON requests(started_at_ms DESC,id DESC);
CREATE INDEX requests_session ON requests(session_id,started_at_ms DESC,id DESC);
CREATE INDEX requests_errors ON requests(started_at_ms DESC,id DESC) WHERE outcome='error';
CREATE TABLE attempts (
 request_id TEXT NOT NULL REFERENCES requests(id) ON DELETE CASCADE,
 number INTEGER NOT NULL,
 node TEXT NOT NULL,
 data TEXT NOT NULL,
 PRIMARY KEY(request_id,number)
);
CREATE TABLE request_events (
 request_id TEXT NOT NULL REFERENCES requests(id) ON DELETE CASCADE,
 sequence INTEGER NOT NULL,
 data TEXT NOT NULL,
 PRIMARY KEY(request_id,sequence)
);
CREATE TABLE content_blobs (
 hash TEXT PRIMARY KEY NOT NULL,
 codec TEXT NOT NULL,
 data BLOB NOT NULL,
 raw_bytes INTEGER NOT NULL,
 stored_bytes INTEGER NOT NULL,
 blob_refs TEXT NOT NULL,
 sequence_refs TEXT NOT NULL,
 refs INTEGER NOT NULL DEFAULT 0 CHECK(refs>=0)
);
CREATE TABLE sequence_nodes (
 hash TEXT PRIMARY KEY NOT NULL,
 previous_hash TEXT REFERENCES sequence_nodes(hash),
 item_hash TEXT NOT NULL REFERENCES content_blobs(hash),
 item_count INTEGER NOT NULL,
 refs INTEGER NOT NULL DEFAULT 0 CHECK(refs>=0)
);
CREATE TABLE payloads (
 id INTEGER PRIMARY KEY,
 request_id TEXT NOT NULL REFERENCES requests(id) ON DELETE CASCADE,
 stage TEXT NOT NULL,
 attempt INTEGER NOT NULL,
 root_hash TEXT NOT NULL REFERENCES content_blobs(hash),
 state TEXT NOT NULL,
 bytes_seen INTEGER NOT NULL,
 representation TEXT NOT NULL,
 created_at_ms INTEGER NOT NULL,
 UNIQUE(request_id,stage,attempt)
);
CREATE INDEX payloads_retention ON payloads(created_at_ms);
CREATE TRIGGER release_payload AFTER DELETE ON payloads BEGIN
 UPDATE content_blobs SET refs=refs-1 WHERE hash=OLD.root_hash;
END;
CREATE TRIGGER release_blob AFTER DELETE ON content_blobs BEGIN
 UPDATE content_blobs SET refs=refs-(SELECT count(*) FROM json_each(OLD.blob_refs) WHERE value=content_blobs.hash)
 WHERE hash IN (SELECT value FROM json_each(OLD.blob_refs));
 UPDATE sequence_nodes SET refs=refs-1 WHERE hash IN (SELECT value FROM json_each(OLD.sequence_refs));
END;
CREATE TRIGGER release_sequence AFTER DELETE ON sequence_nodes BEGIN
 UPDATE content_blobs SET refs=refs-1 WHERE hash=OLD.item_hash;
 UPDATE sequence_nodes SET refs=refs-1 WHERE hash=OLD.previous_hash;
END;
