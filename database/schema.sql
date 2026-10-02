-- SQLite schema for the Bird Monitoring Raspberry Pi system
-- Database: detections

CREATE TABLE IF NOT EXISTS detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    species_code TEXT NOT NULL,
    common_name TEXT NOT NULL,
    sci_name TEXT NOT NULL,
    confidence REAL NOT NULL,

    detected_at TEXT NOT NULL,
    node_id TEXT NOT NULL,

    lat REAL,
    lon REAL,

    sent INTEGER NOT NULL DEFAULT 0,
    sent_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,

    event_id TEXT,
    start_time TEXT,
    end_time TEXT,
    duration REAL
);

-- Prevent duplicate events while allowing legacy rows
-- with NULL event_id.
CREATE UNIQUE INDEX IF NOT EXISTS
idx_detections_event_id
ON detections(event_id)
WHERE event_id IS NOT NULL;

-- Support efficient retrieval of unsent records.
CREATE INDEX IF NOT EXISTS
idx_detections_pending
ON detections(sent, id);
