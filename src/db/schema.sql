-- Engine tables. Your plugin keeps its own tables next to these.
-- Block numbers are BIGINT (int8): enough for any EVM chain's height.

-- Everything up to and including last_block is ingested, in the same
-- transaction as the plugin's writes for it.
CREATE TABLE IF NOT EXISTS indexer_cursor (
  chain_id        INTEGER PRIMARY KEY,
  last_block      BIGINT      NOT NULL,
  last_block_hash TEXT        NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per committed batch end, newest N kept: the window a reorg is
-- searched in for the last block both forks agree on.
CREATE TABLE IF NOT EXISTS block_checkpoint (
  chain_id     INTEGER NOT NULL,
  block_number BIGINT  NOT NULL,
  block_hash   TEXT    NOT NULL,
  PRIMARY KEY (chain_id, block_number)
);

-- Every ingested event, verbatim. It is the replay guard (an event already
-- here is never handed to the plugin again) and a replay source.
CREATE TABLE IF NOT EXISTS raw_event (
  chain_id     INTEGER     NOT NULL,
  block_number BIGINT      NOT NULL,
  block_hash   TEXT        NOT NULL,
  log_index    INTEGER     NOT NULL,
  tx_hash      TEXT        NOT NULL,
  tx_index     INTEGER     NOT NULL,
  address      TEXT        NOT NULL,
  event_name   TEXT        NOT NULL,
  args         JSONB       NOT NULL,
  block_time   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (chain_id, block_hash, log_index)
);
CREATE INDEX IF NOT EXISTS raw_event_chain_block ON raw_event (chain_id, block_number);

-- Adaptive range size, persisted so a restart mid-backfill resumes at the
-- size the provider was last happy with.
CREATE TABLE IF NOT EXISTS range_progress (
  chain_id     INTEGER PRIMARY KEY,
  range_size   INTEGER NOT NULL,
  done_through BIGINT
);
