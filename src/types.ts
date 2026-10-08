import type { AbiEvent, Address, Hex } from 'viem';
import type { PoolClient } from 'pg';

/**
 * How a chain decides which blocks are settled enough to ingest.
 *
 * `confirmations` is head minus a fixed depth: probabilistic, and the only
 * option on most L2s. `finalized` uses the chain's own `finalized` tag, with
 * `maxLagBlocks` as the distrust threshold: a provider serving a pointer that
 * far behind head is ignored and the chain falls back to confirmations.
 */
export type Finality = { mode: 'confirmations' } | { mode: 'finalized'; maxLagBlocks: number };

export interface ChainConfig {
  chainId: number;
  name: string;
  /** HTTP(S) JSON-RPC endpoints, tried in order (viem fallback transport). */
  rpcUrls: string[];
  /** First block to ingest on a fresh database (usually your contracts' deploy block). */
  startBlock: bigint;
  /** Blocks behind head treated as settled (also the fallback depth in `finalized` mode). */
  confirmations: number;
  /**
   * Typical block interval. Only an estimate: it decides when a block the
   * websocket announced is expected to be settled (confirmations x block time).
   */
  blockTimeMs: number;
  /** Defaults to `{ mode: 'confirmations' }`. */
  finality?: Finality;
  /**
   * Trigger ingestion from an `eth_subscribe('logs')` websocket instead of
   * polling every interval. The socket URL defaults to the first RPC URL with
   * http(s) swapped for ws(s), which is how Alchemy, QuickNode and a local
   * anvil serve it; set `wsUrl` for anything else.
   */
  websocket?: boolean;
  wsUrl?: string;
}

/** A decoded log, ready to store. Addresses are lowercase; args are strings. */
export interface DecodedEvent {
  chainId: number;
  blockNumber: bigint;
  blockHash: Hex;
  blockTime: Date;
  logIndex: number;
  txHash: Hex;
  txIndex: number;
  address: Address;
  eventName: string;
  args: Record<string, string>;
}

/** A decoded event before its block time is attached. */
export type UntimedEvent = Omit<DecodedEvent, 'blockTime'>;

/** Context handed to a plugin's trust and discovery hooks. */
export interface TrustContext {
  chainId: number;
  /** Addresses discovered so far on this chain (lowercase). */
  watched: ReadonlySet<string>;
}

/**
 * Everything protocol-specific. The engine owns ingestion: cursors, ranges,
 * retries, reorgs, websockets, leadership. A plugin owns meaning: which
 * events matter, which emitters are trusted, and what to write.
 */
export interface IndexerPlugin {
  /** ABI event items to index. */
  events: AbiEvent[];
  /**
   * Event names too common to fetch by signature alone (ERC-20 `Transfer`:
   * every token on the chain emits it). These are fetched only from the
   * watched address set, in chunks; every other event is fetched by topic
   * from anyone and filtered through `isTrusted`.
   */
  addressScopedEvents?: string[];
  /**
   * A FIXED address list to filter the websocket subscription by. With it,
   * the socket subscribes to every event kind from those addresses (address-
   * scoped ones included); without it, it subscribes to the topic-filtered
   * events from anyone. Use it when your contracts are known up front; leave
   * it out for factory-style discovery.
   */
  subscriptionAddresses?(chainId: number): Address[];
  /**
   * Addresses known when a chain starts, plus any your own tables say were
   * discovered earlier (they are lost on restart otherwise). Called once per
   * chain at start, and again after a reorg rewind.
   */
  loadWatched(chainId: number, db: PoolClient): Promise<Address[]>;
  /**
   * Whether an event comes from a contract you trust. Topic-filtered fetches
   * return any contract emitting the same signature, so this is a security
   * boundary: an untrusted event is never stored or handled. Must be pure.
   */
  isTrusted(event: UntimedEvent, ctx: TrustContext): boolean;
  /**
   * Addresses a trusted event introduces (the factory pattern: a
   * `PairCreated` returns the new pair). They join the watched set at once,
   * so an event emitted by the new contract in the same range is trusted.
   */
  discover?(event: UntimedEvent, ctx: TrustContext): Address[];
  /**
   * Write your projections for one event. Runs inside the commit transaction
   * together with the cursor advance, in (blockNumber, logIndex) order, and
   * only for events never seen before, so handlers need not be idempotent.
   */
  handle(event: DecodedEvent, db: PoolClient): Promise<void>;
  /**
   * Undo everything your tables hold above `forkBlock` on `chainId`. Runs in
   * the rewind transaction; the engine deletes its own rows and moves the
   * cursor back, then re-ingests from `forkBlock + 1`.
   */
  rewind(chainId: number, forkBlock: bigint, db: PoolClient): Promise<void>;
}

export interface Logger {
  debug(msg: string): void;
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string, err?: unknown): void;
}
