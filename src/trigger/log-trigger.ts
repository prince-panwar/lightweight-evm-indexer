import { formatLog, type Hex, type Log } from 'viem';
import type { ChainConfig, IndexerPlugin, Logger } from '../types.js';
import type { Decoder } from '../indexer/decoder.js';
import type { WatchSet } from '../indexer/watch-set.js';
import type { RpcUsage } from '../chain/rpc-usage.js';
import { LogStream, type RpcLog, type SocketFactory } from './log-stream.js';

/** How a chain is being driven right now. */
export type TriggerMode =
  /** No websocket configured: poll every interval. */
  | 'off'
  /** Subscribed: ingest on notifications plus the periodic sweep. */
  | 'live'
  /** Configured but not subscribed (connecting, or dropped): poll meanwhile. */
  | 'down';

interface ChainTrigger {
  stream: LogStream;
  live: boolean;
  /** Highest announced block not yet ingested, and when anything first became pending. */
  pendingMax: bigint | null;
  pendingSince: number;
  /** Set on every (re)connect: events may have been missed while it was down. */
  catchUp: boolean;
  /** Notifications accepted so far (monotonic): lets a pass tell if any arrived meanwhile. */
  notices: number;
  /**
   * Addresses a trusted notification introduced (`discover`) that ingestion
   * has not caught up with yet: an event from a brand-new contract must not
   * be dismissed as foreign just because the watched set has not learned it.
   */
  learned: Set<string>;
}

/** Bounded: ingestion adds these to the watched set within seconds anyway. */
const MAX_LEARNED = 1_000;

/**
 * Tells the runner WHEN a chain has something to ingest, so idle chains stop
 * costing RPC calls between sweeps.
 *
 * One `eth_subscribe('logs')` per enabled chain on the plugin's phase-1
 * topics. Each notification is decoded and passed through the same trust
 * check ingestion uses (topic-only subscriptions also match look-alike
 * contracts, which would otherwise each cost a pass). A trusted one marks the
 * chain pending; the runner ingests once the block has had time to settle.
 * The data itself still comes from eth_getLogs over the cursor range.
 *
 * Opened lazily by the runner after it holds leadership, so a standby opens no
 * sockets and pays for no notifications.
 */
export class LogTrigger {
  private readonly triggers = new Map<number, ChainTrigger>();
  private readonly enabled = new Set<number>();

  constructor(
    private readonly chains: Map<number, ChainConfig>,
    private readonly decoder: Decoder,
    private readonly watched: WatchSet,
    private readonly plugin: IndexerPlugin,
    private readonly topics: Hex[],
    private readonly usage: RpcUsage,
    private readonly log: Logger,
    private readonly socketFactory?: SocketFactory,
  ) {
    for (const chain of chains.values()) if (chain.websocket && topics.length > 0) this.enabled.add(chain.chainId);
  }

  /** The chain's mode, opening its stream on first use. */
  mode(chainId: number): TriggerMode {
    if (!this.enabled.has(chainId)) return 'off';
    const trigger = this.triggers.get(chainId) ?? this.open(chainId);
    if (!trigger) return 'off';
    return trigger.live ? 'live' : 'down';
  }

  pending(chainId: number): { max: bigint; since: number } | null {
    const t = this.triggers.get(chainId);
    return t && t.pendingMax !== null ? { max: t.pendingMax, since: t.pendingSince } : null;
  }

  /** Ingestion reached `toBlock`: every announcement at or below it is done. */
  ingested(chainId: number, toBlock: bigint): void {
    const t = this.triggers.get(chainId);
    if (t && t.pendingMax !== null && t.pendingMax <= toBlock) t.pendingMax = null;
  }

  notices(chainId: number): number {
    return this.triggers.get(chainId)?.notices ?? 0;
  }

  /** True once after each (re)connect: the gap while it was down must be swept. */
  takeCatchUp(chainId: number): boolean {
    const t = this.triggers.get(chainId);
    if (!t?.catchUp) return false;
    t.catchUp = false;
    return true;
  }

  /** Close every socket (they reopen on the next `mode()`): shutdown, or leadership lost. */
  stopAll(): void {
    for (const t of this.triggers.values()) t.stream.stop();
    this.triggers.clear();
  }

  private open(chainId: number): ChainTrigger | null {
    const chain = this.chains.get(chainId);
    const url = chain?.wsUrl ?? socketUrl(chain?.rpcUrls[0]);
    if (!chain || !url) {
      this.log.warn(`chain ${chainId}: websocket enabled but no ws(s) URL could be derived; polling instead`);
      this.enabled.delete(chainId);
      return null;
    }

    const trigger: ChainTrigger = {
      live: false,
      pendingMax: null,
      pendingSince: 0,
      catchUp: false,
      notices: 0,
      learned: new Set(),
      stream: new LogStream({
        chainId,
        url, // never logged: provider URLs carry API keys
        topics: this.topics,
        addresses: this.plugin.subscriptionAddresses?.(chainId),
        socketFactory: this.socketFactory,
        log: this.log,
        onLog: (raw) => {
          if (!this.accept(chainId, trigger, raw)) {
            this.usage.record(chainId, 'ws_ignored');
            return;
          }
          const block = BigInt(raw.blockNumber);
          trigger.notices++;
          if (trigger.pendingMax === null) trigger.pendingSince = Date.now();
          if (trigger.pendingMax === null || block > trigger.pendingMax) trigger.pendingMax = block;
        },
        onState: (live) => {
          trigger.live = live;
          if (live) trigger.catchUp = true;
          this.log.info(`chain ${chainId}: websocket ${live ? 'subscribed' : 'down, polling meanwhile'}`);
        },
        onUsage: (kind) => this.usage.record(chainId, kind === 'notification' ? 'ws_notification' : `ws_${kind}`),
      }),
    };

    this.triggers.set(chainId, trigger);
    trigger.stream.start();
    return trigger;
  }

  /** Decode the notification and apply the plugin's own trust and discovery rules. */
  private accept(chainId: number, trigger: ChainTrigger, raw: RpcLog): boolean {
    let event;
    try {
      [event] = this.decoder.decodeUntimed(chainId, [formatLog(raw as never) as Log]);
    } catch {
      return false;
    }
    if (!event) return false;

    const watched = new Set([...this.watched.set(chainId), ...trigger.learned]);
    const ctx = { chainId, watched };
    if (!this.plugin.isTrusted(event, ctx)) return false;
    for (const address of this.plugin.discover?.(event, ctx) ?? []) trigger.learned.add(address.toLowerCase());
    if (trigger.learned.size > MAX_LEARNED) trigger.learned.clear();
    return true;
  }
}

/**
 * The socket URL for an RPC URL: Alchemy, QuickNode and a local anvil serve
 * the socket on the same host and path, so only the scheme changes. Null for
 * anything else, which keeps that chain on plain polling.
 */
export function socketUrl(rpcUrl: string | undefined): string | null {
  if (rpcUrl?.startsWith('https://')) return `wss://${rpcUrl.slice('https://'.length)}`;
  if (rpcUrl?.startsWith('http://')) return `ws://${rpcUrl.slice('http://'.length)}`;
  return null;
}
