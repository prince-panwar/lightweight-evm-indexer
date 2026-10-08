import type { Hex } from 'viem';
import type { Logger } from '../types.js';

/** The part of a WebSocket this class uses; Node 22's global WebSocket fits it. */
export interface SocketLike {
  onopen: ((ev?: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev?: unknown) => void) | null;
  onerror: ((ev?: unknown) => void) | null;
  send(data: string): void;
  close(): void;
}

export type SocketFactory = (url: string) => SocketLike;

/** A log as eth_subscribe delivers it: hex strings, not yet formatted. */
export interface RpcLog {
  address: string;
  topics: string[];
  data: string;
  blockNumber: string;
  blockHash?: string;
  transactionHash?: string;
  transactionIndex?: string;
  logIndex?: string;
  removed?: boolean;
}

export interface LogStreamOptions {
  chainId: number;
  url: string;
  /** Topic-0 alternatives to subscribe to (any of). */
  topics: Hex[];
  /** Optional address filter (any of). */
  addresses?: Hex[];
  /** A notified log, raw RPC form (hex quantities). Called once per log. */
  onLog: (log: RpcLog) => void;
  /** Subscribed and confirmed (true), or not usable right now (false). */
  onState: (live: boolean) => void;
  /** Billable calls made over the socket (subscribe, heartbeat) and notifications received. */
  onUsage?: (kind: 'eth_subscribe' | 'eth_chainId' | 'notification') => void;
  log?: Pick<Logger, 'warn'>;
  socketFactory?: SocketFactory;
}

const SUBSCRIBE_ID = 1;
/**
 * One cheap round trip every two minutes proves the socket still answers. A
 * socket that dies silently is noticed within HEARTBEAT_MS + HEARTBEAT_TIMEOUT_MS
 * (~135s); the runner's periodic sweep keeps ingesting meanwhile, so the cost of
 * noticing late is latency on announced events, never data.
 */
export const HEARTBEAT_MS = 120_000;
/** A heartbeat unanswered this long means the socket is dead even if not closed. */
export const HEARTBEAT_TIMEOUT_MS = 15_000;
export const RECONNECT_BASE_MS = 1_000;
/** Connect + subscribe must complete within this, or the attempt is abandoned and retried. */
export const SUBSCRIBE_TIMEOUT_MS = 20_000;
export const RECONNECT_MAX_MS = 60_000;

/**
 * One `eth_subscribe('logs')` stream for one chain, kept alive.
 *
 * A TRIGGER, NOT A DATA SOURCE. A notification only tells the indexer that a
 * block holds one of our events; the events themselves are still read with
 * `eth_getLogs` over the cursor's range. So a dropped socket, a lost message or
 * a provider hiccup can only DELAY an event until the next sweep, never lose
 * it — which is why this class can afford to be simple about delivery.
 *
 * Reconnects with backoff, re-subscribing each time, and reports `live` only
 * once the subscription itself is confirmed.
 */
export class LogStream {
  private socket: SocketLike | null = null;
  private stopped = false;
  private live = false;
  private attempts = 0;
  private nextId = SUBSCRIBE_ID + 1;
  private pendingHeartbeat: { id: number; sentAt: number } | null = null;
  private heartbeat: NodeJS.Timeout | null = null;
  private heartbeatTimeout: NodeJS.Timeout | null = null;
  private subscribeTimeout: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private readonly factory: SocketFactory;

  constructor(private readonly o: LogStreamOptions) {
    this.factory = o.socketFactory ?? ((url) => new WebSocket(url) as unknown as SocketLike);
  }

  start(): void {
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.teardown();
    this.setLive(false);
  }

  isLive(): boolean {
    return this.live;
  }

  private connect(): void {
    if (this.stopped) return;
    let socket: SocketLike;
    try {
      socket = this.factory(this.o.url);
    } catch (err) {
      this.o.log?.warn(`chain ${this.o.chainId}: websocket could not be created: ${message(err)}`);
      this.scheduleReconnect();
      return;
    }
    this.socket = socket;

    // A handshake or an eth_subscribe that never answers would otherwise leave
    // the stream connecting forever, silently polling instead.
    this.subscribeTimeout = setTimeout(() => {
      if (this.socket === socket && !this.live) {
        this.o.log?.warn(`chain ${this.o.chainId}: websocket did not subscribe in time; retrying`);
        this.restart();
      }
    }, SUBSCRIBE_TIMEOUT_MS);

    socket.onopen = () => {
      this.o.onUsage?.('eth_subscribe');
      socket.send(
        JSON.stringify({
          jsonrpc: '2.0',
          id: SUBSCRIBE_ID,
          method: 'eth_subscribe',
          params: [
            'logs',
            {
              ...(this.o.addresses && this.o.addresses.length > 0 ? { address: this.o.addresses } : {}),
              topics: [this.o.topics],
            },
          ],
        }),
      );
    };
    socket.onmessage = (ev) => {
      // A throw here would be an uncaught exception in the sole DB writer.
      try {
        this.onMessage(socket, ev.data);
      } catch (err) {
        this.o.log?.warn(`chain ${this.o.chainId}: bad websocket message ignored: ${message(err)}`);
      }
    };
    socket.onerror = () => {
      // `close` always follows; reconnecting is handled there, once.
    };
    socket.onclose = () => {
      if (this.socket !== socket) return; // an old socket we already replaced
      this.teardown();
      this.setLive(false);
      this.scheduleReconnect();
    };
  }

  private onMessage(socket: SocketLike, raw: unknown): void {
    if (this.socket !== socket) return;
    let data: {
      id?: number;
      result?: unknown;
      error?: { message?: string };
      method?: string;
      params?: { result?: RpcLog };
    };
    try {
      data = JSON.parse(typeof raw === 'string' ? raw : String(raw));
    } catch {
      return;
    }

    if (data.method === 'eth_subscription') {
      this.o.onUsage?.('notification');
      const log = data.params?.result;
      if (log && typeof log.blockNumber === 'string' && /^0x[0-9a-fA-F]+$/.test(log.blockNumber)) {
        this.o.onLog(log);
      }
      return;
    }

    if (data.id === SUBSCRIBE_ID) {
      if (data.error || typeof data.result !== 'string') {
        this.o.log?.warn(
          `chain ${this.o.chainId}: eth_subscribe refused: ${data.error?.message ?? 'no subscription id'}`,
        );
        this.restart();
        return;
      }
      // NOT resetting the backoff here: an endpoint that accepts and drops at
      // once would then be retried every second. A heartbeat answered (a
      // minute of stable connection) resets it instead.
      if (this.subscribeTimeout) clearTimeout(this.subscribeTimeout);
      this.subscribeTimeout = null;
      this.setLive(true);
      this.startHeartbeat();
      return;
    }

    if (this.pendingHeartbeat && data.id === this.pendingHeartbeat.id) {
      this.pendingHeartbeat = null;
      if (this.heartbeatTimeout) clearTimeout(this.heartbeatTimeout);
      this.heartbeatTimeout = null;
      this.attempts = 0; // proven stable
    }
  }

  private startHeartbeat(): void {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = setInterval(() => this.beat(), HEARTBEAT_MS);
  }

  private beat(): void {
    const socket = this.socket;
    if (!socket) return;
    if (this.pendingHeartbeat) {
      // The previous one never came back: the connection is dead but nobody
      // closed it, which is exactly the state a subscription goes quiet in.
      this.o.log?.warn(`chain ${this.o.chainId}: websocket heartbeat unanswered; reconnecting`);
      this.restart();
      return;
    }
    const id = this.nextId++;
    this.pendingHeartbeat = { id, sentAt: Date.now() };
    this.o.onUsage?.('eth_chainId');
    socket.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'eth_chainId', params: [] }));
    this.heartbeatTimeout = setTimeout(() => {
      this.heartbeatTimeout = null;
      if (this.pendingHeartbeat?.id === id && this.socket === socket) {
        this.o.log?.warn(`chain ${this.o.chainId}: websocket heartbeat timed out; reconnecting`);
        this.restart();
      }
    }, HEARTBEAT_TIMEOUT_MS);
  }

  /** Drop the current socket and start over (with backoff). */
  private restart(): void {
    this.teardown();
    this.setLive(false);
    this.scheduleReconnect();
  }

  private teardown(): void {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
    if (this.heartbeatTimeout) clearTimeout(this.heartbeatTimeout);
    this.heartbeatTimeout = null;
    if (this.subscribeTimeout) clearTimeout(this.subscribeTimeout);
    this.subscribeTimeout = null;
    this.pendingHeartbeat = null;
    const socket = this.socket;
    this.socket = null;
    if (socket) {
      socket.onopen = socket.onmessage = socket.onclose = socket.onerror = null;
      try {
        socket.close();
      } catch {
        // already closing
      }
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer) return;
    const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** Math.min(this.attempts, 6));
    this.attempts++;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  private setLive(live: boolean): void {
    if (this.live === live) return;
    this.live = live;
    this.o.onState(live);
  }
}

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
