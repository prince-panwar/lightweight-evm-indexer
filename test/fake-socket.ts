import type { SocketLike } from '../src/trigger/log-stream.js';

/** An in-memory WebSocket the tests drive by hand. */
export class FakeSocket implements SocketLike {
  onopen: ((ev?: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev?: unknown) => void) | null = null;
  onerror: ((ev?: unknown) => void) | null = null;
  sent: { id: number; method: string; params?: unknown }[] = [];
  closed = false;

  constructor(readonly url: string) {}

  send(data: string): void {
    this.sent.push(JSON.parse(data));
  }
  close(): void {
    this.closed = true;
  }
  open(): void {
    this.onopen?.();
  }
  reply(msg: unknown): void {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }
  drop(): void {
    this.onclose?.();
  }
  /** Open and confirm the subscription. */
  subscribe(): void {
    this.open();
    this.reply({ jsonrpc: '2.0', id: 1, result: '0xsub' });
  }
  notify(log: Record<string, unknown>): void {
    this.reply({ jsonrpc: '2.0', method: 'eth_subscription', params: { subscription: '0xsub', result: log } });
  }
}
