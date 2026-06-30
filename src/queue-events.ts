import { EventEmitter } from 'events';
import type Redis from 'ioredis';
import type { QueueEventsOptions } from './types';
import { createClient } from './connection';

export class QueueEvents extends EventEmitter {
  readonly name: string;
  readonly opts: QueueEventsOptions;

  private client: Redis;
  private _closing = false;
  private lastId: string;

  constructor(name: string, opts?: QueueEventsOptions) {
    super();
    this.name = name;
    this.opts = {
      connection: { host: '127.0.0.1', port: 6379 },
      blockingTimeout: 10000,
      lastEventId: '$',
      ...opts,
    };
    this.lastId = this.opts.lastEventId ?? '$';
    this.client = createClient(this.opts.connection);
    this.client.on('error', (err) => this.emit('error', err));
  }

  get keyPrefix(): string {
    return `${(this.opts as QueueEventsOptions & { prefix?: string }).prefix ?? 'bull'}:${this.name}`;
  }

  async waitUntilReady(): Promise<Redis> {
    await this.client.ping();
    return this.client;
  }

  async run(): Promise<void> {
    await this.consumeEvents();
  }

  private async consumeEvents(): Promise<void> {
    while (!this._closing) {
      try {
        const timeout = this.opts.blockingTimeout ?? 10000;

        const result = (await this.client.xread(
          'COUNT', 100,
          'BLOCK', timeout,
          'STREAMS', `${this.keyPrefix}:events`, this.lastId,
        )) as Array<[string, Array<[string, string[]]>]> | null;

        if (!result) continue;

        for (const [, messages] of result) {
          for (const [msgId, fields] of messages) {
            this.lastId = msgId;
            const kv: Record<string, string> = {};
            for (let i = 0; i < fields.length; i += 2) {
              kv[fields[i]] = fields[i + 1];
            }

            const event = kv['event'];
            const data: Record<string, unknown> = kv['data'] ? JSON.parse(kv['data']) : {};

            if (event) {
              this.emit(event, data, msgId);
            }
          }
        }
      } catch (err) {
        if (!this._closing) {
          this.emit('error', err);
          await new Promise((r) => setTimeout(r, 1000));
        }
      }
    }
  }

  async close(): Promise<void> {
    this._closing = true;
    await this.client.quit().catch(() => {/* ignore */});
  }
}
