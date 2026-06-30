"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueueEvents = void 0;
const events_1 = require("events");
const connection_1 = require("./connection");
class QueueEvents extends events_1.EventEmitter {
    constructor(name, opts) {
        super();
        this._closing = false;
        this.name = name;
        this.opts = {
            connection: { host: '127.0.0.1', port: 6379 },
            blockingTimeout: 10000,
            lastEventId: '$',
            ...opts,
        };
        this.lastId = this.opts.lastEventId ?? '$';
        this.client = (0, connection_1.createClient)(this.opts.connection);
        this.client.on('error', (err) => this.emit('error', err));
    }
    get keyPrefix() {
        return `${this.opts.prefix ?? 'bull'}:${this.name}`;
    }
    async waitUntilReady() {
        await this.client.ping();
        return this.client;
    }
    async run() {
        await this.consumeEvents();
    }
    async consumeEvents() {
        while (!this._closing) {
            try {
                const timeout = this.opts.blockingTimeout ?? 10000;
                const result = (await this.client.xread('COUNT', 100, 'BLOCK', timeout, 'STREAMS', `${this.keyPrefix}:events`, this.lastId));
                if (!result)
                    continue;
                for (const [, messages] of result) {
                    for (const [msgId, fields] of messages) {
                        this.lastId = msgId;
                        const kv = {};
                        for (let i = 0; i < fields.length; i += 2) {
                            kv[fields[i]] = fields[i + 1];
                        }
                        const event = kv['event'];
                        const data = kv['data'] ? JSON.parse(kv['data']) : {};
                        if (event) {
                            this.emit(event, data, msgId);
                        }
                    }
                }
            }
            catch (err) {
                if (!this._closing) {
                    this.emit('error', err);
                    await new Promise((r) => setTimeout(r, 1000));
                }
            }
        }
    }
    async close() {
        this._closing = true;
        await this.client.quit().catch(() => { });
    }
}
exports.QueueEvents = QueueEvents;
//# sourceMappingURL=queue-events.js.map