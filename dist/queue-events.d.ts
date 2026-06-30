import { EventEmitter } from 'events';
import type Redis from 'ioredis';
import type { QueueEventsOptions } from './types';
export declare class QueueEvents extends EventEmitter {
    readonly name: string;
    readonly opts: QueueEventsOptions;
    private client;
    private _closing;
    private lastId;
    constructor(name: string, opts?: QueueEventsOptions);
    get keyPrefix(): string;
    waitUntilReady(): Promise<Redis>;
    run(): Promise<void>;
    private consumeEvents;
    close(): Promise<void>;
}
//# sourceMappingURL=queue-events.d.ts.map