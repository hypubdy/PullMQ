import Redis from 'ioredis';
import type { ConnectionOptions } from './types';

export function createClient(connection: ConnectionOptions): Redis {
  if (connection instanceof Redis) {
    return connection;
  }
  return new Redis({
    host: connection.host ?? '127.0.0.1',
    port: connection.port ?? 6379,
    password: connection.password,
    db: connection.db ?? 0,
    ...(connection.tls ? { tls: connection.tls as object } : {}),
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
  });
}
