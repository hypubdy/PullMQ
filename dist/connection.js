"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createClient = createClient;
const ioredis_1 = __importDefault(require("ioredis"));
function createClient(connection) {
    if (connection instanceof ioredis_1.default) {
        return connection;
    }
    return new ioredis_1.default({
        host: connection.host ?? '127.0.0.1',
        port: connection.port ?? 6379,
        password: connection.password,
        db: connection.db ?? 0,
        ...(connection.tls ? { tls: connection.tls } : {}),
        maxRetriesPerRequest: null,
        enableReadyCheck: false,
    });
}
//# sourceMappingURL=connection.js.map