"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WaitingChildrenError = exports.UnrecoverableError = void 0;
class UnrecoverableError extends Error {
    constructor(message) {
        super(message);
        this.name = 'UnrecoverableError';
    }
}
exports.UnrecoverableError = UnrecoverableError;
class WaitingChildrenError extends Error {
    constructor() {
        super('Job is waiting for children');
        this.name = 'WaitingChildrenError';
    }
}
exports.WaitingChildrenError = WaitingChildrenError;
//# sourceMappingURL=errors.js.map