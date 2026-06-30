export class UnrecoverableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UnrecoverableError';
  }
}

export class WaitingChildrenError extends Error {
  constructor() {
    super('Job is waiting for children');
    this.name = 'WaitingChildrenError';
  }
}

export class GroupMaxSizeExceededError extends Error {
  constructor(groupId: string, maxSize: number) {
    super(`Group ${groupId} has reached its maximum size of ${maxSize}`);
    this.name = 'GroupMaxSizeExceededError';
  }
}

export class GroupRateLimitError extends Error {
  constructor() {
    super('Group rate limit exceeded');
    this.name = 'GroupRateLimitError';
  }
}
