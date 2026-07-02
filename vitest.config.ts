import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    testTimeout: 30000,
    hookTimeout: 10000,
    // All test files share one Redis instance and call FLUSHALL between tests,
    // so files must never run concurrently against it.
    fileParallelism: false,
  },
})
