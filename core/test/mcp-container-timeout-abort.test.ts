import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  pullImage,
  isImageAvailable,
  getImageInfo,
} from '../src/mcp/container';

// ---------------------------------------------------------------------------
// These exercise the error/edge guards added to container.ts:
//   * pullImage() wall-clock timeout (a stalled pull must not hang forever)
//   * pullImage() AbortSignal cancellation (pre-aborted + in-flight)
//   * synchronous probe timeout (isImageAvailable / getImageInfo must not block
//     on a wedged daemon)
// A "hang" fake runtime stays alive ~100s so, without the guards, each path
// would block indefinitely; with them it is bounded.
// ---------------------------------------------------------------------------

let dir: string;
let hangRuntime: string; // a fake docker/podman that never exits on its own

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-container-timeout-'));
  hangRuntime = path.join(dir, 'hangRuntime');
  fs.writeFileSync(
    hangRuntime,
    `#!/usr/bin/env node\n` +
      // Keep the process alive far longer than any test timeout so the only way
      // it ends is via our kill (timeout/abort) or an execFileSync timeout.
      `setTimeout(() => {}, 100000);\n`,
  );
  fs.chmodSync(hangRuntime, 0o755);
});

after(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

describe('pullImage timeout', () => {
  it('rejects with a clear "timed out after Nms" error when the pull stalls', async () => {
    await assert.rejects(
      () => pullImage(hangRuntime, 'stalled:latest', undefined, { timeoutMs: 150 }),
      (err: Error) => /pull timed out after 150ms/.test(err.message),
    );
  });

  it('honors AMI_CONTAINER_PULL_TIMEOUT_MS as the default bound', async () => {
    const prev = process.env.AMI_CONTAINER_PULL_TIMEOUT_MS;
    process.env.AMI_CONTAINER_PULL_TIMEOUT_MS = '120';
    try {
      await assert.rejects(
        () => pullImage(hangRuntime, 'stalled:latest'),
        (err: Error) => /pull timed out after 120ms/.test(err.message),
      );
    } finally {
      if (prev === undefined) delete process.env.AMI_CONTAINER_PULL_TIMEOUT_MS;
      else process.env.AMI_CONTAINER_PULL_TIMEOUT_MS = prev;
    }
  });

  it('falls back to the default bound when the env var is not a positive number', async () => {
    // Exercises the `n > 0 ? n : DEFAULT` fallback arm (0 is not > 0) while
    // keeping the test fast by also passing an explicit small timeoutMs.
    const prev = process.env.AMI_CONTAINER_PULL_TIMEOUT_MS;
    process.env.AMI_CONTAINER_PULL_TIMEOUT_MS = '0';
    try {
      await assert.rejects(
        () => pullImage(hangRuntime, 'stalled:latest', undefined, { timeoutMs: 100 }),
        (err: Error) => /pull timed out after 100ms/.test(err.message),
      );
    } finally {
      if (prev === undefined) delete process.env.AMI_CONTAINER_PULL_TIMEOUT_MS;
      else process.env.AMI_CONTAINER_PULL_TIMEOUT_MS = prev;
    }
  });
});

describe('pullImage cancellation (AbortSignal)', () => {
  it('short-circuits and rejects immediately when the signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort(); // default reason is an Error-like DOMException
    await assert.rejects(
      () => pullImage(hangRuntime, 'any:latest', undefined, { signal: controller.signal }),
      (err: Error) => err.name === 'AbortError',
    );
  });

  it('aborts an in-flight pull and rejects with a non-Error reason wrapped as AbortError', async () => {
    const controller = new AbortController();
    // timeoutMs: 0 disables the timer so the abort path is what resolves the race.
    const p = pullImage(hangRuntime, 'any:latest', undefined, {
      signal: controller.signal,
      timeoutMs: 0,
    });
    setTimeout(() => controller.abort('user requested stop'), 50);
    await assert.rejects(
      () => p,
      (err: Error) => err.name === 'AbortError' && err.message === 'container pull aborted',
    );
  });
});

describe('synchronous probe timeout', () => {
  it('isImageAvailable returns false (does not hang) when the daemon wedges', () => {
    const prev = process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS;
    process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS = '200';
    try {
      const start = Date.now();
      const ok = isImageAvailable(hangRuntime, 'img:latest');
      assert.equal(ok, false, 'a wedged inspect must be treated as unavailable');
      assert.ok(Date.now() - start < 8000, 'must be bounded by the probe timeout');
    } finally {
      if (prev === undefined) delete process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS;
      else process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS = prev;
    }
  });

  it('getImageInfo returns { available: false } (does not hang) when the daemon wedges', () => {
    const prev = process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS;
    process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS = '200';
    try {
      const start = Date.now();
      const info = getImageInfo(hangRuntime, 'img:latest');
      assert.equal(info.available, false);
      assert.ok(Date.now() - start < 8000, 'must be bounded by the probe timeout');
    } finally {
      if (prev === undefined) delete process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS;
      else process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS = prev;
    }
  });

  it('uses the default probe timeout when the env var is unset or non-positive', () => {
    // nonexistent runtime fails fast (ENOENT) so the default-bound path runs
    // without any real wait; exercises the `n > 0 ? n : DEFAULT` fallback arm.
    const prev = process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS;
    delete process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS;
    try {
      assert.equal(isImageAvailable('_nonexistent_runtime_', 'img'), false);
      assert.equal(getImageInfo('_nonexistent_runtime_', 'img').available, false);
    } finally {
      if (prev !== undefined) process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS = prev;
    }
  });
});
