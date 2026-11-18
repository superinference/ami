import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  runtimeInstancePid,
  isPidAlive,
  instanceScopedTempName,
  parseInstanceTempPid,
} from '../src/utils/runtime-namespace';

describe('runtimeInstancePid', () => {
  it('returns this process PID', () => {
    assert.equal(runtimeInstancePid(), process.pid);
  });
});

describe('isPidAlive', () => {
  it('reports the current process as alive', () => {
    assert.equal(isPidAlive(process.pid), true);
  });

  it('reports a definitely-dead PID as not alive', () => {
    // PID 2^31-1 is effectively never a running process → ESRCH.
    assert.equal(isPidAlive(2147483647), false);
  });

  it('treats a non-ESRCH error (EPERM/other) as alive', () => {
    // PID 1 (init) exists but is typically not killable by an unprivileged
    // process → EPERM, which must be read as "alive" (never ESRCH).
    assert.equal(isPidAlive(1), true);
  });
});

describe('instanceScopedTempName', () => {
  it('embeds the current PID by default', () => {
    assert.equal(instanceScopedTempName('/a/config.json'), `/a/config.json.${process.pid}.tmp`);
  });

  it('accepts an explicit PID', () => {
    assert.equal(instanceScopedTempName('/a/config.json', 4242), '/a/config.json.4242.tmp');
  });

  it('gives two different PIDs isolated scratch names (no collision)', () => {
    assert.notEqual(
      instanceScopedTempName('/x/f', 100),
      instanceScopedTempName('/x/f', 200),
    );
  });
});

describe('parseInstanceTempPid', () => {
  it('classifies a legacy un-scoped temp as null', () => {
    assert.equal(parseInstanceTempPid('config.json.tmp', 'config.json'), null);
  });

  it('extracts the owner PID from a scoped temp', () => {
    assert.equal(parseInstanceTempPid('config.json.9876.tmp', 'config.json'), 9876);
  });

  it('returns undefined for a non-temp sibling', () => {
    assert.equal(parseInstanceTempPid('config.json', 'config.json'), undefined);
    assert.equal(parseInstanceTempPid('config.json.bak', 'config.json'), undefined);
    assert.equal(parseInstanceTempPid('other.5.tmp', 'config.json'), undefined);
  });

  it('rejects a non-numeric or zero-padded middle segment', () => {
    assert.equal(parseInstanceTempPid('config.json.abc.tmp', 'config.json'), undefined);
    assert.equal(parseInstanceTempPid('config.json.0.tmp', 'config.json'), undefined);
    assert.equal(parseInstanceTempPid('config.json.01.tmp', 'config.json'), undefined);
    assert.equal(parseInstanceTempPid('config.json.12.34.tmp', 'config.json'), undefined);
  });

  it('round-trips with instanceScopedTempName', () => {
    const name = instanceScopedTempName('config.json', 555);
    assert.equal(parseInstanceTempPid('config.json.555.tmp', 'config.json'), 555);
    assert.ok(name.endsWith('.555.tmp'));
  });
});
