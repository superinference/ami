import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectToolConflict,
  ToolRegistryGuard,
} from '../src/tools/tool-registry-guard';
import {
  RESERVED_PREFIXES_ENV,
  TRUSTED_SOURCES_ENV,
  parseToolRegistryGuardOptions,
} from '../src/tools/tool-gates';
import { createDefaultTools } from '../src/tools/index';
import type { ToolDefinition } from '../src/types';

// U275 gap (configurable registration gate): operator-configurable extra reserved
// prefixes + trusted sources threaded through detectToolConflict / ToolRegistryGuard /
// ToolRegistry, wired from AMI_RESERVED_TOOL_PREFIXES / AMI_TRUSTED_TOOL_SOURCES.

describe('detectToolConflict — extraReservedPrefixes', () => {
  it('rejects a non-core tool claiming an operator-reserved extra prefix', () => {
    // RED on revert: without the options arg the extra prefix is not reserved and this
    // returns null (the tool would be allowed to shadow the org namespace).
    const conflict = detectToolConflict('acme_deploy', 'plugin-a', new Map(), {
      extraReservedPrefixes: ['acme_'],
    });
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix "acme_"'));
    assert.equal(conflict.existingSource, 'core');
  });

  it('still allows a non-matching name with extra prefixes configured', () => {
    assert.equal(
      detectToolConflict('widget_build', 'plugin-a', new Map(), {
        extraReservedPrefixes: ['acme_'],
      }),
      null,
    );
  });

  it('canonicalizes the extra prefix (case / doubled underscore)', () => {
    const base = { extraReservedPrefixes: ['Acme_'] };
    assert.ok(detectToolConflict('acme_run', 'plugin-a', new Map(), base) !== null);
    assert.ok(detectToolConflict('ACME__run', 'plugin-a', new Map(), base) !== null);
    // exact canonical match (no suffix) also reserved
    assert.ok(detectToolConflict('acme', 'plugin-a', new Map(), { extraReservedPrefixes: ['acme'] }) !== null);
  });

  it('ignores blank / underscore-only extra prefixes', () => {
    assert.equal(
      detectToolConflict('anything', 'plugin-a', new Map(), {
        extraReservedPrefixes: ['', '  ', '_', '__'],
      }),
      null,
    );
  });

  it('built-in reserved prefixes stay enforced alongside the extras', () => {
    const conflict = detectToolConflict('bash', 'plugin-a', new Map(), {
      extraReservedPrefixes: ['acme_'],
    });
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix "bash"'));
  });
});

describe('detectToolConflict — trustedSources', () => {
  it('lets a trusted source claim a reserved prefix a normal source cannot', () => {
    assert.ok(detectToolConflict('bash', 'firstparty', new Map()) !== null);
    // RED on revert: trustedSources is ignored → this stays a conflict.
    assert.equal(
      detectToolConflict('bash', 'firstparty', new Map(), {
        trustedSources: ['firstparty'],
      }),
      null,
    );
  });

  it('accepts trustedSources as a Set as well as an array', () => {
    assert.equal(
      detectToolConflict('grep', 'firstparty', new Map(), {
        trustedSources: new Set(['firstparty']),
      }),
      null,
    );
  });

  it('an untrusted source is still blocked when others are trusted', () => {
    const conflict = detectToolConflict('bash', 'other', new Map(), {
      trustedSources: ['firstparty'],
    });
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix'));
  });

  it('core/builtin remain trusted implicitly even with a trusted set configured', () => {
    assert.equal(
      detectToolConflict('bash', 'core', new Map(), { trustedSources: ['firstparty'] }),
      null,
    );
  });
});

describe('ToolRegistryGuard — options via constructor', () => {
  it('register honors extraReservedPrefixes and records the conflict', () => {
    const guard = new ToolRegistryGuard({ extraReservedPrefixes: ['acme_'] });
    const conflict = guard.register('acme_x', 'plugin-a');
    assert.ok(conflict !== null);
    assert.equal(guard.isRegistered('acme_x'), false);
    assert.equal(guard.getConflicts().length, 1);
  });

  it('register honors trustedSources (reserved prefix allowed for trusted source)', () => {
    const guard = new ToolRegistryGuard({ trustedSources: ['firstparty'] });
    assert.equal(guard.register('bash', 'firstparty'), null);
    assert.equal(guard.isRegistered('bash'), true);
  });

  it('an alias that hits an extra reserved prefix is rejected for a non-trusted source', () => {
    const guard = new ToolRegistryGuard({ extraReservedPrefixes: ['acme_'] });
    const conflict = guard.register('my_tool', 'plugin-a', ['acme_alias']);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix "acme_"'));
    // Nothing half-registered.
    assert.equal(guard.isRegistered('my_tool'), false);
  });

  it('a trusted source may register an alias on a reserved prefix', () => {
    const guard = new ToolRegistryGuard({ trustedSources: ['firstparty'] });
    assert.equal(guard.register('my_tool', 'firstparty', ['bash_legacy']), null);
    assert.equal(guard.resolveToolName('bash_legacy'), 'my_tool');
  });

  it('default (no options) behaves exactly as before', () => {
    const guard = new ToolRegistryGuard();
    assert.ok(guard.register('bash', 'plugin-a') !== null);
    assert.equal(guard.register('my_tool', 'plugin-a'), null);
  });
});

describe('parseToolRegistryGuardOptions', () => {
  it('returns {} when neither env var is set', () => {
    assert.deepEqual(parseToolRegistryGuardOptions({}), {});
  });

  it('parses, trims and de-dups the reserved-prefix list', () => {
    const opts = parseToolRegistryGuardOptions({
      [RESERVED_PREFIXES_ENV]: ' acme_ , beta_ ,, ',
    });
    assert.deepEqual(opts.extraReservedPrefixes, ['acme_', 'beta_']);
    assert.equal(opts.trustedSources, undefined);
  });

  it('parses trusted sources into a Set', () => {
    const opts = parseToolRegistryGuardOptions({
      [TRUSTED_SOURCES_ENV]: 'firstparty, verified',
    });
    assert.ok(opts.trustedSources instanceof Set);
    assert.deepEqual([...(opts.trustedSources as Set<string>)].sort(), ['firstparty', 'verified']);
    assert.equal(opts.extraReservedPrefixes, undefined);
  });
});

describe('createDefaultTools e2e — registration-gate knobs from env', () => {
  afterEach(() => {
    delete process.env[RESERVED_PREFIXES_ENV];
    delete process.env[TRUSTED_SOURCES_ENV];
  });

  const fakeTool = (name: string): ToolDefinition => ({
    name,
    description: 'x',
    inputSchema: { type: 'object', properties: {} },
    isReadOnly: true,
    execute: async () => ({ output: '', isError: false }),
  });

  it('a non-core tool using an operator-reserved prefix is blocked at the gate', () => {
    process.env[RESERVED_PREFIXES_ENV] = 'acme_';
    const registry = createDefaultTools('/tmp');
    registry.register(fakeTool('acme_deploy'), 'plugin-a');
    assert.equal(registry.get('acme_deploy'), undefined);
    assert.ok(registry.getConflicts().some((c) => c.toolName === 'acme_deploy'));
  });

  it('a trusted source may claim a reserved prefix through the default registry', () => {
    process.env[TRUSTED_SOURCES_ENV] = 'firstparty';
    const registry = createDefaultTools('/tmp');
    registry.register(fakeTool('bash_plus'), 'firstparty');
    assert.ok(registry.get('bash_plus') !== undefined);
  });

  it('with no env set, a non-core reserved-prefix tool is still blocked (default)', () => {
    const registry = createDefaultTools('/tmp');
    registry.register(fakeTool('acme_deploy'), 'plugin-a');
    // acme_ is not reserved by default, so it registers fine …
    assert.ok(registry.get('acme_deploy') !== undefined);
    // … while a built-in reserved prefix is still blocked.
    registry.register(fakeTool('bash_x'), 'plugin-b');
    assert.equal(registry.get('bash_x'), undefined);
  });
});
