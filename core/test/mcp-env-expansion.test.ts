import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { expandEnvVars, expandEnvVarsTracked } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// H39: ${VAR:-default} default-value syntax in MCP env expansion.
// H35: missing-variable tracking for diagnostics.
// ---------------------------------------------------------------------------

const TOUCHED = ['__EE_SET', '__EE_EMPTY', '__EE_MISS'];
afterEach(() => { for (const k of TOUCHED) delete process.env[k]; });

describe('expandEnvVars defaults (H39)', () => {
  it('uses the env value when the variable is set, ignoring the default', () => {
    process.env.__EE_SET = 'real';
    assert.equal(expandEnvVars('${__EE_SET:-fallback}'), 'real');
  });

  it('uses the default when the variable is undefined', () => {
    assert.equal(expandEnvVars('${__EE_MISS:-fallback}'), 'fallback');
  });

  it('uses an EMPTY env value over the default (set-but-empty is still "set")', () => {
    process.env.__EE_EMPTY = '';
    assert.equal(expandEnvVars('${__EE_EMPTY:-fallback}'), '');
  });

  it('supports an empty default', () => {
    assert.equal(expandEnvVars('${__EE_MISS:-}'), '');
  });

  it('keeps a `:-` that appears inside the default value', () => {
    assert.equal(expandEnvVars('${__EE_MISS:-a:-b}'), 'a:-b');
  });

  it('expands plain ${VAR} and bare $VAR, preserving the original token for missing ones (Claude-parity)', () => {
    process.env.__EE_SET = 'x';
    // Unresolved references keep their literal token so the broken placeholder
    // stays visible for debugging instead of collapsing to ''.
    assert.equal(expandEnvVars('${__EE_SET}-$__EE_SET-${__EE_MISS}'), 'x-x-${__EE_MISS}');
    assert.equal(expandEnvVars('$__EE_MISS'), '$__EE_MISS');
  });
});

describe('expandEnvVarsTracked missing-var reporting (H35)', () => {
  it('reports only variables that are undefined and have no default', () => {
    process.env.__EE_SET = 'x';
    const r = expandEnvVarsTracked('${__EE_SET}/${__EE_MISS}/${__EE_SET:-d}/${__EE_MISS:-d}');
    // The unresolved `${__EE_MISS}` keeps its literal token (Claude-parity); the
    // `${__EE_MISS:-d}` resolves to its default and is not reported missing.
    assert.equal(r.expanded, 'x/${__EE_MISS}/x/d');
    assert.deepEqual(r.missingVars, ['__EE_MISS']);
  });

  it('reports a missing bare variable', () => {
    const r = expandEnvVarsTracked('$__EE_MISS');
    assert.deepEqual(r.missingVars, ['__EE_MISS']);
  });

  it('reports nothing when all references resolve', () => {
    process.env.__EE_SET = 'x';
    assert.deepEqual(expandEnvVarsTracked('${__EE_SET}').missingVars, []);
  });
});
