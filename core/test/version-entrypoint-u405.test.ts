import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  amiUserAgent,
  resolveEntrypoint,
  AMI_VERSION,
  AMI_CLIENT_NAME,
} from '../src/version';
import { getMcpUserAgent } from '../src/mcp/client';
import { resolveClientSurface } from '../src/tools/git-commit';

// ---------------------------------------------------------------------------
// U405 gap 2: entrypoint / client-app knobs in the client identity so MCP and
// HTTP peers can tell which surface (TUI, detached CLI, MCP, an embedding SDK/
// host app) originated a request. Parity with Claude Code's getMCPUserAgent /
// getUserAgent (src/utils/http.ts), which append CLAUDE_CODE_ENTRYPOINT,
// agent-sdk/<ver> and client-app/<id> to the UA.
// ---------------------------------------------------------------------------

describe('resolveEntrypoint — single entrypoint knob reader', () => {
  it('returns undefined when neither env var is set', () => {
    assert.equal(resolveEntrypoint({}), undefined);
  });

  it('reads AMI_ENTRYPOINT', () => {
    assert.equal(resolveEntrypoint({ AMI_ENTRYPOINT: 'vscode' }), 'vscode');
  });

  it('honors CLAUDE_CODE_ENTRYPOINT for cross-tool compatibility', () => {
    assert.equal(resolveEntrypoint({ CLAUDE_CODE_ENTRYPOINT: 'sdk-cli' }), 'sdk-cli');
  });

  it('AMI_ENTRYPOINT wins over CLAUDE_CODE_ENTRYPOINT', () => {
    assert.equal(
      resolveEntrypoint({ AMI_ENTRYPOINT: 'mcp', CLAUDE_CODE_ENTRYPOINT: 'cli' }),
      'mcp',
    );
  });

  it('treats a blank/whitespace value as unset', () => {
    assert.equal(resolveEntrypoint({ AMI_ENTRYPOINT: '   ' }), undefined);
  });

  it('trims surrounding whitespace', () => {
    assert.equal(resolveEntrypoint({ AMI_ENTRYPOINT: '  gha  ' }), 'gha');
  });
});

describe('amiUserAgent — surface-identifying User-Agent builder', () => {
  it('is the bare ami/<version> when nothing is set', () => {
    assert.equal(amiUserAgent(undefined, {}), `${AMI_CLIENT_NAME}/${AMI_VERSION}`);
  });

  it('appends only a free-form component when given', () => {
    assert.equal(
      amiUserAgent('skill-discovery', {}),
      `ami/${AMI_VERSION} (skill-discovery)`,
    );
  });

  it('appends the originating entrypoint', () => {
    assert.equal(
      amiUserAgent(undefined, { AMI_ENTRYPOINT: 'vscode' }),
      `ami/${AMI_VERSION} (vscode)`,
    );
  });

  it('appends agent-sdk/<ver> from AMI_AGENT_SDK_VERSION', () => {
    assert.equal(
      amiUserAgent(undefined, { AMI_AGENT_SDK_VERSION: '2.1.0' }),
      `ami/${AMI_VERSION} (agent-sdk/2.1.0)`,
    );
  });

  it('honors Claude CLAUDE_AGENT_SDK_VERSION as a fallback', () => {
    assert.equal(
      amiUserAgent(undefined, { CLAUDE_AGENT_SDK_VERSION: '3.0' }),
      `ami/${AMI_VERSION} (agent-sdk/3.0)`,
    );
  });

  it('appends client-app/<id> from AMI_AGENT_SDK_CLIENT_APP', () => {
    assert.equal(
      amiUserAgent(undefined, { AMI_AGENT_SDK_CLIENT_APP: 'my-app/1.0' }),
      `ami/${AMI_VERSION} (client-app/my-app/1.0)`,
    );
  });

  it('honors Claude CLAUDE_AGENT_SDK_CLIENT_APP as a fallback', () => {
    assert.equal(
      amiUserAgent(undefined, { CLAUDE_AGENT_SDK_CLIENT_APP: 'host/9' }),
      `ami/${AMI_VERSION} (client-app/host/9)`,
    );
  });

  it('composes component, entrypoint, agent-sdk and client-app in order', () => {
    assert.equal(
      amiUserAgent('skill-discovery', {
        AMI_ENTRYPOINT: 'mcp',
        AMI_AGENT_SDK_VERSION: '2.1.0',
        AMI_AGENT_SDK_CLIENT_APP: 'embed/4',
      }),
      `ami/${AMI_VERSION} (skill-discovery, mcp, agent-sdk/2.1.0, client-app/embed/4)`,
    );
  });

  it('ignores blank surface knobs', () => {
    assert.equal(
      amiUserAgent(undefined, { AMI_ENTRYPOINT: '  ', AMI_AGENT_SDK_VERSION: '' }),
      `ami/${AMI_VERSION}`,
    );
  });

  it('prefers the AMI knob over the CLAUDE fallback', () => {
    assert.equal(
      amiUserAgent(undefined, {
        AMI_AGENT_SDK_VERSION: 'a',
        CLAUDE_AGENT_SDK_VERSION: 'b',
      }),
      `ami/${AMI_VERSION} (agent-sdk/a)`,
    );
  });
});

describe('getMcpUserAgent — wired to the surface-aware builder', () => {
  function withEnv(vars: Record<string, string>, fn: () => void): void {
    const keys = ['AMI_ENTRYPOINT', 'CLAUDE_CODE_ENTRYPOINT', 'AMI_AGENT_SDK_VERSION', 'AMI_AGENT_SDK_CLIENT_APP', 'CLAUDE_AGENT_SDK_VERSION', 'CLAUDE_AGENT_SDK_CLIENT_APP'];
    const prev = new Map<string, string | undefined>();
    for (const k of keys) { prev.set(k, process.env[k]); delete process.env[k]; }
    try {
      for (const [k, v] of Object.entries(vars)) process.env[k] = v;
      fn();
    } finally {
      for (const k of keys) {
        const p = prev.get(k);
        if (p === undefined) delete process.env[k];
        else process.env[k] = p;
      }
    }
  }

  it('is the bare ami/<version> with no surface env set', () => {
    withEnv({}, () => {
      assert.equal(getMcpUserAgent(), `ami/${AMI_VERSION}`);
    });
  });

  it('surfaces the entrypoint + embedding SDK in the MCP UA', () => {
    withEnv({ AMI_ENTRYPOINT: 'vscode', AMI_AGENT_SDK_VERSION: '2.1.0' }, () => {
      assert.equal(getMcpUserAgent(), `ami/${AMI_VERSION} (vscode, agent-sdk/2.1.0)`);
    });
  });
});

describe('resolveClientSurface — consolidated on resolveEntrypoint', () => {
  it('defaults to cli when no entrypoint is set', () => {
    assert.equal(resolveClientSurface({}), 'cli');
  });

  it('reflects the same entrypoint knob as the UA', () => {
    const env = { AMI_ENTRYPOINT: 'sdk' };
    assert.equal(resolveClientSurface(env), resolveEntrypoint(env));
  });
});
