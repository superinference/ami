import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  CODE_LOADING_VARIABLES,
  POLICY_SOURCE_VARIABLES,
  detectCodeLoadingEnvSet,
  detectBashArgv0Assignment,
} from '../src/tools/bash-security';

describe('CODE_LOADING_VARIABLES', () => {
  it('contains all expected variables', () => {
    assert.ok(CODE_LOADING_VARIABLES.includes('LD_PRELOAD'));
    assert.ok(CODE_LOADING_VARIABLES.includes('LD_LIBRARY_PATH'));
    assert.ok(CODE_LOADING_VARIABLES.includes('LD_AUDIT'));
    assert.ok(CODE_LOADING_VARIABLES.includes('DYLD_INSERT_LIBRARIES'));
    assert.ok(CODE_LOADING_VARIABLES.includes('DYLD_LIBRARY_PATH'));
    assert.ok(CODE_LOADING_VARIABLES.includes('DYLD_FRAMEWORK_PATH'));
    assert.ok(CODE_LOADING_VARIABLES.includes('BASH_ENV'));
    assert.ok(CODE_LOADING_VARIABLES.includes('ENV'));
    assert.ok(CODE_LOADING_VARIABLES.includes('ZDOTDIR'));
    assert.ok(CODE_LOADING_VARIABLES.includes('NODE_OPTIONS'));
    assert.equal(CODE_LOADING_VARIABLES.length, 10);
  });
});

describe('POLICY_SOURCE_VARIABLES', () => {
  it('contains expected variables', () => {
    assert.ok(POLICY_SOURCE_VARIABLES.includes('SUPERINFERENCE_MANAGED_SETTINGS_PATH'));
    assert.ok(POLICY_SOURCE_VARIABLES.includes('SUPERINFERENCE_CONFIG_DIR'));
    assert.equal(POLICY_SOURCE_VARIABLES.length, 2);
  });
});

describe('detectCodeLoadingEnvSet', () => {
  describe('export VAR=value', () => {
    for (const v of CODE_LOADING_VARIABLES) {
      it(`blocks export ${v}=value`, () => {
        const r = detectCodeLoadingEnvSet(`export ${v}=/evil/path`);
        assert.ok(r.blocked, `expected ${v} to be blocked`);
        assert.equal(r.variable, v);
      });
    }

    for (const v of POLICY_SOURCE_VARIABLES) {
      it(`blocks export ${v}=value`, () => {
        const r = detectCodeLoadingEnvSet(`export ${v}=/evil/path`);
        assert.ok(r.blocked, `expected ${v} to be blocked`);
        assert.equal(r.variable, v);
      });
    }
  });

  describe('VAR=val command (inline)', () => {
    it('blocks LD_PRELOAD=evil.so python3 script.py', () => {
      const r = detectCodeLoadingEnvSet('LD_PRELOAD=evil.so python3 script.py');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'LD_PRELOAD');
    });

    it('blocks NODE_OPTIONS=--require=evil.js node app.js', () => {
      const r = detectCodeLoadingEnvSet('NODE_OPTIONS=--require=evil.js node app.js');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'NODE_OPTIONS');
    });
  });

  describe('env VAR=val command', () => {
    it('blocks env LD_PRELOAD=evil.so cmd', () => {
      const r = detectCodeLoadingEnvSet('env LD_PRELOAD=evil.so /usr/bin/myapp');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'LD_PRELOAD');
    });

    it('blocks env with flags: env -i LD_AUDIT=evil.so cmd', () => {
      const r = detectCodeLoadingEnvSet('env -i LD_AUDIT=evil.so cmd');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'LD_AUDIT');
    });
  });

  describe('declare -x VAR=val', () => {
    it('blocks declare -x LD_LIBRARY_PATH=/evil', () => {
      const r = detectCodeLoadingEnvSet('declare -x LD_LIBRARY_PATH=/evil');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'LD_LIBRARY_PATH');
    });

    it('blocks declare -x BASH_ENV=/evil', () => {
      const r = detectCodeLoadingEnvSet('declare -x BASH_ENV=/tmp/evil.sh');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'BASH_ENV');
    });
  });

  describe('unset VAR', () => {
    it('blocks unset LD_PRELOAD', () => {
      const r = detectCodeLoadingEnvSet('unset LD_PRELOAD');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'LD_PRELOAD');
    });

    it('blocks unset NODE_OPTIONS', () => {
      const r = detectCodeLoadingEnvSet('unset NODE_OPTIONS');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'NODE_OPTIONS');
    });
  });

  describe('case-insensitivity', () => {
    it('blocks ld_preload (lowercase)', () => {
      const r = detectCodeLoadingEnvSet('export ld_preload=/evil');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'ld_preload');
    });

    it('blocks Ld_Preload (mixed case)', () => {
      const r = detectCodeLoadingEnvSet('export Ld_Preload=/evil');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'Ld_Preload');
    });

    it('blocks node_options (lowercase)', () => {
      const r = detectCodeLoadingEnvSet('export node_options=--evil');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'node_options');
    });
  });

  describe('safe variables are NOT blocked', () => {
    const safeVars = ['PATH', 'HOME', 'TERM', 'EDITOR', 'VISUAL', 'LANG',
      'LC_ALL', 'USER', 'SHELL', 'TMPDIR', 'GOPATH', 'JAVA_HOME', 'PYTHONPATH'];

    for (const v of safeVars) {
      it(`allows export ${v}=value`, () => {
        const r = detectCodeLoadingEnvSet(`export ${v}=/some/path`);
        assert.ok(!r.blocked, `expected ${v} to be allowed`);
      });
    }
  });

  describe('substring false positives', () => {
    it('does not block MY_LD_PRELOAD', () => {
      const r = detectCodeLoadingEnvSet('export MY_LD_PRELOAD=foo');
      assert.ok(!r.blocked);
    });

    it('does not block LD_PRELOAD_EXTRA', () => {
      const r = detectCodeLoadingEnvSet('export LD_PRELOAD_EXTRA=foo');
      assert.ok(!r.blocked);
    });

    it('does not block NODE_OPTIONS_EXTRA', () => {
      const r = detectCodeLoadingEnvSet('export NODE_OPTIONS_EXTRA=foo');
      assert.ok(!r.blocked);
    });
  });

  describe('safe commands that mention variables', () => {
    it('allows echo $LD_PRELOAD', () => {
      const r = detectCodeLoadingEnvSet('echo $LD_PRELOAD');
      assert.ok(!r.blocked);
    });

    it('allows grep LD_PRELOAD /proc/self/maps', () => {
      const r = detectCodeLoadingEnvSet('grep LD_PRELOAD /proc/self/maps');
      assert.ok(!r.blocked);
    });

    it('allows env | grep NODE_OPTIONS', () => {
      const r = detectCodeLoadingEnvSet('env | grep NODE_OPTIONS');
      assert.ok(!r.blocked);
    });
  });

  describe('compound commands', () => {
    it('blocks export LD_PRELOAD in a chain: cd /tmp && export LD_PRELOAD=evil', () => {
      const r = detectCodeLoadingEnvSet('cd /tmp && export LD_PRELOAD=evil.so');
      assert.ok(r.blocked);
      assert.equal(r.variable, 'LD_PRELOAD');
    });
  });
});

describe('bash tool blocks code-loading env set', () => {
  it('returns error for LD_PRELOAD export', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const ctx = { cwd: process.cwd(), abortSignal: new AbortController().signal };
    const result = await bashTool.execute({ command: 'export LD_PRELOAD=/evil.so' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('LD_PRELOAD'));
    assert.ok(result.output.includes('blocked'));
  });

  it('returns error for NODE_OPTIONS inline', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const ctx = { cwd: process.cwd(), abortSignal: new AbortController().signal };
    const result = await bashTool.execute({ command: 'NODE_OPTIONS=--require=evil node app.js' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('NODE_OPTIONS'));
    assert.ok(result.output.includes('blocked'));
  });

  it('allows safe commands through', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const ctx = { cwd: process.cwd(), abortSignal: new AbortController().signal };
    const result = await bashTool.execute({ command: 'echo hello' }, ctx);
    assert.ok(!result.isError);
  });
});

// ---------------------------------------------------------------------------
// BASH_ARGV0 assignment detection
// ---------------------------------------------------------------------------

describe('detectBashArgv0Assignment', () => {
  it('blocks BASH_ARGV0= prefix assignment', () => {
    const result = detectBashArgv0Assignment('BASH_ARGV0=httpd /bin/sh');
    assert.ok(result.blocked);
    assert.ok(result.reason!.includes('BASH_ARGV0'));
  });

  it('blocks BASH_ARGV0 with export', () => {
    const result = detectBashArgv0Assignment('export BASH_ARGV0=sshd');
    assert.ok(result.blocked);
  });

  it('blocks BASH_ARGV0 with spaces around =', () => {
    const result = detectBashArgv0Assignment('BASH_ARGV0 = legit_name bash evil.sh');
    assert.ok(result.blocked);
  });

  it('allows commands without BASH_ARGV0', () => {
    const result = detectBashArgv0Assignment('echo hello world');
    assert.ok(!result.blocked);
  });

  it('allows reading BASH_ARGV0 (no assignment)', () => {
    const result = detectBashArgv0Assignment('echo $BASH_ARGV0');
    assert.ok(!result.blocked);
  });
});

describe('bash tool blocks BASH_ARGV0 assignment', () => {
  it('returns error for BASH_ARGV0 assignment', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const ctx = { cwd: process.cwd(), abortSignal: new AbortController().signal };
    const result = await bashTool.execute(
      { command: 'BASH_ARGV0=httpd /bin/sh -c "echo hi"' },
      ctx,
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('BASH_ARGV0'));
  });

  it('allows referencing BASH_ARGV0 in echo', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const ctx = { cwd: process.cwd(), abortSignal: new AbortController().signal };
    const result = await bashTool.execute(
      { command: 'echo "argv0 is $BASH_ARGV0"' },
      ctx,
    );
    assert.ok(!result.isError);
  });
});
