import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  PermissionManager,
  PolicyMemo,
  verdictRank,
  stripSafeEnvVars,
  containsUnquotedExpansion,
  containsShellSubstitution,
  detectCommandChaining,
  detectHardlineCommand,
} from '../src/permissions';
import type { PermissionRule } from '../src/permissions';

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-perm-w1-'));
}

// ---------------------------------------------------------------------------
// verdictRank — the 'modify' arm (ranks equal to 'ask' = 2)
// ---------------------------------------------------------------------------

describe('verdictRank — modify arm', () => {
  it("ranks 'modify' as 2 (same tier as ask)", () => {
    assert.equal(verdictRank('modify'), 2);
    // Must be distinct from allow(1) and deny(3), proving the dedicated arm fired
    assert.ok(verdictRank('modify') > verdictRank('allow'));
    assert.ok(verdictRank('modify') < verdictRank('deny'));
  });
});

// ---------------------------------------------------------------------------
// validateCommandFlags — flag loop + blocked-flag rejection
// (exercised through classifyBashCommand for SAFE_COMMANDS with flag defs)
// ---------------------------------------------------------------------------

describe('classifyBashCommand — SAFE_COMMAND flag validation', () => {
  const pm = new PermissionManager('ask');

  it('allows a flag-validated safe command with an allowed flag', () => {
    // netstat has a flag def; -t is not blocked, so the flag loop runs and passes
    assert.equal(pm.classifyBashCommand('netstat -t'), 'safe');
  });

  it('allows a flag-validated safe command with a non-dash positional arg', () => {
    // '2' does not start with '-', exercising the false arm of the dash test
    assert.equal(pm.classifyBashCommand('tree -L 2'), 'safe');
  });

  it('rejects a safe command when a blocked flag is present', () => {
    // tree blocks -R (recursive-follow) — the guard must flip it to unsafe.
    assert.equal(pm.classifyBashCommand('tree -R'), 'unsafe');
    // The bare form (no blocked flag) is still safe — proves -R is what blocks.
    assert.equal(pm.classifyBashCommand('tree'), 'safe');
  });

  it('rejects fd with its blocked --exec equivalents', () => {
    assert.equal(pm.classifyBashCommand('fd foo -x rm'), 'unsafe');
    assert.equal(pm.classifyBashCommand('fd foo -t f'), 'safe');
  });

  it('rejects a blocked flag passed in --flag=value form', () => {
    // split('=')[0] of "--exec=rm" is "--exec"; fd blocks --exec
    assert.equal(pm.classifyBashCommand('fd pattern --exec=rm'), 'unsafe');
  });
});

// ---------------------------------------------------------------------------
// extractBaseCommands — $(...) / `...` / subshell extraction
// (findMatchingParen + extractDelimited backtick arms)
// ---------------------------------------------------------------------------

describe('classifyBashCommand — command-substitution extraction', () => {
  const pm = new PermissionManager('ask');

  it('extracts and classifies a balanced $(...) substitution', () => {
    // $(ls -la) must be parsed; echo + shell substitution => unsafe (not auto-safe)
    assert.equal(pm.classifyBashCommand('echo $(ls -la)'), 'unsafe');
  });

  it('detects a destructive command inside a backtick substitution', () => {
    // `rm foo` is extracted as its own command and flagged destructive
    assert.equal(pm.classifyBashCommand('echo `rm foo`'), 'destructive');
  });

  it('tolerates an unbalanced $( with no closing paren (findMatchingParen → -1)', () => {
    // No closing ')' — extraction yields null; still classified (unsafe via $()
    const result = pm.classifyBashCommand('echo $(ls -la');
    assert.ok(result === 'unsafe' || result === 'destructive');
  });

  it('tolerates an unterminated backtick (indexOf → -1)', () => {
    // Single backtick, no closing one — extraction yields null, echo stays safe
    assert.equal(pm.classifyBashCommand('echo `ls'), 'safe');
  });

  it('extracts commands from a subshell group (...)', () => {
    // Subshell containing rm must still be detected as destructive
    assert.equal(pm.classifyBashCommand('(cat x && rm y)'), 'destructive');
  });
});

// ---------------------------------------------------------------------------
// extractBaseCommands — comment / empty segment skip + env-assign after wrapper
// ---------------------------------------------------------------------------

describe('classifyBashCommand — segment skipping', () => {
  const pm = new PermissionManager('ask');

  it('skips a comment-only segment during base-command extraction', () => {
    // The '# note' segment is skipped in extractBaseCommands (startsWith('#')),
    // so no spurious command base is produced from it. The command is still
    // conservatively 'unsafe' because allSegmentsKnown re-splits and the comment
    // segment is not a known-safe command; bare `ls` alone stays 'safe'.
    assert.equal(pm.classifyBashCommand('ls ; # note'), 'unsafe');
    assert.equal(pm.classifyBashCommand('ls'), 'safe');
  });

  it('skips empty segments produced by doubled separators', () => {
    assert.equal(pm.classifyBashCommand('ls ;; cat file'), 'safe');
  });

  it('skips env-var assignments after an env wrapper', () => {
    // `env PATH=/usr/bin ls` -> wrapper env, skip PATH=/usr/bin, classify ls
    assert.equal(pm.classifyBashCommand('env PATH=/usr/bin ls'), 'safe');
  });

  it('skips a leading env-var assignment prefix', () => {
    assert.equal(pm.classifyBashCommand('FOO=bar ls -l'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// check() — auto-allow mode WITH rules (first-match deny still fires; Gap #32)
// ---------------------------------------------------------------------------

describe('check() — auto-allow mode rule evaluation', () => {
  it('a matching deny rule (with pattern) still fires in auto-allow', async () => {
    const rules: PermissionRule[] = [{ tool: 'bash', pattern: 'rm *', action: 'deny' }];
    const pm = new PermissionManager('auto-allow', rules);
    assert.equal(await pm.check('bash', { command: 'rm foo.txt' }), 'deny');
    // A command the deny pattern does NOT match is still auto-allowed
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  });

  it('a matching allow rule (with pattern) breaks to auto-allow', async () => {
    const rules: PermissionRule[] = [{ tool: 'bash', pattern: 'git *', action: 'allow' }];
    const pm = new PermissionManager('auto-allow', rules);
    assert.equal(await pm.check('bash', { command: 'git status' }), 'allow');
  });

  it('a pattern-less deny rule fires in auto-allow', async () => {
    const rules: PermissionRule[] = [{ tool: 'web_fetch', action: 'deny' }];
    const pm = new PermissionManager('auto-allow', rules);
    assert.equal(await pm.check('web_fetch', { url: 'http://x' }), 'deny');
  });

  it('a pattern-less allow rule breaks to auto-allow', async () => {
    const rules: PermissionRule[] = [{ tool: 'grep', action: 'allow' }];
    const pm = new PermissionManager('auto-allow', rules);
    assert.equal(await pm.check('grep', { pattern: 'x' }), 'allow');
  });

  it('skips paramPattern rules in auto-allow (falls through to allow)', async () => {
    const rules: PermissionRule[] = [{ tool: 'bash', paramPattern: { command: 'rm*' }, action: 'deny' }];
    const pm = new PermissionManager('auto-allow', rules);
    // paramPattern rule is skipped by the auto-allow fast-path -> allow
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  });

  it('skips rules whose tool does not match in auto-allow', async () => {
    const rules: PermissionRule[] = [{ tool: 'file_write', action: 'deny' }];
    const pm = new PermissionManager('auto-allow', rules);
    // deny rule targets a different tool -> bash is auto-allowed
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  });
});

// ---------------------------------------------------------------------------
// isXargsSafe — target-command safety (xargs as the leading safe command)
// ---------------------------------------------------------------------------

describe('classifyBashCommand — xargs target safety (isXargsSafe)', () => {
  const pm = new PermissionManager('ask');

  it('treats xargs with a safe read-only target as safe', () => {
    assert.equal(pm.classifyBashCommand('xargs cat'), 'safe');
  });

  it('treats xargs with a non-whitelisted target as unsafe', () => {
    // chmod is not in XARGS_SAFE_TARGETS -> isXargsSafe returns false
    assert.equal(pm.classifyBashCommand('xargs chmod'), 'unsafe');
  });

  it('treats bare xargs with no target as unsafe (no regex match)', () => {
    assert.equal(pm.classifyBashCommand('xargs'), 'unsafe');
  });
});

// ---------------------------------------------------------------------------
// isJqDangerous — system() call in the jq expression vs flag-only invocation
// ---------------------------------------------------------------------------

describe('classifyBashCommand — jq dangerous-construct detection', () => {
  const pm = new PermissionManager('ask');

  it('flags a jq expression containing system() as unsafe', () => {
    assert.equal(pm.classifyBashCommand('jq \'system("id")\' data.json'), 'unsafe');
  });

  it('allows a flag-only jq invocation (no expression to inspect)', () => {
    assert.equal(pm.classifyBashCommand('jq -n'), 'safe');
  });

  it('allows a benign jq filter', () => {
    assert.equal(pm.classifyBashCommand('jq -r .name data.json'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// isScriptOneLinerSafe — ruby / perl / node language arms
// ---------------------------------------------------------------------------

describe('classifyBashCommand — script one-liner language arms', () => {
  const pm = new PermissionManager('ask');

  it('ruby -e with no shell-exec is safe', () => {
    assert.equal(pm.classifyBashCommand("ruby -e 'puts 1+1'"), 'safe');
  });
  it('ruby -e using system() is unsafe', () => {
    assert.equal(pm.classifyBashCommand("ruby -e 'system(\"ls\")'"), 'unsafe');
  });
  it('ruby -e using backticks is unsafe', () => {
    assert.equal(pm.classifyBashCommand("ruby -e 'x = `ls`'"), 'unsafe');
  });
  it('perl -e with no shell-exec is safe', () => {
    assert.equal(pm.classifyBashCommand("perl -e 'print 2+2'"), 'safe');
  });
  it('perl -e using exec is unsafe', () => {
    assert.equal(pm.classifyBashCommand("perl -e 'exec(\"ls\")'"), 'unsafe');
  });
  it('perl -e using qx is unsafe', () => {
    assert.equal(pm.classifyBashCommand("perl -e 'qx(ls)'"), 'unsafe');
  });
  it('node -e requiring fs is unsafe', () => {
    assert.equal(pm.classifyBashCommand("node -e 'require(\"fs\")'"), 'unsafe');
  });
  it('node -e that is pure compute is safe', () => {
    assert.equal(pm.classifyBashCommand("node -e 'console.log(1)'"), 'safe');
  });
});

// ---------------------------------------------------------------------------
// stripSafeEnvVars — safe-only prefix stripped; unsafe prefix preserved
// ---------------------------------------------------------------------------

describe('stripSafeEnvVars', () => {
  it('strips a prefix of only whitelisted env vars', () => {
    assert.equal(stripSafeEnvVars('NODE_ENV=production ls -la'), 'ls -la');
  });
  it('preserves an unsafe (non-whitelisted) env var prefix', () => {
    assert.equal(stripSafeEnvVars('SECRET_TOKEN=abc ls'), 'SECRET_TOKEN=abc ls');
  });
  it('keeps only the unsafe assignments from a mixed prefix', () => {
    assert.equal(stripSafeEnvVars('NODE_ENV=prod SECRET=xyz run'), 'SECRET=xyz run');
  });
  it('does not drop a command that has no env prefix (empty-match path)', () => {
    // No assignments to strip: the anchored match is empty; the stripped result
    // still contains the full command (a harmless leading space may be added).
    assert.equal(stripSafeEnvVars('echo hello').trim(), 'echo hello');
  });
});

// ---------------------------------------------------------------------------
// classifyBashCommandCore — destructive/unsafe decision arms
// ---------------------------------------------------------------------------

describe('classifyBashCommand — SQL destructive patterns', () => {
  const pm = new PermissionManager('ask');
  it('DROP DATABASE is destructive', () => {
    assert.equal(pm.classifyBashCommand('psql -c "DROP DATABASE app"'), 'destructive');
  });
  it('DELETE FROM is destructive', () => {
    assert.equal(pm.classifyBashCommand('mysql -e "DELETE FROM users"'), 'destructive');
  });
  it('TRUNCATE TABLE is destructive', () => {
    assert.equal(pm.classifyBashCommand('sqlite3 db "TRUNCATE TABLE t"'), 'destructive');
  });
});

describe('classifyBashCommand — destructive git operations', () => {
  const pm = new PermissionManager('ask');
  it('git checkout -- . discards worktree (destructive)', () => {
    assert.equal(pm.classifyBashCommand('git checkout -- .'), 'destructive');
  });
  it('git restore . discards worktree (destructive)', () => {
    assert.equal(pm.classifyBashCommand('git restore .'), 'destructive');
  });
  it('git stash drop is destructive', () => {
    assert.equal(pm.classifyBashCommand('git stash drop'), 'destructive');
  });
  it('git stash clear is destructive', () => {
    assert.equal(pm.classifyBashCommand('git stash clear'), 'destructive');
  });
  it('git reset --merge is destructive', () => {
    assert.equal(pm.classifyBashCommand('git reset --merge'), 'destructive');
  });
  it('git reset --keep is destructive', () => {
    assert.equal(pm.classifyBashCommand('git reset --keep'), 'destructive');
  });
});

describe('classifyBashCommand — flag-dependent tools', () => {
  const pm = new PermissionManager('ask');
  it('sed -i (in-place) is unsafe', () => {
    assert.equal(pm.classifyBashCommand('sed -i s/a/b/ file.txt'), 'unsafe');
  });
  it('sed without -i (stdout) is safe', () => {
    assert.equal(pm.classifyBashCommand('sed s/a/b/ file.txt'), 'safe');
  });
  it('find -delete is destructive', () => {
    assert.equal(pm.classifyBashCommand('find . -name "*.tmp" -delete'), 'destructive');
  });
  it('find -exec rm is destructive', () => {
    assert.equal(pm.classifyBashCommand('find . -exec rm {} ;'), 'destructive');
  });
  it('make clean is unsafe', () => {
    assert.equal(pm.classifyBashCommand('make clean'), 'unsafe');
  });
});

describe('classifyBashCommand — docker subcommand safety', () => {
  const pm = new PermissionManager('ask');
  it('docker ps (read-only subcommand) is safe', () => {
    assert.equal(pm.classifyBashCommand('docker ps -a'), 'safe');
  });
  it('docker run (mutating subcommand) is unsafe', () => {
    assert.equal(pm.classifyBashCommand('docker run ubuntu echo hi'), 'unsafe');
  });
});

// ---------------------------------------------------------------------------
// checkDenyRules — bash matchBashPattern arm, non-bash arm, reason fallback
// ---------------------------------------------------------------------------

describe('checkDenyRules', () => {
  it('matches a bash deny rule via compound-aware matcher and uses custom reason', () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'rm *', action: 'deny', reason: 'no rm' });
    const r = pm.checkDenyRules('bash', 'ls && rm -rf build');
    assert.equal(r.denied, true);
    assert.equal(r.reason, 'no rm');
  });

  it('matches a non-bash deny rule via glob matcher', () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'file_write', pattern: '*secret*', action: 'deny' });
    const r = pm.checkDenyRules('file_write', '/x/secret.txt');
    assert.equal(r.denied, true);
    assert.match(r.reason!, /Denied by rule/);
  });

  it('matches a whole-tool deny rule (no pattern) with default reason', () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'web_fetch', action: 'deny' });
    const r = pm.checkDenyRules('web_fetch', 'http://x');
    assert.equal(r.denied, true);
  });

  it('skips paramPattern rules (cannot evaluate from a string) and returns not-denied', () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', paramPattern: { command: 'rm*' }, action: 'deny' });
    assert.equal(pm.checkDenyRules('bash', 'rm -rf x').denied, false);
  });

  it('returns not-denied when a deny pattern does not match', () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'rm *', action: 'deny' });
    assert.equal(pm.checkDenyRules('bash', 'ls -la').denied, false);
  });
});

// ---------------------------------------------------------------------------
// check() — mode-specific guard arms
// ---------------------------------------------------------------------------

describe('check() — dangerouslyDisableSandbox arms', () => {
  it('asks in ask mode', async () => {
    const pm = new PermissionManager('ask');
    assert.equal(await pm.check('bash', { command: 'ls', dangerouslyDisableSandbox: true }), 'ask');
  });
  it('denies in deny-all mode', async () => {
    const pm = new PermissionManager('deny-all');
    assert.equal(await pm.check('bash', { command: 'ls', dangerouslyDisableSandbox: true }), 'deny');
  });
  it('denies in dontAsk mode', async () => {
    const pm = new PermissionManager('dontAsk');
    assert.equal(await pm.check('bash', { command: 'ls', dangerouslyDisableSandbox: true }), 'deny');
  });
});

describe('check() — acceptEdits mode', () => {
  it('allows an ordinary file edit', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('file_edit', { file_path: 'src/app.ts' }), 'allow');
  });
  it('asks for a build-config file edit (supply-chain guard)', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('file_write', { file_path: '/repo/package.json' }), 'ask');
  });
  it('allows a safe bash command', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
  });
  it('falls through (ask) for a non-safe bash command', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'curl http://x' }), 'ask');
  });
});

describe('check() — plan mode', () => {
  it('allows read-only tools', async () => {
    const pm = new PermissionManager('plan');
    assert.equal(await pm.check('file_read', { file_path: 'x' }), 'allow');
    assert.equal(await pm.check('grep', { pattern: 'x' }), 'allow');
  });
  it('denies mutating tools', async () => {
    const pm = new PermissionManager('plan');
    assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
    assert.equal(await pm.check('file_write', { file_path: 'x' }), 'deny');
  });
});

describe('check() — bypassPermissions mode', () => {
  it('allows ordinary operations', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  });
  it('asks when touching a protected safety path', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('file_write', { file_path: '/home/u/.ssh/authorized_keys' }), 'ask');
  });
});

describe('check() — credential & system-dir guards in ask mode', () => {
  it('asks for a bash command that targets a credential path', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: '*', action: 'allow' });
    // wildcard allow must NOT auto-approve reading a credential file
    assert.equal(await pm.check('bash', { command: 'cat ~/.ssh/id_rsa' }), 'ask');
  });
  it('asks for a bash command that writes to a system directory', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: '*', action: 'allow' });
    assert.equal(await pm.check('bash', { command: 'echo x > /etc/hosts' }), 'ask');
  });
  it('downgrades a destructive bash command under a wildcard allow to ask', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: '*', action: 'allow' });
    assert.equal(await pm.check('bash', { command: 'rm -rf build' }), 'ask');
  });
});

describe('check() — policy memo caching', () => {
  it('returns the cached decision on a repeat identical call', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'ls*', action: 'allow' });
    pm.enableMemo(10_000);
    const first = await pm.check('bash', { command: 'ls -la' });
    const second = await pm.check('bash', { command: 'ls -la' });
    assert.equal(first, 'allow');
    assert.equal(second, 'allow');
    assert.ok(pm.getMemo()!.size() >= 1);
  });
});

// ---------------------------------------------------------------------------
// toolNameMatches — namespace glob (e.g. github:*)
// ---------------------------------------------------------------------------

describe('check() — namespaced tool glob rule', () => {
  it('a github:* deny rule matches a github:create_issue tool', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'github:*', action: 'deny' });
    assert.equal(await pm.check('github:create_issue', { title: 'x' }), 'deny');
    // a tool outside the namespace is unaffected (defaults to ask)
    assert.equal(await pm.check('gitlab:create', { title: 'x' }), 'ask');
  });
});

// ---------------------------------------------------------------------------
// loadRules — sanitizeParamPattern rejects malformed (non-string) values
// ---------------------------------------------------------------------------

describe('loadRules — paramPattern sanitization', () => {
  it('skips a rule whose paramPattern has a non-string value', () => {
    const dir = makeTempDir();
    try {
      const cfgDir = path.join(dir, '.superinference');
      fs.mkdirSync(cfgDir, { recursive: true });
      fs.writeFileSync(path.join(cfgDir, 'permissions.json'), JSON.stringify({
        rules: [
          { tool: 'bash', action: 'allow', paramPattern: { model: 123 } }, // malformed -> skipped
          { tool: 'grep', action: 'deny', paramPattern: { pattern: 'x*' } }, // valid -> loaded
        ],
      }));
      const pm = new PermissionManager('ask');
      pm.loadRules(dir);
      const rules = pm.getRules();
      assert.equal(rules.length, 1);
      assert.equal(rules[0].tool, 'grep');
      assert.deepEqual(rules[0].paramPattern, { pattern: 'x*' });
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// matchPattern — NUL-byte poisoning rejection (via a rule pattern)
// ---------------------------------------------------------------------------

describe('check() — NUL byte in pattern never matches', () => {
  it('a rule pattern containing a NUL byte cannot match a command', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'rm\u0000*', action: 'deny' });
    // matchPattern short-circuits to false on NUL -> no deny -> destructive rm asks
    assert.notEqual(await pm.check('bash', { command: 'rm x' }), 'deny');
  });
});

// ---------------------------------------------------------------------------
// matchBashPattern — compound ALLOW requires every sub-command to match
// ---------------------------------------------------------------------------

describe('check() — compound allow-rule semantics', () => {
  it('allows a compound command when every sub-command matches the allow pattern', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'allow' });
    assert.equal(await pm.check('bash', { command: 'echo a && echo b' }), 'allow');
  });
  it('does not allow when one sub-command falls outside the allow pattern', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'allow' });
    // grep does not match "echo *" -> allow rule fails -> default ask
    assert.equal(await pm.check('bash', { command: 'echo a && grep b file' }), 'ask');
  });
});

// ---------------------------------------------------------------------------
// isPathAllowed — escape-to-home vs escape-to-nowhere arms
// ---------------------------------------------------------------------------

describe('isPathAllowed — relative-escape arms', () => {
  const pm = new PermissionManager('ask');
  it('allows an absolute path that escapes cwd but lands inside home', () => {
    const home = os.homedir();
    const cwd = path.join(home, 'proj', 'sub');
    const target = path.join(home, 'notes.txt');
    assert.equal(pm.isPathAllowed(target, cwd), true);
  });
  it('denies an absolute path that escapes cwd and is neither home nor a system dir', () => {
    const cwd = path.join(os.homedir(), 'proj');
    // /tmp is outside home and not in SYSTEM_DIRECTORIES -> escape returns false
    const target = path.join(os.tmpdir(), 'sibling-escape-xyz.txt');
    assert.equal(pm.isPathAllowed(target, cwd), false);
  });
});

// ---------------------------------------------------------------------------
// loadRules — paramPattern that is an array or empty object is rejected
// ---------------------------------------------------------------------------

describe('loadRules — paramPattern array/empty rejection', () => {
  it('skips rules whose paramPattern is an array or an empty object', () => {
    const dir = makeTempDir();
    try {
      const cfgDir = path.join(dir, '.superinference');
      fs.mkdirSync(cfgDir, { recursive: true });
      fs.writeFileSync(path.join(cfgDir, 'permissions.json'), JSON.stringify({
        rules: [
          { tool: 'bash', action: 'allow', paramPattern: [] },        // array -> rejected
          { tool: 'bash', action: 'allow', paramPattern: {} },        // empty -> rejected
          { tool: 'grep', action: 'allow' },                           // plain -> loaded
        ],
      }));
      const pm = new PermissionManager('ask');
      pm.loadRules(dir);
      const rules = pm.getRules();
      assert.equal(rules.length, 1);
      assert.equal(rules[0].tool, 'grep');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// isScriptOneLinerSafe — python arms + node import variant
// ---------------------------------------------------------------------------

describe('classifyBashCommand — python one-liner arms', () => {
  const pm = new PermissionManager('ask');
  it('python3 -c pure compute is safe', () => {
    assert.equal(pm.classifyBashCommand("python3 -c 'print(2+2)'"), 'safe');
  });
  it('python3 -c importing os is unsafe', () => {
    assert.equal(pm.classifyBashCommand("python3 -c 'import os'"), 'unsafe');
  });
  it('python -c with from subprocess import is unsafe', () => {
    assert.equal(pm.classifyBashCommand("python -c 'from subprocess import run'"), 'unsafe');
  });
  it('node -e with import-from fs is unsafe', () => {
    assert.equal(pm.classifyBashCommand("node -e 'import x from \"fs\"'"), 'unsafe');
  });
});

// ---------------------------------------------------------------------------
// extractBaseCommands — subshell group + $-guarded paren + nested substitution
// ---------------------------------------------------------------------------

describe('classifyBashCommand — nested / subshell extraction arms', () => {
  const pm = new PermissionManager('ask');
  it('parses a subshell group (paren-attached tokens make it conservatively unsafe)', () => {
    // The '(' attaches to the first token, so allSegmentsKnown cannot recognize it
    // as a known-safe prefix; the subshell is still fully parsed without crashing.
    assert.equal(pm.classifyBashCommand('(cat a && ls b)'), 'unsafe');
  });
  it('detects destructive command nested in $() inside a subshell', () => {
    assert.equal(pm.classifyBashCommand('(echo $(rm -rf x))'), 'destructive');
  });
  it('handles an unbalanced subshell paren without crashing', () => {
    const r = pm.classifyBashCommand('(cat a');
    assert.ok(['safe', 'unsafe', 'destructive'].includes(r));
  });
});

// ---------------------------------------------------------------------------
// allSegmentsKnown — multi-word safe prefix + env-only segment
// ---------------------------------------------------------------------------

describe('classifyBashCommand — allSegmentsKnown prefix arms', () => {
  const pm = new PermissionManager('ask');
  it('multi-word safe prefixes across a pipeline are all recognized', () => {
    assert.equal(pm.classifyBashCommand('git status | cat'), 'safe');
  });
  it('a 3-word safe prefix (tsc --noEmit) is recognized in a chain', () => {
    assert.equal(pm.classifyBashCommand('cargo check && go build'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// Complementary short-circuit operands (cover the other side of || / &&)
// ---------------------------------------------------------------------------

describe('isScriptOneLinerSafe — remaining exec-family operands', () => {
  const pm = new PermissionManager('ask');
  it('ruby -e using exec is unsafe (2nd operand)', () => {
    assert.equal(pm.classifyBashCommand("ruby -e 'exec(\"ls\")'"), 'unsafe');
  });
  it('ruby -e using IO.popen is unsafe (4th operand)', () => {
    assert.equal(pm.classifyBashCommand("ruby -e 'IO.popen(\"ls\")'"), 'unsafe');
  });
  it('perl -e using system is unsafe (1st operand)', () => {
    assert.equal(pm.classifyBashCommand("perl -e 'system(\"ls\")'"), 'unsafe');
  });
  it('perl -e using backticks is unsafe (3rd operand)', () => {
    assert.equal(pm.classifyBashCommand("perl -e 'my $x = `ls`'"), 'unsafe');
  });
});

describe('matchPattern — NUL byte on the value side', () => {
  it('a NUL-poisoned command value cannot match a plain deny pattern', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'rm *', action: 'deny' });
    // value contains \u0000, pattern does not -> right operand of the NUL guard
    assert.notEqual(await pm.check('bash', { command: 'rm \u0000evil' }), 'deny');
  });
});

describe('matchBashPattern — empty command path', () => {
  it('a wildcard allow rule matches an empty command', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: '*', action: 'allow' });
    assert.equal(await pm.check('bash', { command: '' }), 'allow');
  });
});

describe('classifyBashCommand — env-only compound segment', () => {
  const pm = new PermissionManager('ask');
  it('an assignment-only segment is skipped, leaving the pipeline safe', () => {
    assert.equal(pm.classifyBashCommand('A=1 ; ls'), 'safe');
  });
});

describe('classifyBashCommand — subshell not at index 0', () => {
  const pm = new PermissionManager('ask');
  it('parses a subshell that follows a separator (src[i-1] !== "$")', () => {
    const r = pm.classifyBashCommand('ls ; (cat foo)');
    assert.ok(['safe', 'unsafe', 'destructive'].includes(r));
  });
});

describe('isPathAllowed — escape lands on a system directory', () => {
  const pm = new PermissionManager('ask');
  it('denies an escaping absolute path that resolves into a system dir', () => {
    const cwd = path.join(os.homedir(), 'proj');
    assert.equal(pm.isPathAllowed('/usr/local/somebinary', cwd), false);
  });
});

describe('check() — previously-denied key and acceptEdits path param', () => {
  it('auto-denies an exact invocation that was tracked as denied', async () => {
    const pm = new PermissionManager('ask');
    pm.trackDenial('bash', { command: 'make release' });
    assert.equal(await pm.check('bash', { command: 'make release' }), 'deny');
  });
  it('acceptEdits uses the path param for the build-config guard', async () => {
    const pm = new PermissionManager('acceptEdits');
    // uses `path` (not `file_path`) -> still caught as build-config -> ask
    assert.equal(await pm.check('file_edit', { path: '/repo/Dockerfile' }), 'ask');
  });
});

// ---------------------------------------------------------------------------
// Additional check() arms
// ---------------------------------------------------------------------------

describe('check() — bypass/acceptEdits/credential corner arms', () => {
  it('bypassPermissions allows a tool that is not bash/file_edit/file_write', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('grep', { pattern: 'x' }), 'allow');
  });

  it('acceptEdits allows an edit with no file path (build-config guard skipped)', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('file_edit', {}), 'allow');
  });

  it('an explicit deny rule beats the credential-path ask guard', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'cat *', action: 'deny' });
    // hasExplicitDeny short-circuits the credential "ask" and the rule denies
    assert.equal(await pm.check('bash', { command: 'cat ~/.ssh/id_rsa' }), 'deny');
  });

  it('a pattern-less allow rule on a destructive command downgrades to ask', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', action: 'allow' });
    assert.equal(await pm.check('bash', { command: 'rm -rf dist' }), 'ask');
  });
});

// ---------------------------------------------------------------------------
// Additional classifyBashCommand arms
// ---------------------------------------------------------------------------

describe('classifyBashCommand — remaining classification arms', () => {
  const pm = new PermissionManager('ask');

  it('an all-comment command is safe (no effective commands)', () => {
    assert.equal(pm.classifyBashCommand('# just a note'), 'safe');
  });
  it('an empty command is safe', () => {
    assert.equal(pm.classifyBashCommand('   '), 'safe');
  });
  it('rm -f (force only) is destructive', () => {
    assert.equal(pm.classifyBashCommand('rm -f file'), 'destructive');
  });
  it('rm -r (recursive only) is destructive', () => {
    assert.equal(pm.classifyBashCommand('rm -r dir'), 'destructive');
  });
  it('DROP TABLE is destructive', () => {
    assert.equal(pm.classifyBashCommand('psql -c "DROP TABLE t"'), 'destructive');
  });
  it('bare TRUNCATE is destructive', () => {
    assert.equal(pm.classifyBashCommand('psql -c "TRUNCATE logs"'), 'destructive');
  });
  it('find -ok rm is destructive', () => {
    assert.equal(pm.classifyBashCommand('find . -ok rm {} ;'), 'destructive');
  });
  it('docker with no subcommand is unsafe', () => {
    assert.equal(pm.classifyBashCommand('docker'), 'unsafe');
  });
  it('a command whose extracted path targets a system directory is destructive', () => {
    // tar is not in the safe/unsafe/destructive name sets, so classification
    // falls through to the extractCommandPaths system-directory guard.
    assert.equal(pm.classifyBashCommand('tar -C /etc xf archive'), 'destructive');
  });
});

// ---------------------------------------------------------------------------
// loadRules — non-object paramPattern + invalid mode + malformed json
// ---------------------------------------------------------------------------

describe('loadRules — extra validation arms', () => {
  it('skips a rule whose paramPattern is a bare string, ignores invalid mode, keeps valid rule', () => {
    const dir = makeTempDir();
    try {
      const cfgDir = path.join(dir, '.superinference');
      fs.mkdirSync(cfgDir, { recursive: true });
      fs.writeFileSync(path.join(cfgDir, 'permissions.json'), JSON.stringify({
        mode: 'not-a-real-mode',
        rules: [
          { tool: 'bash', action: 'allow', paramPattern: 'oops' }, // non-object -> skipped
          { tool: 'file_read', action: 'ask' },                     // valid -> loaded
        ],
      }));
      const pm = new PermissionManager('ask');
      pm.loadRules(dir);
      assert.equal(pm.getMode(), 'ask'); // invalid mode ignored
      const rules = pm.getRules();
      assert.equal(rules.length, 1);
      assert.equal(rules[0].tool, 'file_read');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('silently ignores a malformed permissions.json', () => {
    const dir = makeTempDir();
    try {
      const cfgDir = path.join(dir, '.superinference');
      fs.mkdirSync(cfgDir, { recursive: true });
      fs.writeFileSync(path.join(cfgDir, 'permissions.json'), '{ not json ');
      const pm = new PermissionManager('ask');
      assert.doesNotThrow(() => pm.loadRules(dir));
      assert.equal(pm.getRules().length, 0);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('applies a valid persisted mode', () => {
    const dir = makeTempDir();
    try {
      const cfgDir = path.join(dir, '.superinference');
      fs.mkdirSync(cfgDir, { recursive: true });
      fs.writeFileSync(path.join(cfgDir, 'permissions.json'), JSON.stringify({ mode: 'acceptEdits' }));
      const pm = new PermissionManager('ask');
      pm.loadRules(dir);
      assert.equal(pm.getMode(), 'acceptEdits');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// extractBaseCommands — redirect-to-system-path + 3-token prefix
// ---------------------------------------------------------------------------

describe('classifyBashCommand — redirect & multi-token prefixes', () => {
  const pm = new PermissionManager('ask');
  it('a redirect to a system path is destructive', () => {
    assert.equal(pm.classifyBashCommand('echo x > /etc/profile'), 'destructive');
  });
  it('a 3-token safe prefix (git stash list) is recognized', () => {
    assert.equal(pm.classifyBashCommand('git stash list'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// isPathAllowed — relative (non-absolute) path with .. segments
// ---------------------------------------------------------------------------

describe('isPathAllowed — non-absolute escape', () => {
  const pm = new PermissionManager('ask');
  it('a relative .. path resolving to a system dir is denied', () => {
    const cwd = path.join(os.homedir(), 'proj');
    // filePath is NOT absolute -> the home-escape block is skipped, the
    // system-directory loop runs instead.
    assert.equal(pm.isPathAllowed('../../../../etc/shadow', cwd), false);
  });
  it('a plain in-cwd relative path is allowed', () => {
    const dir = makeTempDir();
    try {
      assert.equal(pm.isPathAllowed('sub/file.ts', dir), true);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// containsUnquotedExpansion / containsShellSubstitution (exported predicates)
// ---------------------------------------------------------------------------

describe('containsUnquotedExpansion', () => {
  it('detects an unquoted variable', () => {
    assert.equal(containsUnquotedExpansion('echo $HOME'), true);
  });
  it('detects an unquoted glob', () => {
    assert.equal(containsUnquotedExpansion('ls *.ts'), true);
  });
  it('detects an unquoted bracket expression', () => {
    assert.equal(containsUnquotedExpansion('ls file[0-9]'), true);
  });
  it('ignores expansions inside quotes', () => {
    assert.equal(containsUnquotedExpansion('echo "$HOME is safe"'), false);
    assert.equal(containsUnquotedExpansion("echo 'literal *'"), false);
  });
});

describe('containsShellSubstitution', () => {
  it('detects $VAR / $() but tolerates globs', () => {
    assert.equal(containsShellSubstitution('cat $FILE'), true);
    assert.equal(containsShellSubstitution('cat $(ls)'), true);
    assert.equal(containsShellSubstitution('ls *.ts'), false);
  });
});

// ---------------------------------------------------------------------------
// detectCommandChaining — each operator variant
// ---------------------------------------------------------------------------

describe('detectCommandChaining', () => {
  it('reports && chaining', () => {
    const r = detectCommandChaining('a && b');
    assert.deepEqual(r.operators, ['&&']);
    assert.equal(r.chained, true);
  });
  it('reports || and ; together', () => {
    const r = detectCommandChaining('a || b ; c');
    assert.ok(r.operators.includes('||'));
    assert.ok(r.operators.includes(';'));
    assert.equal(r.count, 3);
  });
  it('reports no chaining for a single command', () => {
    const r = detectCommandChaining('ls -la');
    assert.equal(r.chained, false);
    assert.equal(r.count, 1);
  });
  it('ignores operators inside quotes', () => {
    const r = detectCommandChaining('echo "a && b"');
    assert.equal(r.chained, false);
  });
});

// ---------------------------------------------------------------------------
// check() — auto-allow with a NON-bash patterned rule (matchPattern side)
// ---------------------------------------------------------------------------

describe('check() — auto-allow non-bash patterned rules', () => {
  it('a non-bash deny rule with a glob pattern fires via matchPattern', async () => {
    const pm = new PermissionManager('auto-allow', [
      { tool: 'file_write', pattern: '*.secret', action: 'deny' },
    ]);
    assert.equal(await pm.check('file_write', { file_path: 'creds.secret' }), 'deny');
    // a non-matching path falls through to auto-allow
    assert.equal(await pm.check('file_write', { file_path: 'notes.txt' }), 'allow');
  });

  it('a non-bash allow rule with a glob pattern breaks to auto-allow', async () => {
    const pm = new PermissionManager('auto-allow', [
      { tool: 'file_read', pattern: 'src/*', action: 'allow' },
    ]);
    assert.equal(await pm.check('file_read', { file_path: 'src/app.ts' }), 'allow');
  });
});

// ---------------------------------------------------------------------------
// PolicyMemo — LRU eviction when exceeding the max-entries cap
// ---------------------------------------------------------------------------

describe('PolicyMemo — LRU eviction', () => {
  it('evicts the least-recently-used entry past the cap and keeps the hot one', () => {
    const memo = new PolicyMemo(100_000);
    // Insert well past POLICY_MEMO_MAX_ENTRIES (1000) to trigger _evictLRU
    for (let i = 0; i < 1100; i++) memo.set('bash', 'h' + i, 'allow');
    // Touch a recent key so it is NOT the LRU victim
    memo.get('bash', 'h1099');
    assert.ok(memo.size() <= 1000, 'cache stays bounded');
    // The hot key survives eviction
    assert.equal(memo.get('bash', 'h1099'), 'allow');
  });

  it('expires entries past their TTL on get and prunes them', () => {
    const memo = new PolicyMemo(-1); // already-expired TTL
    memo.set('bash', 'x', 'deny');
    assert.equal(memo.get('bash', 'x'), null); // expired -> deleted on get
    memo.set('bash', 'y', 'ask');
    assert.equal(memo.prune(), 1); // y is expired -> pruned
  });
});

// ---------------------------------------------------------------------------
// detectHardlineCommand — variable indirection with undefined reference
// ---------------------------------------------------------------------------

describe('detectHardlineCommand — variable indirection arms', () => {
  it('resolves an assigned dangerous base and blocks it', () => {
    const r = detectHardlineCommand('c=rm; $c -rf /');
    assert.equal(r.blocked, true);
  });
  it('leaves an unassigned variable reference intact (ternary false arm)', () => {
    // $other is never assigned, so it must NOT be substituted; a benign command
    // with an undefined var reference is not hardline-blocked.
    const r = detectHardlineCommand('c=rm; echo $other hello');
    assert.equal(r.blocked, false);
  });
});

// ---------------------------------------------------------------------------
// classifyBashCommand — find read-only vs sed read-only edges
// ---------------------------------------------------------------------------

describe('classifyBashCommand — read-only find/sed edges', () => {
  const pm = new PermissionManager('ask');
  it('find without a command-running primary is safe', () => {
    assert.equal(pm.classifyBashCommand('find . -name "*.ts" -type f'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// matchPattern — exact-equality fast path (pattern === value)
// ---------------------------------------------------------------------------

describe('check() — exact pattern equals command (matchPattern fast path)', () => {
  it('a rule pattern identical to the command matches without regex', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'ls', action: 'allow' });
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  });
});

// ---------------------------------------------------------------------------
// classifyBashCommandCore — safe prefix/command with an unknown chained segment
// (exercises the `break` paths where allSegmentsKnown returns false)
// ---------------------------------------------------------------------------

describe('classifyBashCommand — known command chained with unknown', () => {
  const pm = new PermissionManager('ask');
  it('a safe prefix chained with an unknown command is not safe', () => {
    assert.notEqual(pm.classifyBashCommand('git status && frobnicate --go'), 'safe');
  });
  it('a safe single command chained with an unknown command is not safe', () => {
    assert.notEqual(pm.classifyBashCommand('ls && frobnicate'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// isPathAllowed — additional sensitive-name pattern arms
// ---------------------------------------------------------------------------

describe('isPathAllowed — sensitive pattern arms', () => {
  const pm = new PermissionManager('ask');
  it('denies a .netrc file', () => {
    const dir = makeTempDir();
    try { assert.equal(pm.isPathAllowed(path.join(dir, '.netrc'), dir), false); }
    finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it('denies a secrets.yaml file', () => {
    const dir = makeTempDir();
    try { assert.equal(pm.isPathAllowed(path.join(dir, 'secrets.yaml'), dir), false); }
    finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it('denies a .env.local file', () => {
    const dir = makeTempDir();
    try { assert.equal(pm.isPathAllowed(path.join(dir, '.env.local'), dir), false); }
    finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it('denies a .p12 keystore by extension', () => {
    const dir = makeTempDir();
    try { assert.equal(pm.isPathAllowed(path.join(dir, 'cert.p12'), dir), false); }
    finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
});

// ---------------------------------------------------------------------------
// classifyBashCommand — credential-path elevation of an otherwise-safe read
// ---------------------------------------------------------------------------

describe('classifyBashCommand — credential elevation wrapper', () => {
  const pm = new PermissionManager('ask');
  it('elevates a safe read of a credential file to unsafe', () => {
    // `cat ~/.ssh/id_rsa` would be "safe" by base-command, but reading a
    // credential path must never auto-classify as safe.
    assert.equal(pm.classifyBashCommand('cat ~/.ssh/id_rsa'), 'unsafe');
  });
  it('leaves a normal safe read as safe', () => {
    assert.equal(pm.classifyBashCommand('cat README.md'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// isPathAllowed — exact-equality operands (resolved === cwd / home / sysdir)
// ---------------------------------------------------------------------------

describe('isPathAllowed — exact directory equality', () => {
  const pm = new PermissionManager('ask');
  it('allows the cwd itself', () => {
    const dir = makeTempDir();
    try {
      const real = fs.realpathSync(dir);
      assert.equal(pm.isPathAllowed(real, real), true);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it('allows the home directory itself', () => {
    const home = fs.realpathSync(os.homedir());
    const dir = makeTempDir();
    try { assert.equal(pm.isPathAllowed(home, dir), true); }
    finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it('denies a system directory root exactly', () => {
    const dir = makeTempDir();
    try { assert.equal(pm.isPathAllowed('/etc', dir), false); }
    finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
});

// ---------------------------------------------------------------------------
// check() — deny rule matched via compound-aware matcher in the main loop
// ---------------------------------------------------------------------------

describe('check() — compound deny via main rule loop', () => {
  it('a deny rule fires when ANY sub-command of a compound matches', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'rm *', action: 'deny' });
    // matchBashPattern(..., 'deny') uses .some across sub-commands
    assert.equal(await pm.check('bash', { command: 'ls && rm build' }), 'deny');
  });
  it('an in-home (non-cwd) path is allowed via the final inHome arm', () => {
    const pm = new PermissionManager('ask');
    const home = fs.realpathSync(os.homedir());
    const cwd = makeTempDir();
    try {
      // target is under home but NOT under the temp cwd -> final `inCwd || inHome`
      assert.equal(pm.isPathAllowed(path.join(home, 'some-visible-notes.md'), cwd), true);
    } finally { fs.rmSync(cwd, { recursive: true, force: true }); }
  });
});
