import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  containsCredential,
  redactCredentials,
  findCredentials,
  REDACT_PLACEHOLDER,
  CredentialGuard,
} from '../src/credential-guard';
import {
  isPinnedVariable,
  getPinCategory,
  getAllPinnedVariables,
  PIN_CATEGORIES,
  POLICY_VARIABLES,
  CODE_LOADING_VARIABLES,
  TELEMETRY_VARIABLES,
  EnvProtectionGuard,
} from '../src/env-protection';
import {
  isBlockedInSafeMode,
  isPartiallyBlocked,
  shouldEnableSafeMode,
  BLOCKED_RESOURCES,
  SafeModeGuard,
  type BlockedResource,
} from '../src/safe-mode';
import {
  isSensitiveEnvVar,
  canAccessEnv,
  ENV_ACCESS_TIERS,
  ENV_OPERATIONS,
  SENSITIVE_ENV_VARS,
  EnvAccessGuard,
} from '../src/env-access-guard';
import {
  scanCodeForSecurityPatterns,
  SECURITY_PATTERNS,
} from '../src/code-security-patterns';

// ---------------------------------------------------------------------------
// POV: core credential/env/safe-mode guards — observable-behavior matrix.
// Every expectation is computed by calling the real guard functions against
// crafted positive / negative inputs. No source-text assertions.
// ---------------------------------------------------------------------------

// Fake-but-pattern-matching secrets (never real credentials).
const SECRETS: Array<{ label: string; text: string; type: string }> = [
  // Secret-shaped fixtures are assembled from split literals so no contiguous token appears in
  // source (keeps GitHub push-protection/secret-scanning from flagging test data); the detector
  // still sees the full joined pattern at runtime.
  { label: 'anthropic', text: 'sk-ant-' + 'api03-ABCDEFGHIJKLMNOPQRSTUV0123456789', type: 'anthropic_key' },
  { label: 'openai', text: 'sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', type: 'openai_key' },
  { label: 'google', text: 'AIzaSyABCDEFGHIJKLMNOPQRSTUVWXYZ0123456', type: 'google_key' },
  { label: 'github_pat', text: 'ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', type: 'github_pat' },
  { label: 'github_oauth', text: 'gho_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', type: 'github_oauth' },
  { label: 'github_fine_pat', text: 'github_pat_ABCDEFGHIJKLMNOPQRSTUV_more01234', type: 'github_fine_pat' },
  { label: 'gitlab', text: 'glpat-ABCDEFGHIJKLMNOPQRSTUV', type: 'gitlab_pat' },
  { label: 'slack_bot', text: 'xoxb' + '-123456789-ABCDEFGHIJKLMNOP', type: 'slack_bot' },
  { label: 'slack_user', text: 'xoxp' + '-987654321-ZYXWVUTSRQPONML', type: 'slack_user' },
  { label: 'aws', text: 'AKIAIOSFODNN7EXAMPLE', type: 'aws_access_key' },
  {
    label: 'jwt',
    text: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.' + 'eyJzdWIiOiIxMjM0NTY3ODkwIn0' + '.ABCDEFGHIJKLMNOPQRSTUVWXYZ012345',
    type: 'jwt',
  },
  { label: 'nvapi', text: 'nvapi-ABCDEFGHIJKLMNOPQRSTUV0123456789', type: null as unknown as string },
  { label: 'bearer', text: 'Authorization: Bearer ABCDEFGHIJKLMNOPQRSTUVWXYZ0123', type: 'bearer_token' },
];

const PRIVATE_KEY =
  '-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEAabc\ndefghijklmnop\n-----END RSA PRIVATE KEY-----';

describe('credential-guard :: containsCredential positives', () => {
  for (const s of SECRETS) {
    it(`detects ${s.label}`, () => {
      assert.equal(containsCredential(s.text), true);
    });
  }

  it('detects a PEM private key block', () => {
    assert.equal(containsCredential(PRIVATE_KEY), true);
  });

  it('detects a contextual high-entropy assigned secret', () => {
    const text = 'api_key = "Zx9Qw3Lp8Rt2Vb6Nm4Kc1Df7Gh5Jk0"';
    assert.equal(containsCredential(text), true);
  });

  it('detects secret embedded in surrounding log text', () => {
    const text = `log line before\nusing token ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 now\nafter`;
    assert.equal(containsCredential(text), true);
  });
});

describe('credential-guard :: containsCredential negatives', () => {
  const benign = [
    'the quick brown fox jumps over the lazy dog',
    'function add(a, b) { return a + b; }',
    'git commit -m "fix the thing"',
    'https://example.com/path/to/resource?query=value',
    'API_KEY is not set in this environment',
    'short', // below length floor
    '',
    'sk-tooshort', // sk- prefix but too short for pattern
    'password = hunterhunter', // low entropy assigned value, not flagged
    'name: a repeated aaaaaaaaaaaaaaaaaaaa string', // low variety
  ];
  for (const t of benign) {
    it(`ignores benign input ${JSON.stringify(t).slice(0, 40)}`, () => {
      assert.equal(containsCredential(t), false);
    });
  }

  it('returns false for null-ish/empty text', () => {
    assert.equal(containsCredential(''), false);
    assert.equal(containsCredential(undefined as unknown as string), false);
  });
});

describe('credential-guard :: redactCredentials', () => {
  it('replaces each known secret with the placeholder', () => {
    for (const s of SECRETS) {
      const out = redactCredentials(s.text);
      assert.equal(out.includes(REDACT_PLACEHOLDER), true, `${s.label} should be redacted`);
      assert.equal(containsCredential(out), false, `${s.label} residual after redaction`);
    }
  });

  it('redacts a private key block', () => {
    const out = redactCredentials(PRIVATE_KEY);
    assert.equal(out.includes(REDACT_PLACEHOLDER), true);
    assert.equal(out.includes('BEGIN RSA PRIVATE KEY'), false);
  });

  it('redacts only the high-entropy assigned value and keeps the key', () => {
    const text = 'api_key = "Zx9Qw3Lp8Rt2Vb6Nm4Kc1Df7Gh5Jk0"';
    const out = redactCredentials(text);
    assert.equal(out.includes('api_key'), true);
    assert.equal(out.includes(REDACT_PLACEHOLDER), true);
    assert.equal(out.includes('Zx9Qw3Lp8Rt2Vb6Nm4Kc1Df7Gh5Jk0'), false);
  });

  it('leaves benign text untouched', () => {
    const text = 'the quick brown fox jumps over the lazy dog repeatedly';
    assert.equal(redactCredentials(text), text);
  });

  it('returns short text unchanged', () => {
    assert.equal(redactCredentials('short'), 'short');
    assert.equal(redactCredentials(''), '');
  });

  it('is idempotent — redacting twice is stable', () => {
    const once = redactCredentials('token ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789');
    assert.equal(redactCredentials(once), once);
  });
});

describe('credential-guard :: findCredentials', () => {
  it('returns typed, position-sorted matches', () => {
    const text = `start AKIAIOSFODNN7EXAMPLE middle ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 end`;
    const found = findCredentials(text);
    assert.equal(found.length >= 2, true);
    const types = found.map(f => f.type);
    assert.ok(types.includes('aws_access_key'));
    assert.ok(types.includes('github_pat'));
    // Sorted ascending by start offset.
    for (let i = 1; i < found.length; i++) {
      assert.ok(found[i].start >= found[i - 1].start);
    }
    // Each reported slice equals the real matched token.
    for (const f of found) {
      const slice = text.slice(f.start, f.start + f.length);
      assert.equal(containsCredential(slice) || f.type === 'generic_secret', true);
    }
  });

  it('labels a contextual secret as generic_secret with correct bounds', () => {
    const text = 'client_secret: "Zx9Qw3Lp8Rt2Vb6Nm4Kc1Df7Gh5Jk0"';
    const found = findCredentials(text);
    const gen = found.find(f => f.type === 'generic_secret');
    assert.ok(gen);
    assert.equal(text.slice(gen!.start, gen!.start + gen!.length), 'Zx9Qw3Lp8Rt2Vb6Nm4Kc1Df7Gh5Jk0');
  });

  it('returns [] for benign and short inputs', () => {
    assert.deepEqual(findCredentials('hello world no secrets'), []);
    assert.deepEqual(findCredentials('short'), []);
    assert.deepEqual(findCredentials(''), []);
  });
});

describe('credential-guard :: CredentialGuard instance', () => {
  it('records detections on scan and reports count/types', () => {
    const g = new CredentialGuard();
    assert.equal(g.getDetectionCount(), 0);
    const hit = g.scan('leaked AKIAIOSFODNN7EXAMPLE here', 'test-source');
    assert.equal(hit, true);
    assert.equal(g.getDetectionCount(), 1);
    assert.equal(g.getDetections()[0].type, 'aws_access_key');
    assert.equal(g.getDetections()[0].source, 'test-source');
  });

  it('returns false and records nothing for clean input', () => {
    const g = new CredentialGuard();
    assert.equal(g.scan('no secrets at all here', 'src'), false);
    assert.equal(g.getDetectionCount(), 0);
  });

  it('redact delegates to redactCredentials', () => {
    const g = new CredentialGuard();
    const out = g.redact('token ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789');
    assert.equal(out.includes(REDACT_PLACEHOLDER), true);
  });

  it('clear empties the detection log', () => {
    const g = new CredentialGuard();
    g.scan('AKIAIOSFODNN7EXAMPLE', 's');
    assert.equal(g.getDetectionCount(), 1);
    g.clear();
    assert.equal(g.getDetectionCount(), 0);
    assert.deepEqual([...g.getDetections()], []);
  });
});

// ---------------------------------------------------------------------------
// env-protection
// ---------------------------------------------------------------------------

describe('env-protection :: isPinnedVariable / getPinCategory', () => {
  const cases: Array<[string, string | null]> = [
    ['PATH', 'code_loading'],
    ['LD_PRELOAD', 'code_loading'],
    ['NODE_OPTIONS', 'code_loading'],
    ['SUPERINFERENCE_CONFIG_DIR', 'policy'],
    ['USE_LOCAL_OAUTH', 'policy'],
    ['DISABLE_TELEMETRY', 'telemetry'],
    ['DO_NOT_TRACK', 'telemetry'],
    ['SOME_RANDOM_VAR', null],
    ['HOME', null],
  ];
  for (const [name, cat] of cases) {
    it(`${name} -> category ${cat}`, () => {
      assert.equal(getPinCategory(name), cat);
      assert.equal(isPinnedVariable(name), cat !== null);
    });
  }

  it('is case-insensitive on the variable name', () => {
    assert.equal(isPinnedVariable('path'), true);
    assert.equal(getPinCategory('ld_preload'), 'code_loading');
    assert.equal(getPinCategory('disable_telemetry'), 'telemetry');
  });

  it('every pinned var resolves to a valid PIN_CATEGORIES member', () => {
    for (const v of getAllPinnedVariables()) {
      const cat = getPinCategory(v);
      assert.ok(cat && (PIN_CATEGORIES as readonly string[]).includes(cat));
    }
  });

  it('getAllPinnedVariables unions the three sets', () => {
    const all = new Set(getAllPinnedVariables());
    for (const s of [POLICY_VARIABLES, CODE_LOADING_VARIABLES, TELEMETRY_VARIABLES]) {
      for (const v of s) assert.ok(all.has(v));
    }
    assert.equal(
      all.size,
      POLICY_VARIABLES.size + CODE_LOADING_VARIABLES.size + TELEMETRY_VARIABLES.size,
    );
  });
});

describe('env-protection :: EnvProtectionGuard', () => {
  it('blocks a statically pinned variable and records the attempt', () => {
    const g = new EnvProtectionGuard();
    const res = g.checkAndBlock('LD_PRELOAD', 'tool-x');
    assert.equal(res.blocked, true);
    assert.equal(res.variable, 'LD_PRELOAD');
    assert.equal(res.category, 'code_loading');
    assert.ok(res.reason.includes('pinned'));
    assert.equal(g.getBlockedCount(), 1);
    assert.equal(g.getBlockedAttempts()[0].source, 'tool-x');
  });

  it('allows a non-pinned variable without recording', () => {
    const g = new EnvProtectionGuard();
    const res = g.checkAndBlock('MY_APP_SETTING', 'tool-x');
    assert.equal(res.blocked, false);
    assert.equal(res.category, null);
    assert.equal(res.reason, '');
    assert.equal(g.getBlockedCount(), 0);
  });

  it('blocks a dynamically pinned var with category dynamic_policy', () => {
    const g = new EnvProtectionGuard();
    assert.equal(g.isDynamicallyPinned('CUSTOM_POLICY'), false);
    g.mergePinnedFromPolicy(['custom_policy']);
    assert.equal(g.isDynamicallyPinned('CUSTOM_POLICY'), true);
    const res = g.checkAndBlock('CUSTOM_POLICY', 'policy-src');
    assert.equal(res.blocked, true);
    assert.equal(res.category, 'dynamic_policy');
    // Dynamic-only pins are not static categories, so checkSet records nothing.
    assert.equal(g.getBlockedCount(), 0);
  });

  it('checkSet records only static-category attempts', () => {
    const g = new EnvProtectionGuard();
    assert.equal(g.checkSet('NOT_PINNED', 's'), null);
    const attempt = g.checkSet('path', 's');
    assert.ok(attempt);
    assert.equal(attempt!.variable, 'PATH');
    assert.equal(attempt!.category, 'code_loading');
    assert.equal(g.getBlockedCount(), 1);
  });

  it('clear empties blocked attempts', () => {
    const g = new EnvProtectionGuard();
    g.checkAndBlock('PATH', 's');
    assert.equal(g.getBlockedCount(), 1);
    g.clear();
    assert.equal(g.getBlockedCount(), 0);
  });
});

// ---------------------------------------------------------------------------
// safe-mode
// ---------------------------------------------------------------------------

describe('safe-mode :: isBlockedInSafeMode / isPartiallyBlocked', () => {
  const fullyBlocked: BlockedResource[] = [
    'prompt_override',
    'project_memory',
    'project_skills',
    'mcp_servers',
    'hooks',
    'plugins',
  ];
  for (const r of fullyBlocked) {
    it(`${r} is fully blocked`, () => {
      assert.equal(isBlockedInSafeMode(r), true);
      assert.equal(isPartiallyBlocked(r), false);
    });
  }

  it('project_config is partially, not fully, blocked', () => {
    assert.equal(isBlockedInSafeMode('project_config'), false);
    assert.equal(isPartiallyBlocked('project_config'), true);
  });

  it('BLOCKED_RESOURCES is partitioned into full and partial', () => {
    for (const r of BLOCKED_RESOURCES) {
      const full = isBlockedInSafeMode(r);
      const partial = isPartiallyBlocked(r);
      assert.equal(full && partial, false, `${r} cannot be both`);
      assert.equal(full || partial, true, `${r} must be one`);
    }
  });
});

describe('safe-mode :: shouldEnableSafeMode', () => {
  it('enables for an untrusted project', () => {
    assert.equal(shouldEnableSafeMode('/x', { untrustedProject: true }), true);
  });
  it('enables for a clone from an unknown source', () => {
    assert.equal(shouldEnableSafeMode('/x', { clonedFromUnknown: true }), true);
  });
  it('enables only when prompt-override AND untrusted-mcp combine', () => {
    assert.equal(shouldEnableSafeMode('/x', { hasPromptOverride: true, hasUntrustedMcp: true }), true);
    assert.equal(shouldEnableSafeMode('/x', { hasPromptOverride: true }), false);
    assert.equal(shouldEnableSafeMode('/x', { hasUntrustedMcp: true }), false);
  });
  it('does not enable with no indicators', () => {
    assert.equal(shouldEnableSafeMode('/x', {}), false);
  });
});

describe('safe-mode :: SafeModeGuard', () => {
  it('when disabled, allows everything and records nothing', () => {
    const g = new SafeModeGuard({ enabled: false });
    assert.equal(g.isEnabled(), false);
    assert.equal(g.check('hooks'), true);
    assert.equal(g.check('plugins', '/p'), true);
    assert.equal(g.getBlockedCount(), 0);
  });

  it('when enabled, blocks fully-blocked resources and logs them', () => {
    const g = new SafeModeGuard({ enabled: true, reason: 'untrusted' });
    assert.equal(g.isEnabled(), true);
    assert.equal(g.getReason(), 'untrusted');
    assert.equal(g.check('hooks', '/h'), false);
    assert.equal(g.check('mcp_servers'), false);
    assert.equal(g.getBlockedCount(), 2);
    assert.equal(g.getBlocked()[0].resource, 'hooks');
    assert.equal(g.getBlocked()[0].path, '/h');
  });

  it('when enabled, allows a partially-blocked resource', () => {
    const g = new SafeModeGuard({ enabled: true });
    assert.equal(g.getReason(), 'safe mode active');
    assert.equal(g.check('project_config'), true);
    assert.equal(g.getBlockedCount(), 0);
  });

  it('enable/disable toggles behavior', () => {
    const g = new SafeModeGuard({ enabled: false });
    assert.equal(g.check('plugins'), true);
    g.enable('now unsafe');
    assert.equal(g.isEnabled(), true);
    assert.equal(g.getReason(), 'now unsafe');
    assert.equal(g.check('plugins'), false);
    g.disable();
    assert.equal(g.isEnabled(), false);
    assert.equal(g.getReason(), '');
    assert.equal(g.check('plugins'), true);
  });

  it('getBlockedResources lists exactly the fully-blocked set', () => {
    const g = new SafeModeGuard({ enabled: true });
    const listed = new Set(g.getBlockedResources());
    for (const r of BLOCKED_RESOURCES) {
      assert.equal(listed.has(r), isBlockedInSafeMode(r));
    }
  });

  it('clear resets the blocked log', () => {
    const g = new SafeModeGuard({ enabled: true });
    g.check('hooks');
    assert.equal(g.getBlockedCount(), 1);
    g.clear();
    assert.equal(g.getBlockedCount(), 0);
  });
});

// ---------------------------------------------------------------------------
// env-access-guard
// ---------------------------------------------------------------------------

describe('env-access-guard :: isSensitiveEnvVar', () => {
  const sensitive = [
    'API_KEY',
    'OPENAI_API_KEY',
    'MY_SECRET_KEY',
    'DB_PASSWORD',
    'GITHUB_ACCESS_TOKEN',
    'service_private_key',
    'SOME_CREDENTIAL_X',
    'AUTH_TOKEN_V2',
  ];
  for (const n of sensitive) {
    it(`flags ${n} as sensitive`, () => assert.equal(isSensitiveEnvVar(n), true));
  }

  const benign = ['HOME', 'PATH', 'LANG', 'NODE_ENV', 'MY_SETTING', 'PORT'];
  for (const n of benign) {
    it(`treats ${n} as non-sensitive`, () => assert.equal(isSensitiveEnvVar(n), false));
  }

  it('is case-insensitive', () => {
    assert.equal(isSensitiveEnvVar('my_api_key'), true);
    assert.equal(isSensitiveEnvVar('password'), true);
  });

  it('every SENSITIVE_ENV_VARS entry matches itself', () => {
    for (const s of SENSITIVE_ENV_VARS) assert.equal(isSensitiveEnvVar(s), true);
  });
});

describe('env-access-guard :: canAccessEnv', () => {
  it('blocks org-plugin (prepend/append) gets through user tier', () => {
    assert.equal(canAccessEnv('X', 'get', 'user', 'prepend'), false);
    assert.equal(canAccessEnv('X', 'get', 'user', 'append'), false);
  });

  it('allows gets when reader tier is not org-plugin', () => {
    assert.equal(canAccessEnv('X', 'get', 'user', 'user'), true);
    assert.equal(canAccessEnv('X', 'get', 'user', 'engine'), true);
    assert.equal(canAccessEnv('X', 'get', 'user'), true);
  });

  it('allows gets when caller is not user tier', () => {
    assert.equal(canAccessEnv('X', 'get', 'engine', 'prepend'), true);
    assert.equal(canAccessEnv('X', 'get', 'builtin', 'append'), true);
  });

  it('allows set/delete regardless of tier (pinning handled elsewhere)', () => {
    for (const tier of ENV_ACCESS_TIERS) {
      assert.equal(canAccessEnv('X', 'set', tier), true);
      assert.equal(canAccessEnv('X', 'delete', tier), true);
    }
  });

  it('ENV_OPERATIONS covers get/set/delete', () => {
    assert.deepEqual([...ENV_OPERATIONS].sort(), ['delete', 'get', 'set']);
  });
});

describe('env-access-guard :: EnvAccessGuard', () => {
  it('logs every check and computes allowed per policy', () => {
    const g = new EnvAccessGuard();
    assert.equal(g.check('X', 'get', 'user', 'prepend'), false);
    assert.equal(g.check('Y', 'get', 'engine'), true);
    assert.equal(g.getEvents().length, 2);
    assert.equal(g.getBlockedCount(), 1);
    assert.equal(g.getBlockedEvents().length, 1);
    assert.equal(g.getBlockedEvents()[0].variable, 'X');
  });

  it('respects maxEvents cap', () => {
    const g = new EnvAccessGuard(1);
    g.check('A', 'get', 'user');
    g.check('B', 'get', 'user');
    assert.equal(g.getEvents().length, 1);
  });

  it('safeGetEnv redacts sensitive values but reports existence', () => {
    const g = new EnvAccessGuard();
    const name = 'POV_MATRIX_API_KEY';
    process.env[name] = 'super-secret-value';
    try {
      const r = g.safeGetEnv(name);
      assert.equal(r.redacted, true);
      assert.equal(r.value, '[REDACTED]');
    } finally {
      delete process.env[name];
    }
  });

  it('safeGetEnv reports undefined for an unset sensitive var', () => {
    const g = new EnvAccessGuard();
    const r = g.safeGetEnv('POV_MATRIX_UNSET_SECRET_KEY');
    assert.equal(r.redacted, true);
    assert.equal(r.value, undefined);
  });

  it('safeGetEnv passes through non-sensitive values unredacted', () => {
    const g = new EnvAccessGuard();
    const name = 'POV_MATRIX_PLAIN_SETTING';
    process.env[name] = 'plain';
    try {
      const r = g.safeGetEnv(name);
      assert.equal(r.redacted, false);
      assert.equal(r.value, 'plain');
    } finally {
      delete process.env[name];
    }
  });

  it('clear empties the event log', () => {
    const g = new EnvAccessGuard();
    g.check('A', 'get', 'user', 'prepend');
    assert.equal(g.getEvents().length, 1);
    g.clear();
    assert.equal(g.getEvents().length, 0);
    assert.equal(g.getBlockedCount(), 0);
  });
});

// ---------------------------------------------------------------------------
// code-security-patterns
// ---------------------------------------------------------------------------

function ids(content: string, file: string): string[] {
  return scanCodeForSecurityPatterns(content, file).map(f => f.patternId);
}

describe('code-security-patterns :: scanCodeForSecurityPatterns positives', () => {
  it('flags eval() in JS', () => {
    assert.ok(ids('const x = eval("1+1");', 'a.js').includes('eval_injection'));
  });

  it('flags child_process.exec / execSync in JS only', () => {
    assert.ok(ids('child_process.exec(cmd);', 'a.js').includes('child_process_exec'));
    assert.ok(ids('execSync(cmd);', 'a.ts').includes('child_process_exec'));
    // Python file: JS-only pattern must not fire.
    assert.ok(!ids('execSync(cmd);', 'a.py').includes('child_process_exec'));
  });

  it('flags new Function()', () => {
    assert.ok(ids('const f = new Function("return 1");', 'a.js').includes('new_function_injection'));
  });

  it('flags innerHTML / outerHTML / insertAdjacentHTML / document.write', () => {
    assert.ok(ids('el.innerHTML = user;', 'a.js').includes('innerHTML_xss'));
    assert.ok(ids('el.outerHTML = user;', 'a.js').includes('outerHTML_xss'));
    assert.ok(ids('el.insertAdjacentHTML("beforeend", user);', 'a.js').includes('insertAdjacentHTML_xss'));
    assert.ok(ids('document.write(user);', 'a.js').includes('document_write'));
  });

  it('flags dangerouslySetInnerHTML', () => {
    assert.ok(ids('<div dangerouslySetInnerHTML={{__html: x}} />', 'a.tsx').includes('dangerously_set_html'));
  });

  it('flags os.system and subprocess shell=True in Python', () => {
    assert.ok(ids('os.system("rm -rf /")', 'a.py').includes('os_system_injection'));
    assert.ok(ids('subprocess.run(cmd, shell=True)', 'a.py').includes('subprocess_shell'));
  });

  it('flags pickle / unsafe yaml / torch.load in Python', () => {
    assert.ok(ids('data = pickle.loads(blob)', 'a.py').includes('pickle_load'));
    assert.ok(ids('cfg = yaml.load(f)', 'a.py').includes('unsafe_yaml_load'));
    assert.ok(ids('m = torch.load("model.pt")', 'a.py').includes('torch_unsafe_load'));
  });

  it('does not flag torch.load with weights_only=True', () => {
    assert.ok(!ids('m = torch.load("model.pt", weights_only=True)', 'a.py').includes('torch_unsafe_load'));
  });

  it('flags Go exec.Command with shell', () => {
    assert.ok(ids('exec.Command("sh", "-c", cmd)', 'a.go').includes('go_exec_shell'));
  });

  it('flags disabled TLS verification across languages', () => {
    assert.ok(ids('requests.get(u, verify=False)', 'a.py').includes('tls_verify_disabled'));
    assert.ok(ids('{ rejectUnauthorized: false }', 'a.js').includes('tls_verify_disabled'));
  });

  it('flags deprecated crypto and AES-ECB', () => {
    assert.ok(ids('crypto.createCipher("aes", k)', 'a.js').includes('deprecated_crypto'));
    assert.ok(ids('const algo = "aes-256-ecb";', 'a.js').includes('aes_ecb_mode'));
  });

  it('reports correct 1-based line numbers', () => {
    const content = 'line one\nconst x = eval("1");\nline three';
    const findings = scanCodeForSecurityPatterns(content, 'a.js');
    const ev = findings.find(f => f.patternId === 'eval_injection');
    assert.ok(ev);
    assert.equal(ev!.line, 2);
  });

  it('deduplicates the same pattern on the same line', () => {
    const findings = scanCodeForSecurityPatterns('eval(eval("x"))', 'a.js');
    const evals = findings.filter(f => f.patternId === 'eval_injection');
    assert.equal(evals.length, 1);
  });
});

describe('code-security-patterns :: scanCodeForSecurityPatterns negatives', () => {
  it('does not flag eval in a markdown doc (excluded extension)', () => {
    assert.ok(!ids('Use eval() carefully in your code.', 'README.md').includes('eval_injection'));
  });

  it('does not flag member access like foo.eval(', () => {
    assert.ok(!ids('myObj.eval(x)', 'a.js').includes('eval_injection'));
  });

  it('does not flag Python-only patterns in a JS file', () => {
    assert.deepEqual(ids('os.system("x")', 'a.js'), []);
  });

  it('returns [] for clean code', () => {
    assert.deepEqual(scanCodeForSecurityPatterns('const x = 1 + 2;\nexport default x;', 'a.ts'), []);
  });

  it('returns [] for empty content', () => {
    assert.deepEqual(scanCodeForSecurityPatterns('', 'a.js'), []);
  });

  it('every SECURITY_PATTERNS id has unique, non-empty metadata', () => {
    const seen = new Set<string>();
    for (const p of SECURITY_PATTERNS) {
      assert.ok(p.id.length > 0);
      assert.ok(p.message.length > 0);
      assert.ok(p.severity === 'high' || p.severity === 'medium');
      assert.equal(seen.has(p.id), false);
      seen.add(p.id);
    }
  });
});
