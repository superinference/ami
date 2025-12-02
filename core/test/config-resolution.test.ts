import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';

// ---------------------------------------------------------------------------
// Pure resolution function that mirrors the logic in
// vscode/src/core-adapter.ts :: getProviderConfigFromSettings()
//
// We extract this into a testable function so we can verify the priority chain
// without importing the VSCode API.
// ---------------------------------------------------------------------------

interface ResolveConfigOpts {
  /** Explicitly user-set VSCode baseUrl (NOT the default from package.json) */
  userBaseUrl?: string;
  /** Explicitly user-set VSCode apiKey */
  userApiKey?: string;
  /** Explicitly user-set VSCode model */
  userModel?: string;
  /** AI_BASE_URL env var */
  envAiBaseUrl?: string;
  /** AI_API_KEY env var */
  envAiApiKey?: string;
  /** AI_MODEL env var */
  envAiModel?: string;
  /** GOOGLE_API_KEY env var */
  envGoogleApiKey?: string;
}

interface ProviderConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
}

const OPENAI_DEFAULT_URL = 'https://api.openai.com/v1';
const GEMINI_URL = 'https://generativelanguage.googleapis.com/v1beta/openai';

function resolveConfig(opts: ResolveConfigOpts): ProviderConfig {
  const googleKey = opts.envGoogleApiKey || '';
  const hasGoogleKey = !!googleKey;

  const apiKey = opts.userApiKey || opts.envAiApiKey || googleKey;
  const isGeminiKey = !opts.userApiKey && !opts.envAiApiKey && hasGoogleKey;

  return {
    baseUrl:
      opts.userBaseUrl ||
      opts.envAiBaseUrl ||
      (isGeminiKey ? GEMINI_URL : OPENAI_DEFAULT_URL),
    apiKey,
    model:
      opts.userModel ||
      opts.envAiModel ||
      (isGeminiKey ? 'gemini-2.0-flash' : 'gpt-4o'),
  };
}

// ---------------------------------------------------------------------------
// .env parser — mirrors the loadDotEnv logic from core-adapter.ts
// ---------------------------------------------------------------------------

function parseDotEnv(content: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const value = trimmed.slice(eqIdx + 1).trim();
    result[key] = value;
  }
  return result;
}

/**
 * Simulates loadDotEnv: applies parsed values to the given env object only
 * when the key is not already set (matching the real "don't overwrite" logic).
 */
function applyDotEnv(
  parsed: Record<string, string>,
  env: Record<string, string | undefined>,
): void {
  for (const [key, value] of Object.entries(parsed)) {
    if (!env[key]) {
      env[key] = value;
    }
  }
}

// ===========================================================================
// Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. VSCode settings defaults vs user values
// ---------------------------------------------------------------------------
describe('VSCode settings defaults vs user values', () => {
  it('default baseUrl should NOT override AI_BASE_URL env var', () => {
    // Bug scenario: VSCode package.json declares
    //   "default": "https://api.openai.com/v1"
    // but vscode.workspace.getConfiguration().get() returns that default.
    // The resolution must use `inspect()` to distinguish defaults from
    // user-set values. Here, userBaseUrl is undefined because the user never
    // explicitly changed it.
    const cfg = resolveConfig({
      envAiBaseUrl: 'https://my-custom-endpoint.example.com/v1',
      envAiApiKey: 'sk-test-key',
    });
    assert.equal(cfg.baseUrl, 'https://my-custom-endpoint.example.com/v1');
  });

  it('default baseUrl should NOT override GOOGLE_API_KEY auto-detection', () => {
    // Same bug: the VSCode default for baseUrl is the OpenAI URL,
    // but if only GOOGLE_API_KEY is set we should detect Gemini.
    const cfg = resolveConfig({ envGoogleApiKey: 'AIza-google-test' });
    assert.equal(cfg.baseUrl, GEMINI_URL);
    assert.equal(cfg.model, 'gemini-2.0-flash');
  });

  it('explicitly user-set baseUrl SHOULD override env vars', () => {
    const cfg = resolveConfig({
      userBaseUrl: 'https://user-chose-this.example.com/v1',
      envAiBaseUrl: 'https://env-endpoint.example.com/v1',
      envAiApiKey: 'sk-test',
    });
    assert.equal(cfg.baseUrl, 'https://user-chose-this.example.com/v1');
  });

  it('explicitly user-set model SHOULD override env and Gemini auto-detect', () => {
    const cfg = resolveConfig({
      userModel: 'claude-3.5-sonnet',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.model, 'claude-3.5-sonnet');
    // baseUrl should still be Gemini because userBaseUrl is not set
    assert.equal(cfg.baseUrl, GEMINI_URL);
  });

  it('explicitly user-set apiKey SHOULD override all env vars', () => {
    const cfg = resolveConfig({
      userApiKey: 'sk-user-explicit',
      envAiApiKey: 'sk-env-ai',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.apiKey, 'sk-user-explicit');
    // With userApiKey set, isGeminiKey is false -> OpenAI defaults
    assert.equal(cfg.baseUrl, OPENAI_DEFAULT_URL);
    assert.equal(cfg.model, 'gpt-4o');
  });
});

// ---------------------------------------------------------------------------
// 2. GOOGLE_API_KEY auto-detection
// ---------------------------------------------------------------------------
describe('GOOGLE_API_KEY auto-detection', () => {
  it('auto-configures Gemini when only GOOGLE_API_KEY is set', () => {
    const cfg = resolveConfig({ envGoogleApiKey: 'AIza-test-key-123' });
    assert.equal(cfg.baseUrl, GEMINI_URL);
    assert.equal(cfg.model, 'gemini-2.0-flash');
    assert.equal(cfg.apiKey, 'AIza-test-key-123');
  });

  it('does NOT auto-configure Gemini when AI_API_KEY is also set', () => {
    const cfg = resolveConfig({
      envAiApiKey: 'sk-openai-key',
      envGoogleApiKey: 'AIza-google-key',
    });
    // AI_API_KEY takes priority -> not a Gemini key scenario
    assert.equal(cfg.apiKey, 'sk-openai-key');
    assert.equal(cfg.baseUrl, OPENAI_DEFAULT_URL);
    assert.equal(cfg.model, 'gpt-4o');
  });

  it('does NOT auto-configure Gemini when user apiKey is set', () => {
    const cfg = resolveConfig({
      userApiKey: 'sk-user-custom',
      envGoogleApiKey: 'AIza-google-key',
    });
    assert.equal(cfg.apiKey, 'sk-user-custom');
    assert.equal(cfg.baseUrl, OPENAI_DEFAULT_URL);
    assert.equal(cfg.model, 'gpt-4o');
  });

  it('falls back to empty apiKey when nothing is set', () => {
    const cfg = resolveConfig({});
    assert.equal(cfg.apiKey, '');
    assert.equal(cfg.baseUrl, OPENAI_DEFAULT_URL);
    assert.equal(cfg.model, 'gpt-4o');
  });
});

// ---------------------------------------------------------------------------
// 3. Double-nesting prevention
// ---------------------------------------------------------------------------
describe('double-nesting prevention', () => {
  it('resolveConfig returns a flat {baseUrl, apiKey, model} object', () => {
    const cfg = resolveConfig({ envAiApiKey: 'sk-test' });
    // Must have exactly the three expected keys
    const keys = Object.keys(cfg).sort();
    assert.deepEqual(keys, ['apiKey', 'baseUrl', 'model']);
  });

  it('result must NOT contain a nested provider key', () => {
    const cfg = resolveConfig({ envGoogleApiKey: 'AIza-test' }) as Record<string, unknown>;
    assert.equal('provider' in cfg, false, 'provider must not be nested inside provider');
  });

  it('EngineConfig.provider shape is flat after assignment', () => {
    // Simulate what createEngine does: assign the resolved provider to
    // EngineConfig.provider and verify the shape.
    const provider = resolveConfig({ envAiApiKey: 'sk-test' });
    const engineConfig = {
      provider,
      cwd: '/tmp',
      tools: [],
    };
    assert.equal(typeof engineConfig.provider.baseUrl, 'string');
    assert.equal(typeof engineConfig.provider.apiKey, 'string');
    assert.equal(typeof engineConfig.provider.model, 'string');
    assert.equal('provider' in engineConfig.provider, false);
  });
});

// ---------------------------------------------------------------------------
// 4. .env file loading
// ---------------------------------------------------------------------------
describe('.env file loading', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-config-test-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('parses GOOGLE_API_KEY from a .env file', () => {
    const envContent = 'GOOGLE_API_KEY=test123\n';
    const parsed = parseDotEnv(envContent);
    assert.equal(parsed.GOOGLE_API_KEY, 'test123');
  });

  it('parses multiple keys from .env', () => {
    const envContent = [
      'AI_API_KEY=sk-abc123',
      'AI_BASE_URL=https://custom.example.com/v1',
      'AI_MODEL=gpt-4-turbo',
      'GOOGLE_API_KEY=AIza-xyz',
    ].join('\n');
    const parsed = parseDotEnv(envContent);
    assert.equal(parsed.AI_API_KEY, 'sk-abc123');
    assert.equal(parsed.AI_BASE_URL, 'https://custom.example.com/v1');
    assert.equal(parsed.AI_MODEL, 'gpt-4-turbo');
    assert.equal(parsed.GOOGLE_API_KEY, 'AIza-xyz');
  });

  it('skips comment lines and blank lines', () => {
    const envContent = [
      '# This is a comment',
      '',
      '  # Another comment',
      'FOO=bar',
      '',
    ].join('\n');
    const parsed = parseDotEnv(envContent);
    assert.deepEqual(parsed, { FOO: 'bar' });
  });

  it('skips lines without an equals sign', () => {
    const envContent = 'NO_EQUALS\nGOOD_KEY=good_value\n';
    const parsed = parseDotEnv(envContent);
    assert.equal(parsed.GOOD_KEY, 'good_value');
    assert.equal('NO_EQUALS' in parsed, false);
  });

  it('handles values containing equals signs', () => {
    const envContent = 'KEY=value=with=equals\n';
    const parsed = parseDotEnv(envContent);
    assert.equal(parsed.KEY, 'value=with=equals');
  });

  it('writes and reads a real .env file from disk', () => {
    const envPath = path.join(tmpDir, '.env');
    fs.writeFileSync(envPath, 'GOOGLE_API_KEY=test123\nAI_MODEL=gpt-4o\n');

    const content = fs.readFileSync(envPath, 'utf8');
    const parsed = parseDotEnv(content);
    assert.equal(parsed.GOOGLE_API_KEY, 'test123');
    assert.equal(parsed.AI_MODEL, 'gpt-4o');
  });

  it('applyDotEnv does not overwrite existing env values', () => {
    const env: Record<string, string | undefined> = {
      GOOGLE_API_KEY: 'already-set',
    };
    const parsed = { GOOGLE_API_KEY: 'from-dotenv', NEW_KEY: 'new-value' };
    applyDotEnv(parsed, env);
    assert.equal(env.GOOGLE_API_KEY, 'already-set');
    assert.equal(env.NEW_KEY, 'new-value');
  });

  it('end-to-end: .env GOOGLE_API_KEY feeds into resolveConfig', () => {
    const envContent = 'GOOGLE_API_KEY=AIza-from-dotenv\n';
    const parsed = parseDotEnv(envContent);

    const cfg = resolveConfig({ envGoogleApiKey: parsed.GOOGLE_API_KEY });
    assert.equal(cfg.apiKey, 'AIza-from-dotenv');
    assert.equal(cfg.baseUrl, GEMINI_URL);
    assert.equal(cfg.model, 'gemini-2.0-flash');
  });
});

// ---------------------------------------------------------------------------
// 5. Priority chain: userSetting > AI_API_KEY env > GOOGLE_API_KEY env > default
// ---------------------------------------------------------------------------
describe('priority chain', () => {
  it('userSetting wins over everything (apiKey)', () => {
    const cfg = resolveConfig({
      userApiKey: 'sk-user',
      envAiApiKey: 'sk-env',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.apiKey, 'sk-user');
  });

  it('AI_API_KEY wins over GOOGLE_API_KEY', () => {
    const cfg = resolveConfig({
      envAiApiKey: 'sk-ai-env',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.apiKey, 'sk-ai-env');
  });

  it('GOOGLE_API_KEY wins over empty/default', () => {
    const cfg = resolveConfig({
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.apiKey, 'AIza-google');
  });

  it('falls back to empty string when nothing is provided', () => {
    const cfg = resolveConfig({});
    assert.equal(cfg.apiKey, '');
  });

  it('userSetting wins over everything (baseUrl)', () => {
    const cfg = resolveConfig({
      userBaseUrl: 'https://user.example.com/v1',
      envAiBaseUrl: 'https://env.example.com/v1',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.baseUrl, 'https://user.example.com/v1');
  });

  it('AI_BASE_URL wins over Gemini auto-detect', () => {
    const cfg = resolveConfig({
      envAiBaseUrl: 'https://custom-base.example.com/v1',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.baseUrl, 'https://custom-base.example.com/v1');
    // apiKey still comes from Google since AI_API_KEY is not set
    assert.equal(cfg.apiKey, 'AIza-google');
  });

  it('Gemini auto-detect wins over OpenAI default', () => {
    const cfg = resolveConfig({ envGoogleApiKey: 'AIza-google' });
    assert.equal(cfg.baseUrl, GEMINI_URL);
    assert.notEqual(cfg.baseUrl, OPENAI_DEFAULT_URL);
  });

  it('OpenAI default is last resort for baseUrl', () => {
    const cfg = resolveConfig({});
    assert.equal(cfg.baseUrl, OPENAI_DEFAULT_URL);
  });

  it('userSetting wins over everything (model)', () => {
    const cfg = resolveConfig({
      userModel: 'my-custom-model',
      envAiModel: 'env-model',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.model, 'my-custom-model');
  });

  it('AI_MODEL wins over Gemini auto-detect', () => {
    const cfg = resolveConfig({
      envAiModel: 'gpt-4-turbo',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.model, 'gpt-4-turbo');
  });

  it('Gemini model auto-detect wins over OpenAI default', () => {
    const cfg = resolveConfig({ envGoogleApiKey: 'AIza-google' });
    assert.equal(cfg.model, 'gemini-2.0-flash');
  });

  it('gpt-4o is last resort for model', () => {
    const cfg = resolveConfig({});
    assert.equal(cfg.model, 'gpt-4o');
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------
describe('edge cases', () => {
  it('empty string env vars are treated as unset', () => {
    // The real code checks `process.env.GOOGLE_API_KEY || ''` and then
    // `!!googleKey`. An empty string is falsy, so it should NOT trigger
    // Gemini auto-detection.
    const cfg = resolveConfig({ envGoogleApiKey: '' });
    assert.equal(cfg.baseUrl, OPENAI_DEFAULT_URL);
    assert.equal(cfg.model, 'gpt-4o');
    assert.equal(cfg.apiKey, '');
  });

  it('all user settings provided — env vars are completely ignored', () => {
    const cfg = resolveConfig({
      userBaseUrl: 'https://user-url.example.com',
      userApiKey: 'sk-user-key',
      userModel: 'user-model',
      envAiBaseUrl: 'https://env-url.example.com',
      envAiApiKey: 'sk-env-key',
      envAiModel: 'env-model',
      envGoogleApiKey: 'AIza-ignored',
    });
    assert.equal(cfg.baseUrl, 'https://user-url.example.com');
    assert.equal(cfg.apiKey, 'sk-user-key');
    assert.equal(cfg.model, 'user-model');
  });

  it('AI_BASE_URL + AI_MODEL env but GOOGLE_API_KEY for auth', () => {
    // User has custom base URL and model via env, but only Google key.
    // Since AI_API_KEY is not set, apiKey falls through to Google.
    // But since AI_BASE_URL is set, baseUrl uses that (not Gemini URL).
    const cfg = resolveConfig({
      envAiBaseUrl: 'https://proxy.example.com/v1',
      envAiModel: 'custom-model',
      envGoogleApiKey: 'AIza-google',
    });
    assert.equal(cfg.baseUrl, 'https://proxy.example.com/v1');
    assert.equal(cfg.model, 'custom-model');
    assert.equal(cfg.apiKey, 'AIza-google');
  });

  it('only AI_API_KEY set — uses OpenAI defaults for baseUrl and model', () => {
    const cfg = resolveConfig({ envAiApiKey: 'sk-openai' });
    assert.equal(cfg.apiKey, 'sk-openai');
    assert.equal(cfg.baseUrl, OPENAI_DEFAULT_URL);
    assert.equal(cfg.model, 'gpt-4o');
  });
});
