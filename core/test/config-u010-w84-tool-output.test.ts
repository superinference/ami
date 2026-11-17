import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  resolveToolOutputMaxChars,
  DEFAULT_TOOL_OUTPUT_MAX_CHARS,
  TOOL_OUTPUT_MAX_CHARS_ENV,
  TOOL_OUTPUT_MAX_UPPER_LIMIT,
  validateConfigFileContent,
  mergeConfigs,
  findUnknownConfigKeys,
  getConfigJSONSchema,
} from '../src/config';
import { configTool, SUPPORTED_SETTINGS } from '../src/tools/config-tool';
import { webFetchTool } from '../src/tools/web-fetch';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// resolveToolOutputMaxChars — global default clamp (parity: getPersistenceThreshold)
// ---------------------------------------------------------------------------

describe('resolveToolOutputMaxChars', () => {
  const ENV = TOOL_OUTPUT_MAX_CHARS_ENV;
  afterEach(() => { delete process.env[ENV]; });

  it('clamps a declared cap DOWN to the built-in global default when nothing is configured', () => {
    assert.equal(resolveToolOutputMaxChars(1_000_000), DEFAULT_TOOL_OUTPUT_MAX_CHARS);
  });

  it('keeps a declared cap that is already below the global default (tool asks for less)', () => {
    assert.equal(resolveToolOutputMaxChars(1000), 1000);
  });

  it('an explicit config value overrides the default and clamps the declared cap', () => {
    assert.equal(resolveToolOutputMaxChars(50_000, 2000), 2000);
    // A config value ABOVE the declared cap cannot raise it past the declared cap.
    assert.equal(resolveToolOutputMaxChars(800, 5000), 800);
  });

  it('the env knob is honored when no explicit config value is given', () => {
    process.env[ENV] = '1234';
    assert.equal(resolveToolOutputMaxChars(50_000), 1234);
  });

  it('an explicit config value wins over the env knob', () => {
    process.env[ENV] = '9999';
    assert.equal(resolveToolOutputMaxChars(50_000, 500), 500);
  });

  it('floors a fractional config value and ignores non-positive values', () => {
    assert.equal(resolveToolOutputMaxChars(50_000, 2000.9), 2000);
    // Zero / negative config falls back to the default (then clamps declared down).
    assert.equal(resolveToolOutputMaxChars(50_000, 0), DEFAULT_TOOL_OUTPUT_MAX_CHARS);
    assert.equal(resolveToolOutputMaxChars(50_000, -10), DEFAULT_TOOL_OUTPUT_MAX_CHARS);
  });

  it('caps a huge config value at the hard upper limit', () => {
    assert.equal(
      resolveToolOutputMaxChars(TOOL_OUTPUT_MAX_UPPER_LIMIT + 5_000_000, TOOL_OUTPUT_MAX_UPPER_LIMIT * 10),
      TOOL_OUTPUT_MAX_UPPER_LIMIT,
    );
  });

  it('treats a non-finite declared cap as a deliberate opt-out (never spill)', () => {
    assert.equal(resolveToolOutputMaxChars(Infinity), Infinity);
    assert.equal(resolveToolOutputMaxChars(Infinity, 100), Infinity);
  });
});

// ---------------------------------------------------------------------------
// toolOutputMaxChars config key — schema / merge / settable registry wiring
// ---------------------------------------------------------------------------

describe('toolOutputMaxChars config key wiring', () => {
  it('is a recognized key (not reported as unknown)', () => {
    assert.deepEqual(findUnknownConfigKeys({ toolOutputMaxChars: 5000 }), []);
  });

  it('is present in the generated JSON schema as a number field', () => {
    const schema = getConfigJSONSchema();
    assert.ok(schema.properties.toolOutputMaxChars, 'schema must include toolOutputMaxChars');
    assert.equal(schema.properties.toolOutputMaxChars.type, 'number');
  });

  it('merges across layers (higher layer wins)', () => {
    // project config (higher) overrides global config (lower).
    const merged = mergeConfigs({}, {}, { toolOutputMaxChars: 8000 }, { toolOutputMaxChars: 1000 });
    assert.equal(merged.toolOutputMaxChars, 8000);
  });

  it('is get/set-able through the config tool registry', () => {
    const entry = SUPPORTED_SETTINGS.toolOutputMaxChars;
    assert.ok(entry, 'config tool must support toolOutputMaxChars');
    assert.equal(entry.type, 'number');
    // Write-time validation rejects a non-positive value.
    assert.equal(entry.validateOnWrite!(-1).valid, false);
    assert.equal(entry.validateOnWrite!(5000).valid, true);
  });
});

// ---------------------------------------------------------------------------
// validateConfigFileContent — strict, schema-aware content validation
// ---------------------------------------------------------------------------

describe('validateConfigFileContent', () => {
  it('accepts a valid config and returns the full schema', () => {
    const r = validateConfigFileContent('{"model":"opus","maxTurns":40}');
    assert.equal(r.valid, true);
    assert.equal(r.parseError, undefined);
    assert.deepEqual(r.issues, []);
    assert.deepEqual(r.unknownKeys, []);
    assert.ok(r.schema.properties.model, 'returned schema must describe known keys');
  });

  it('reports a parse error (and still returns the schema) for malformed JSON', () => {
    const r = validateConfigFileContent('{ not json');
    assert.equal(r.valid, false);
    assert.match(r.parseError!, /Invalid JSON/);
    assert.ok(r.schema.properties.model);
  });

  it('reports a shape error when the content is not a JSON object', () => {
    const r = validateConfigFileContent('[1,2,3]');
    assert.equal(r.valid, false);
    assert.match(r.parseError!, /Expected a JSON object/);
  });

  it('reports field-level value issues (bad type / out-of-range)', () => {
    const r = validateConfigFileContent('{"maxTurns":-5,"safeMode":"yes"}');
    assert.equal(r.valid, false);
    assert.ok(r.issues.length >= 1, 'must surface per-field issues');
    const fields = r.issues.map(i => i.field);
    assert.ok(fields.includes('safeMode'), 'bad boolean type must be flagged');
  });

  it('reports unrecognized top-level keys', () => {
    const r = validateConfigFileContent('{"modle":"opus"}');
    assert.equal(r.valid, false);
    assert.deepEqual(r.unknownKeys, ['modle']);
  });
});

// ---------------------------------------------------------------------------
// config tool — validate action (end-to-end through the tool)
// ---------------------------------------------------------------------------

describe('config tool validate action', () => {
  let tmp: string;
  before(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-validate-')); });
  after(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

  const ctx = (): ToolContext => ({ cwd: tmp, abortSignal: new AbortController().signal });

  it('reports a clean config as valid', async () => {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(tmp, '.ami', 'config.json'), '{"model":"opus"}');
    const res = await configTool.execute({ action: 'validate' }, ctx());
    assert.ok(!res.isError, res.output);
    assert.match(res.output, /is valid/);
    assert.equal(res.metadata!.valid, true);
  });

  it('lists every bad value / unknown key plus the valid keys for a broken config', async () => {
    fs.writeFileSync(path.join(tmp, '.ami', 'config.json'), '{"maxTurns":-5,"modle":"x"}');
    const res = await configTool.execute({ action: 'validate' }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /maxTurns/);
    assert.match(res.output, /unknown key "modle"/);
    assert.match(res.output, /did you mean "model"/);
    assert.match(res.output, /Valid keys:/);
    assert.equal(res.metadata!.valid, false);
  });

  it('treats an absent config file as nothing-to-validate (not an error)', async () => {
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-empty-'));
    try {
      const res = await configTool.execute({ action: 'validate' }, { cwd: empty, abortSignal: new AbortController().signal });
      assert.ok(!res.isError, res.output);
      assert.match(res.output, /No project config/);
    } finally {
      fs.rmSync(empty, { recursive: true, force: true });
    }
  });

  it('round-trips a toolOutputMaxChars set then validate', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-set-'));
    try {
      const c: ToolContext = { cwd: dir, abortSignal: new AbortController().signal };
      const set = await configTool.execute({ action: 'set', key: 'toolOutputMaxChars', value: 4096 }, c);
      assert.ok(!set.isError, set.output);
      const got = await configTool.execute({ action: 'get', key: 'toolOutputMaxChars' }, c);
      assert.match(got.output, /4096/);
      const val = await configTool.execute({ action: 'validate' }, c);
      assert.ok(!val.isError, val.output);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// web_fetch honors the global tool-output cap (end-to-end integration)
// ---------------------------------------------------------------------------

describe('web_fetch honors toolOutputMaxChars / AMI_TOOL_OUTPUT_MAX_CHARS', () => {
  let server: http.Server;
  let base: string;
  const BODY_LEN = 20_000;
  const ENV = TOOL_OUTPUT_MAX_CHARS_ENV;

  before(async () => {
    const s = http.createServer((req, res) => {
      // Fixed long text body; the query only varies the cache key between calls.
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('a'.repeat(BODY_LEN));
    });
    await new Promise<void>(resolve => s.listen(0, '127.0.0.1', () => resolve()));
    server = s;
    base = `http://127.0.0.1:${(s.address() as { port: number }).port}`;
  });
  after(() => { server.close(); });
  afterEach(() => { delete process.env[ENV]; });

  const ctx = (cwd: string): ToolContext => ({
    cwd,
    abortSignal: new AbortController().signal,
    _allowLocalhostForTesting: true,
  });

  function readOffset(output: string): number | null {
    const m = output.match(/Use offset=(\d+) to continue reading/);
    return m ? parseInt(m[1], 10) : null;
  }

  it('returns the whole body (no truncation) at the default 50k cap', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'wf-default-'));
    try {
      const res = await webFetchTool.execute({ url: `${base}/?n=default` }, ctx(tmp));
      assert.ok(!res.isError, res.output);
      assert.equal(readOffset(res.output), null, `20k body < 50k default must NOT truncate; got: ${res.output.slice(-160)}`);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('truncates at a small env-configured cap and reports a continue-reading offset', async () => {
    process.env[ENV] = '5000';
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'wf-env-'));
    try {
      const res = await webFetchTool.execute({ url: `${base}/?n=env` }, ctx(tmp));
      assert.ok(!res.isError, res.output);
      const off = readOffset(res.output);
      assert.notEqual(off, null, `expected truncation at cap 5000; got: ${res.output.slice(-200)}`);
      assert.ok(off! > 0 && off! <= 5000, `offset ${off} must be within the 5000 cap`);
      assert.match(res.output, /characters unread/);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('truncates at a small project-config cap (toolOutputMaxChars)', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'wf-cfg-'));
    try {
      fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
      fs.writeFileSync(path.join(tmp, '.ami', 'config.json'), '{"toolOutputMaxChars":3000}');
      const res = await webFetchTool.execute({ url: `${base}/?n=cfg` }, ctx(tmp));
      assert.ok(!res.isError, res.output);
      const off = readOffset(res.output);
      assert.notEqual(off, null, `expected truncation at cap 3000; got: ${res.output.slice(-200)}`);
      assert.ok(off! > 0 && off! <= 3000, `offset ${off} must be within the 3000 cap`);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
