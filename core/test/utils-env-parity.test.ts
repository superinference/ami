import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  parseEnvFloat,
  parseEnvBool,
  resolveEnvVar,
  parseDotenv,
  loadDotenv,
} from '../src/utils/env';

// ---------------------------------------------------------------------------
// Additive env-utils parity: float/bool coercion, .env parsing/loading, and
// env waterfall resolution. Every assertion discriminates the new behaviour
// (see the red-green notes in the lane report).
// ---------------------------------------------------------------------------

describe('parseEnvFloat', () => {
  it('parses decimals, separators and scientific notation without truncation', () => {
    assert.equal(parseEnvFloat('3.7', 0), 3.7);
    assert.equal(parseEnvFloat('1_000.5', 0), 1000.5);
    assert.equal(parseEnvFloat('2.5e2', 0), 250);
  });
  it('returns the fallback for undefined / empty / non-finite', () => {
    assert.equal(parseEnvFloat(undefined, 1.5), 1.5);
    assert.equal(parseEnvFloat('', 1.5), 1.5);
    assert.equal(parseEnvFloat('   ', 1.5), 1.5);
    assert.equal(parseEnvFloat('nope', 1.5), 1.5);
    assert.equal(parseEnvFloat('Infinity', 1.5), 1.5);
  });
});

describe('parseEnvBool', () => {
  it('treats the conventional truthy spellings as true', () => {
    for (const v of ['1', 'true', 'TRUE', 'Yes', 'on', 'y', 't', 'enabled', ' On ']) {
      assert.equal(parseEnvBool(v), true, `${v} should be true`);
    }
  });
  it('treats the conventional falsy spellings as false', () => {
    for (const v of ['0', 'false', 'No', 'off', 'n', 'f', 'disabled', '']) {
      assert.equal(parseEnvBool(v), false, `${v} should be false`);
    }
  });
  it('returns the fallback for undefined and unrecognised values', () => {
    assert.equal(parseEnvBool(undefined, true), true);
    assert.equal(parseEnvBool(undefined, false), false);
    assert.equal(parseEnvBool('maybe', true), true);
    assert.equal(parseEnvBool('maybe', false), false);
  });
});

describe('resolveEnvVar waterfall', () => {
  it('returns the first set, non-empty candidate', () => {
    const env = { A: '', B: 'second', C: 'third' };
    assert.equal(resolveEnvVar(['A', 'B', 'C'], env), 'second');
  });
  it('skips set-but-empty by default but can accept it with allowEmpty', () => {
    const env = { A: '', B: 'x' };
    assert.equal(resolveEnvVar(['A', 'B'], env), 'x');
    assert.equal(resolveEnvVar(['A', 'B'], env, { allowEmpty: true }), '');
  });
  it('returns undefined when no candidate is set', () => {
    assert.equal(resolveEnvVar(['NOPE1', 'NOPE2'], {}), undefined);
  });
  it('defaults to process.env', () => {
    const key = '__ENV_WATERFALL_TEST__';
    process.env[key] = 'from-proc';
    try {
      assert.equal(resolveEnvVar([key]), 'from-proc');
    } finally {
      delete process.env[key];
    }
  });
});

describe('parseDotenv', () => {
  it('parses plain pairs and strips the export prefix', () => {
    const r = parseDotenv('FOO=bar\nexport BAZ=qux');
    assert.equal(r.FOO, 'bar');
    assert.equal(r.BAZ, 'qux');
  });

  it('ignores blank lines and comment lines', () => {
    const r = parseDotenv('\n# a comment\n   # indented comment\nFOO=bar\n');
    assert.deepEqual(r, { FOO: 'bar' });
  });

  it('strips inline comments from unquoted values but keeps `#` without a space', () => {
    const r = parseDotenv('A=hello # trailing comment\nB=no#space');
    assert.equal(r.A, 'hello');
    assert.equal(r.B, 'no#space');
  });

  it('keeps `#` literally inside quoted values', () => {
    const r = parseDotenv('A="a # b"\nB=\'c # d\'');
    assert.equal(r.A, 'a # b');
    assert.equal(r.B, 'c # d');
  });

  it('treats single quotes as literal (no escapes, no expansion)', () => {
    process.env.__DOTENV_X = 'EXP';
    try {
      const r = parseDotenv("A='literal \\n $__DOTENV_X'");
      assert.equal(r.A, 'literal \\n $__DOTENV_X');
    } finally {
      delete process.env.__DOTENV_X;
    }
  });

  it('processes escapes inside double quotes', () => {
    const r = parseDotenv('A="line1\\nline2\\ttab"');
    assert.equal(r.A, 'line1\nline2\ttab');
  });

  it('expands ${VAR}, $VAR and ${VAR:-default} in unquoted/double-quoted values', () => {
    const r = parseDotenv('BASE=root\nP="${BASE}/sub"\nQ=$BASE-x\nD=${MISSING:-fallback}');
    assert.equal(r.P, 'root/sub');
    assert.equal(r.Q, 'root-x');
    assert.equal(r.D, 'fallback');
  });

  it('honours \\$ as a literal dollar (no expansion)', () => {
    const r = parseDotenv('A="price \\$5"');
    assert.equal(r.A, 'price $5');
  });

  it('prefers a set-but-empty value over the :- default', () => {
    const r = parseDotenv('E=\nV=${E:-fallback}');
    assert.equal(r.V, '');
  });

  it('supports multiline double-quoted values', () => {
    const r = parseDotenv('KEY="line1\nline2\nline3"\nNEXT=ok');
    assert.equal(r.KEY, 'line1\nline2\nline3');
    assert.equal(r.NEXT, 'ok');
  });

  it('lets later duplicate keys override earlier ones', () => {
    const r = parseDotenv('K=first\nK=second');
    assert.equal(r.K, 'second');
  });

  it('skips malformed keys and lines without `=`', () => {
    const r = parseDotenv('1BAD=x\nGOOD=y\njust-a-line\nhas space=z');
    assert.deepEqual(r, { GOOD: 'y' });
  });

  it('normalises CRLF and strips a leading BOM', () => {
    const r = parseDotenv('\uFEFFA=1\r\nB=2\r\n');
    assert.equal(r.A, '1');
    assert.equal(r.B, '2');
  });

  it('can disable expansion with expand:false', () => {
    const r = parseDotenv('BASE=root\nP=${BASE}', { expand: false });
    assert.equal(r.P, '${BASE}');
  });
});

describe('loadDotenv', () => {
  const tmps: string[] = [];
  const touched: string[] = [];
  afterEach(() => {
    for (const d of tmps.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    for (const k of touched.splice(0)) delete process.env[k];
  });

  function writeEnv(content: string): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dotenv-'));
    tmps.push(dir);
    const fp = path.join(dir, '.env');
    fs.writeFileSync(fp, content);
    return fp;
  }

  it('returns {} for a missing file', () => {
    assert.deepEqual(loadDotenv(path.join(os.tmpdir(), 'does-not-exist-xyz', '.env')), {});
  });

  it('parses a file without touching process.env when apply is not set', () => {
    const fp = writeEnv('__LD_A=one\n');
    const r = loadDotenv(fp);
    assert.equal(r.__LD_A, 'one');
    assert.equal(process.env.__LD_A, undefined);
  });

  it('applies to process.env but preserves existing values unless override', () => {
    touched.push('__LD_NEW', '__LD_EXIST');
    process.env.__LD_EXIST = 'original';
    const fp = writeEnv('__LD_NEW=added\n__LD_EXIST=replaced\n');
    loadDotenv(fp, { apply: true });
    assert.equal(process.env.__LD_NEW, 'added');
    assert.equal(process.env.__LD_EXIST, 'original');

    loadDotenv(fp, { apply: true, override: true });
    assert.equal(process.env.__LD_EXIST, 'replaced');
  });
});
