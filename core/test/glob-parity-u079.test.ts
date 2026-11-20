// Parity unit U079 — glob tool additive capabilities:
//  - richer `path` parameter description (anti-undefined/null)
//  - description advertises mtime-sort + delegate-open-ended-search-to-`task`
//  - AMI_GLOB_HIDDEN env toggle (include/exclude dotfiles)
//  - AMI_GLOB_NO_IGNORE env toggle (respect .gitignore when disabled)
//  - truncation notice carries an actionable "narrow your pattern/path" hint
import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  globTool,
  globHooks,
  globIncludesHidden,
  globRespectsGitignore,
  GLOB_HIDDEN_ENV,
  GLOB_NO_IGNORE_ENV,
  GLOB_RIPGREP_ENV,
} from '../src/tools/glob';
import type { ToolContext } from '../src/types';

function getProp(name: string): { type: string; description: string } {
  const schema = globTool.inputSchema as { properties: Record<string, { type: string; description: string }> };
  return schema.properties[name];
}

describe('U079 glob — richer descriptions', () => {
  it('path description steers away from literal "undefined"/"null" and requires a valid dir', () => {
    const d = getProp('path').description;
    assert.match(d, /Omit this field/i);
    assert.match(d, /undefined/);
    assert.match(d, /null/);
    assert.match(d, /valid directory/i);
  });

  it('tool description advertises modification-time sorting', () => {
    assert.match(globTool.description, /modification time/i);
  });

  it('tool description steers open-ended multi-round searches to the `task` sub-agent', () => {
    assert.match(globTool.description, /open-ended/i);
    assert.match(globTool.description, /`task`/);
  });

  it('tool description advertises that it scales to any codebase size', () => {
    // Parity with the reference's prompt bullet "works with any codebase size" so the
    // model knows glob is the right tool even on very large trees.
    assert.match(globTool.description, /any codebase size/i);
  });

  it('tool description shows concrete example glob patterns', () => {
    // Parity: the reference's prompt gives example patterns ("**/*.js" or "src/**/*.ts")
    // so the model has a template for the pattern syntax.
    assert.match(globTool.description, /\*\*\/\*\./);
    assert.match(globTool.description, /src\/\*\*\//);
  });
});

describe('U079 glob — env toggle parsers', () => {
  it('globIncludesHidden defaults true; only explicit falsey tokens disable', () => {
    assert.equal(globIncludesHidden({}), true);
    assert.equal(globIncludesHidden({ [GLOB_HIDDEN_ENV]: '' }), true);
    assert.equal(globIncludesHidden({ [GLOB_HIDDEN_ENV]: '1' }), true);
    assert.equal(globIncludesHidden({ [GLOB_HIDDEN_ENV]: 'false' }), false);
    assert.equal(globIncludesHidden({ [GLOB_HIDDEN_ENV]: '0' }), false);
    assert.equal(globIncludesHidden({ [GLOB_HIDDEN_ENV]: 'off' }), false);
  });

  it('globRespectsGitignore defaults false; only explicit falsey NO_IGNORE enables respect', () => {
    assert.equal(globRespectsGitignore({}), false);
    assert.equal(globRespectsGitignore({ [GLOB_NO_IGNORE_ENV]: '' }), false);
    assert.equal(globRespectsGitignore({ [GLOB_NO_IGNORE_ENV]: '1' }), false);
    assert.equal(globRespectsGitignore({ [GLOB_NO_IGNORE_ENV]: 'false' }), true);
    assert.equal(globRespectsGitignore({ [GLOB_NO_IGNORE_ENV]: 'no' }), true);
  });
});

describe('U079 glob — AMI_GLOB_HIDDEN integration (tinyglobby route)', () => {
  let tmpDir: string;
  let ctx: ToolContext;
  const origHidden = process.env[GLOB_HIDDEN_ENV];

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-hidden-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'visible.ts'), '1');
    fs.writeFileSync(path.join(tmpDir, '.hidden.ts'), '2');
  });
  after(() => {
    if (origHidden === undefined) delete process.env[GLOB_HIDDEN_ENV];
    else process.env[GLOB_HIDDEN_ENV] = origHidden;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('includes dotfiles by default', async () => {
    delete process.env[GLOB_HIDDEN_ENV];
    const r = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(r.output.includes('.hidden.ts'), 'dotfile must be listed by default');
    assert.ok(r.output.includes('visible.ts'));
  });

  it('excludes dotfiles when AMI_GLOB_HIDDEN=false', async () => {
    process.env[GLOB_HIDDEN_ENV] = 'false';
    const r = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(!r.output.includes('.hidden.ts'), 'dotfile must be hidden when toggled off');
    assert.ok(r.output.includes('visible.ts'));
  });
});

describe('U079 glob — AMI_GLOB_NO_IGNORE integration (tinyglobby route)', () => {
  let tmpDir: string;
  let ctx: ToolContext;
  const origNoIgnore = process.env[GLOB_NO_IGNORE_ENV];

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-ignore-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'kept.ts'), '1');
    fs.writeFileSync(path.join(tmpDir, 'ignored.ts'), '2');
    fs.writeFileSync(path.join(tmpDir, '.gitignore'), 'ignored.ts\n');
  });
  after(() => {
    if (origNoIgnore === undefined) delete process.env[GLOB_NO_IGNORE_ENV];
    else process.env[GLOB_NO_IGNORE_ENV] = origNoIgnore;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('ignores .gitignore by default (both files listed)', async () => {
    delete process.env[GLOB_NO_IGNORE_ENV];
    const r = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(r.output.includes('ignored.ts'), 'gitignored file still listed by default');
    assert.ok(r.output.includes('kept.ts'));
  });

  it('honors .gitignore when AMI_GLOB_NO_IGNORE=false (ignored file excluded)', async () => {
    process.env[GLOB_NO_IGNORE_ENV] = 'false';
    const r = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(!r.output.includes('ignored.ts'), 'gitignored file must be excluded');
    assert.ok(r.output.includes('kept.ts'));
  });
});

describe('U079 glob — ripgrep route threads the toggles into rg args', () => {
  let tmpDir: string;
  let ctx: ToolContext;
  const origExec = globHooks.execFileImpl;
  const origRg = globHooks.rgFiles;
  const origRgEnv = process.env[GLOB_RIPGREP_ENV];
  const origHidden = process.env[GLOB_HIDDEN_ENV];
  const origNoIgnore = process.env[GLOB_NO_IGNORE_ENV];

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-rg-args-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), '1');
  });
  afterEach(() => {
    globHooks.execFileImpl = origExec;
    globHooks.rgFiles = origRg;
  });
  after(() => {
    if (origRgEnv === undefined) delete process.env[GLOB_RIPGREP_ENV]; else process.env[GLOB_RIPGREP_ENV] = origRgEnv;
    if (origHidden === undefined) delete process.env[GLOB_HIDDEN_ENV]; else process.env[GLOB_HIDDEN_ENV] = origHidden;
    if (origNoIgnore === undefined) delete process.env[GLOB_NO_IGNORE_ENV]; else process.env[GLOB_NO_IGNORE_ENV] = origNoIgnore;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  async function captureRgArgs(): Promise<string[]> {
    let seen: string[] = [];
    globHooks.execFileImpl = ((_f: string, args: string[], _o: unknown, cb: (e: unknown, out: string) => void) => {
      seen = args;
      cb(null, 'a.ts\n');
      return { kill() {} } as never;
    }) as typeof globHooks.execFileImpl;
    await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    return seen;
  }

  it('default: rg args include --no-ignore and --hidden', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    delete process.env[GLOB_HIDDEN_ENV];
    delete process.env[GLOB_NO_IGNORE_ENV];
    const args = await captureRgArgs();
    assert.ok(args.includes('--no-ignore'), 'default drops gitignore respect via --no-ignore');
    assert.ok(args.includes('--hidden'), 'default includes hidden files');
  });

  it('AMI_GLOB_NO_IGNORE=false omits --no-ignore (rg honors .gitignore)', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    process.env[GLOB_NO_IGNORE_ENV] = 'false';
    delete process.env[GLOB_HIDDEN_ENV];
    const args = await captureRgArgs();
    assert.ok(!args.includes('--no-ignore'), 'respecting gitignore must omit --no-ignore');
    assert.ok(args.includes('--hidden'));
  });

  it('AMI_GLOB_HIDDEN=false omits --hidden', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    process.env[GLOB_HIDDEN_ENV] = 'false';
    delete process.env[GLOB_NO_IGNORE_ENV];
    const args = await captureRgArgs();
    assert.ok(!args.includes('--hidden'), 'excluding hidden must omit --hidden');
    assert.ok(args.includes('--no-ignore'));
  });
});

describe('U079 glob — truncation affordance', () => {
  let tmpDir: string;
  let ctx: ToolContext;
  const origRg = globHooks.rgFiles;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-trunc-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'only.ts'), '1');
  });
  after(() => {
    globHooks.rgFiles = origRg;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('truncation notice tells the model to narrow the pattern/path', async () => {
    // Force a large match set via the rg seam, then page so truncation triggers.
    process.env[GLOB_RIPGREP_ENV] = '1';
    const many = Array.from({ length: 10 }, (_, i) => `f${i}.ts`);
    globHooks.rgFiles = async () => many;
    try {
      const r = await globTool.execute!({ pattern: '**/*.ts', limit: 3 }, ctx);
      assert.ok(r.output.includes('truncated'));
      assert.match(r.output, /more specific path or pattern/i);
    } finally {
      delete process.env[GLOB_RIPGREP_ENV];
      globHooks.rgFiles = origRg;
    }
  });
});
