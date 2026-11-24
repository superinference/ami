/**
 * Optional runtime packages (pdf-parse): resolved from the project first, else from
 * AMI's own optional-deps folder; a missing one yields an actionable error carrying the
 * exact install command, so the agent can install it (with the user's approval for the
 * command) into AMI's folder — never into the user's project — and retry.
 */
import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { optionalDepsDir, optionalDepInstallCommand, optionalDepRoot } from '../src/tools/file-read-detect-image-format-from-buffer';
import { classifyPdfError } from '../src/tools/file-read-build-file-read-input-schema';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-optdeps-'));
after(() => fs.rmSync(tmp, { recursive: true, force: true }));
const env = { SUPERINFERENCE_DATA_DIR: path.join(tmp, 'data') } as NodeJS.ProcessEnv;

/** A minimal resolvable package under `<root>/node_modules/<name>`. */
function fakePkg(root: string, name: string): void {
  const dir = path.join(root, 'node_modules', name);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({ name, version: '0.0.0', main: 'index.js' }));
  fs.writeFileSync(path.join(dir, 'index.js'), 'module.exports = {};');
}

describe('optional dependency resolution', () => {
  it('optional-deps lives under AMI\'s config dir, never the project', () => {
    assert.equal(optionalDepsDir(env), path.join(tmp, 'data', 'optional-deps'));
    assert.equal(optionalDepInstallCommand('pdf-parse', env), `npm install --prefix ${JSON.stringify(path.join(tmp, 'data', 'optional-deps'))} pdf-parse`);
  });

  it('prefers the project\'s own install', () => {
    const proj = path.join(tmp, 'proj-with');
    fakePkg(proj, 'ami-fake-optional');
    assert.equal(optionalDepRoot('ami-fake-optional', proj, env), path.join(proj, '_'));
  });

  it('falls back to AMI\'s optional-deps folder when the project lacks it', () => {
    const proj = path.join(tmp, 'proj-without');
    fs.mkdirSync(proj, { recursive: true });
    fakePkg(optionalDepsDir(env), 'ami-fake-optional-2');
    const root = optionalDepRoot('ami-fake-optional-2', proj, env);
    assert.equal(root, path.join(optionalDepsDir(env), '_'));
  });
});

describe('PDF unavailable error is actionable', () => {
  it('a missing pdf-parse carries the exact install command', () => {
    const msg = classifyPdfError("Cannot find module 'pdf-parse'");
    assert.match(msg, /^Error: PDF support is unavailable: Cannot find module 'pdf-parse'/);
    assert.match(msg, /install the optional pdf-parse package into AMI's own folder \(not the project\)/);
    assert.ok(msg.includes(optionalDepInstallCommand('pdf-parse')), msg);
  });
  it('other unavailable reasons are unchanged', () => {
    assert.equal(classifyPdfError('pdftoppm is not installed'), 'Error: PDF support is unavailable: pdftoppm is not installed');
  });
});
