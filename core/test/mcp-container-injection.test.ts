import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { isImageAvailable, getImageInfo } from '../src/mcp/container';

// ---------------------------------------------------------------------------
// H66: container image inspection must not pass the (config-controlled) image
// name through a shell. A fake runtime records its argv + whether a shell would
// have run an injected command; the image arg must arrive VERBATIM as one argv
// element and the injected `touch` must never execute.
// ---------------------------------------------------------------------------

let dir: string;
let runtime: string;   // fake runtime executable
let argvFile: string;  // where the fake runtime dumps its received argv
let pwnFile: string;   // created ONLY if a shell interprets the injection

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-container-inj-'));
  argvFile = path.join(dir, 'argv.json');
  pwnFile = path.join(dir, 'PWNED');
  runtime = path.join(dir, 'fakeRuntime');
  // A fake docker/podman: dump argv and, for getImageInfo, print the format
  // template so the parse path runs. Exits 0 so isImageAvailable returns true.
  fs.writeFileSync(
    runtime,
    `#!/usr/bin/env node\n` +
    `const fs=require('fs');\n` +
    `fs.writeFileSync(${JSON.stringify(argvFile)}, JSON.stringify(process.argv.slice(2)));\n` +
    `process.stdout.write('id|||size|||created');\n`,
  );
  fs.chmodSync(runtime, 0o755);
});

after(() => { fs.rmSync(dir, { recursive: true, force: true }); });

describe('container image inspection is shell-injection safe (H66)', () => {
  it('isImageAvailable passes the image name verbatim (no shell)', () => {
    const injected = `alpine; touch ${pwnFile}`;
    const ok = isImageAvailable(runtime, injected);
    assert.equal(ok, true, 'fake runtime exits 0');
    const argv = JSON.parse(fs.readFileSync(argvFile, 'utf-8'));
    assert.deepEqual(argv, ['image', 'inspect', injected], 'image arrives as one literal argv element');
    assert.equal(fs.existsSync(pwnFile), false, 'no shell ran the injected touch');
  });

  it('getImageInfo passes the image name verbatim (no shell)', () => {
    const injected = `alpine && touch ${pwnFile}`;
    const info = getImageInfo(runtime, injected);
    assert.equal(info.available, true);
    const argv = JSON.parse(fs.readFileSync(argvFile, 'utf-8'));
    assert.equal(argv[argv.length - 1], injected, 'image is the final literal argv element');
    assert.equal(fs.existsSync(pwnFile), false, 'no shell ran the injected touch');
  });
});
