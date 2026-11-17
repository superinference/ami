import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  surfaceAttachmentTool,
  formatAttachmentDelivery,
  expandAttachmentPath,
  resolveSurfacedAttachments,
  SURFACE_ATTACHMENT_METADATA_KEY,
} from '../src/tools/surface-attachments';
import { surfaceAttachmentTool as viaIndex, createDefaultTools } from '../src/tools/index';
import type { ToolContext } from '../src/types';

let tmp: string;
let pngPath: string;
let txtPath: string;

before(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'surface-att-'));
  pngPath = path.join(tmp, 'shot.png');
  txtPath = path.join(tmp, 'notes.txt');
  fs.writeFileSync(pngPath, Buffer.from([0x89, 0x50, 0x4e, 0x47])); // 4 bytes
  fs.writeFileSync(txtPath, 'hello world'); // 11 bytes
});

after(() => {
  fs.rmSync(tmp, { recursive: true, force: true });
});

const ctx = (): ToolContext => ({ cwd: tmp } as ToolContext);

describe('formatAttachmentDelivery', () => {
  it('has no suffix when there are no attachments', () => {
    assert.equal(formatAttachmentDelivery(0), 'Message delivered to user.');
  });
  it('uses the singular noun for one attachment', () => {
    assert.equal(formatAttachmentDelivery(1), 'Message delivered to user. (1 attachment included)');
  });
  it('pluralizes for multiple attachments', () => {
    assert.equal(formatAttachmentDelivery(3), 'Message delivered to user. (3 attachments included)');
  });
});

describe('expandAttachmentPath', () => {
  it('resolves a relative path against cwd', () => {
    assert.equal(expandAttachmentPath('shot.png', '/work'), path.resolve('/work', 'shot.png'));
  });
  it('keeps an absolute path', () => {
    assert.equal(expandAttachmentPath('/a/b.png', '/work'), '/a/b.png');
  });
  it('expands a leading ~ to the home dir', () => {
    assert.equal(expandAttachmentPath('~/x.png', '/work'), path.join(os.homedir(), '/x.png'));
    assert.equal(expandAttachmentPath('~', '/work'), path.join(os.homedir(), ''));
  });
});

describe('surfaceAttachmentTool', () => {
  it('is registered in the default tool pool so the model can call it', () => {
    assert.equal(viaIndex.name, 'surface_attachment');
    const names = createDefaultTools('/tmp').getAll().map(t => t.name);
    assert.ok(names.includes('surface_attachment'));
  });

  it('is dispatchable end-to-end through the registry the engine uses', async () => {
    const registry = createDefaultTools(tmp);
    const tool = registry.get('surface_attachment');
    assert.ok(tool, 'surface_attachment resolvable via registry.get');
    const r = await tool!.execute!(
      { message: 'here is the screenshot', attachments: ['shot.png'] },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.equal(r.output, 'Message delivered to user. (1 attachment included)');
    const resolved = r.metadata?.[SURFACE_ATTACHMENT_METADATA_KEY] as Array<{ path: string }>;
    assert.equal(resolved.length, 1);
    assert.equal(resolved[0]!.path, pngPath);
  });

  it('is a read-only, open-world, concurrency-safe message channel', () => {
    assert.equal(surfaceAttachmentTool.isReadOnly, true);
    assert.equal(surfaceAttachmentTool.isOpenWorld, true);
    assert.equal(surfaceAttachmentTool.isConcurrencySafe, true);
  });

  it('describes the attachment affordance (parity with BriefTool wording)', () => {
    const d = surfaceAttachmentTool.description;
    assert.match(d, /photos, screenshots, diffs, logs/);
    assert.match(d, /surface/i);
  });

  it('delivers a message with no attachments', async () => {
    const r = await surfaceAttachmentTool.execute!({ message: 'done' }, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(r.output, 'Message delivered to user.');
    assert.deepEqual(r.metadata?.[SURFACE_ATTACHMENT_METADATA_KEY], []);
    assert.equal(r.metadata?.surfaceStatus, 'normal');
  });

  it('resolves attachments with path/size/isImage and confirms count', async () => {
    const r = await surfaceAttachmentTool.execute!(
      { message: 'see these', attachments: ['shot.png', 'notes.txt'], status: 'proactive' },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.equal(r.output, 'Message delivered to user. (2 attachments included)');
    assert.equal(r.metadata?.surfaceStatus, 'proactive');
    const resolved = r.metadata?.[SURFACE_ATTACHMENT_METADATA_KEY] as Array<{ path: string; size: number; isImage: boolean }>;
    assert.equal(resolved.length, 2);
    assert.deepEqual(resolved[0], { path: pngPath, size: 4, isImage: true });
    assert.deepEqual(resolved[1], { path: txtPath, size: 11, isImage: false });
  });

  it('rejects a missing message', async () => {
    const r = await surfaceAttachmentTool.execute!({ attachments: ['shot.png'] }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /"message" is required/);
  });

  it('rejects an unknown parameter', async () => {
    const r = await surfaceAttachmentTool.execute!({ message: 'x', bogus: 1 }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /unknown parameter\(s\): bogus/);
  });

  it('rejects an invalid status', async () => {
    const r = await surfaceAttachmentTool.execute!({ message: 'x', status: 'loud' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /invalid status "loud"/);
  });

  it('rejects a non-array attachments value', async () => {
    const r = await surfaceAttachmentTool.execute!({ message: 'x', attachments: 'shot.png' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /must be an array/);
  });

  it('rejects non-string attachment entries', async () => {
    const r = await surfaceAttachmentTool.execute!({ message: 'x', attachments: [5] }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /must be a string path/);
  });

  it('rejects too many attachments', async () => {
    const many = Array.from({ length: 51 }, () => 'shot.png');
    const r = await surfaceAttachmentTool.execute!({ message: 'x', attachments: many }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /too many attachments/);
  });

  it('reports a blank path as not a regular file', async () => {
    const r = await surfaceAttachmentTool.execute!({ message: 'x', attachments: ['   '] }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /is not a regular file/);
  });

  it('reports a non-existent path with the cwd (parity: validateAttachmentPaths)', async () => {
    const r = await surfaceAttachmentTool.execute!({ message: 'x', attachments: ['nope.png'] }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /does not exist\. Current working directory:/);
    assert.ok(r.output.includes(tmp));
  });

  it('reports a directory as not a regular file', async () => {
    const r = await surfaceAttachmentTool.execute!({ message: 'x', attachments: [tmp] }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /is not a regular file/);
  });
});

describe('resolveSurfacedAttachments (injected stat seam)', () => {
  const throwErrno = (code: string): StatFnThrow => () => {
    const e = new Error(code) as NodeJS.ErrnoException;
    e.code = code;
    throw e;
  };
  type StatFnThrow = () => { isFile(): boolean; size: number };

  it('maps EACCES to a permission-denied message', () => {
    const r = resolveSurfacedAttachments(['/x/y.png'], '/x', throwErrno('EACCES'));
    assert.equal(r.ok, false);
    assert.ok(!r.ok && /permission denied/.test(r.message));
  });

  it('maps EPERM to a permission-denied message', () => {
    const r = resolveSurfacedAttachments(['/x/y.png'], '/x', throwErrno('EPERM'));
    assert.equal(r.ok, false);
    assert.ok(!r.ok && /permission denied/.test(r.message));
  });

  it('surfaces an unexpected errno rather than swallowing it', () => {
    const r = resolveSurfacedAttachments(['/x/y.png'], '/x', throwErrno('EIO'));
    assert.equal(r.ok, false);
    assert.ok(!r.ok && /could not be read \(EIO\)/.test(r.message));
  });

  it('surfaces an unknown (code-less) stat error', () => {
    const r = resolveSurfacedAttachments(['/x/y.png'], '/x', () => { throw new Error('boom'); });
    assert.equal(r.ok, false);
    assert.ok(!r.ok && /could not be read \(unknown error\)/.test(r.message));
  });

  it('resolves via an injected stat', () => {
    const r = resolveSurfacedAttachments(['a.png', 'b.log'], '/w', (p) => ({ isFile: () => true, size: p.endsWith('a.png') ? 10 : 20 }));
    assert.equal(r.ok, true);
    assert.ok(r.ok && r.resolved[0]!.isImage === true && r.resolved[0]!.size === 10);
    assert.ok(r.ok && r.resolved[1]!.isImage === false && r.resolved[1]!.size === 20);
  });
});
