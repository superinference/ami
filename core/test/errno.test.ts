import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getErrnoCode,
  getErrnoPath,
  isENOENT,
  isFsInaccessible,
  FS_INACCESSIBLE_CODES,
} from '../src/utils/errno';

function errno(code: string, extra: Record<string, unknown> = {}): NodeJS.ErrnoException {
  const e = new Error(`${code}: synthetic`) as NodeJS.ErrnoException;
  e.code = code;
  Object.assign(e, extra);
  return e;
}

describe('errno helpers', () => {
  describe('getErrnoCode', () => {
    it('extracts a string code from an ErrnoException', () => {
      assert.equal(getErrnoCode(errno('ENOENT')), 'ENOENT');
      assert.equal(getErrnoCode(errno('EACCES')), 'EACCES');
    });
    it('returns undefined when there is no code / non-string code / non-object', () => {
      assert.equal(getErrnoCode(new Error('plain')), undefined);
      assert.equal(getErrnoCode({ code: 42 }), undefined);
      assert.equal(getErrnoCode(null), undefined);
      assert.equal(getErrnoCode(undefined), undefined);
      assert.equal(getErrnoCode('ENOENT'), undefined);
    });
  });

  describe('getErrnoPath', () => {
    it('extracts the offending path', () => {
      assert.equal(getErrnoPath(errno('ENOENT', { path: '/tmp/missing' })), '/tmp/missing');
    });
    it('returns undefined when there is no path / non-string path / non-object', () => {
      assert.equal(getErrnoPath(errno('ENOENT')), undefined);
      assert.equal(getErrnoPath({ path: 123 }), undefined);
      assert.equal(getErrnoPath(null), undefined);
      assert.equal(getErrnoPath('x'), undefined);
    });
  });

  describe('isENOENT', () => {
    it('is true only for ENOENT', () => {
      assert.equal(isENOENT(errno('ENOENT')), true);
      assert.equal(isENOENT(errno('EACCES')), false);
      assert.equal(isENOENT(new Error('plain')), false);
    });
  });

  describe('isFsInaccessible', () => {
    it('is true for every grouped inaccessible code', () => {
      for (const code of ['ENOENT', 'EACCES', 'EPERM', 'ENOTDIR', 'ELOOP']) {
        assert.equal(isFsInaccessible(errno(code)), true, `${code} should be inaccessible`);
      }
    });
    it('is false for unrelated errnos and non-errno errors', () => {
      assert.equal(isFsInaccessible(errno('EIO')), false);
      assert.equal(isFsInaccessible(errno('EBUSY')), false);
      assert.equal(isFsInaccessible(new Error('plain')), false);
      assert.equal(isFsInaccessible(null), false);
    });
  });

  describe('FS_INACCESSIBLE_CODES', () => {
    it('is the exact frozen grouped set', () => {
      assert.deepEqual([...FS_INACCESSIBLE_CODES], ['ENOENT', 'EACCES', 'EPERM', 'ENOTDIR', 'ELOOP']);
      assert.ok(Object.isFrozen(FS_INACCESSIBLE_CODES));
    });
  });
});
