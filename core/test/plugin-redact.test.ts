import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { redactUrlCredentials } from '../src/plugins';

describe('redactUrlCredentials', () => {
  it('masks user:password userinfo in an https URL', () => {
    assert.equal(
      redactUrlCredentials('https://alice:s3cret@github.com/o/r.git'),
      'https://***@github.com/o/r.git',
    );
  });

  it('masks a bare token userinfo', () => {
    assert.equal(
      redactUrlCredentials('https://ghp_deadbeef@github.com/o/r.git'),
      'https://***@github.com/o/r.git',
    );
  });

  it('redacts credentials embedded inside a longer error message', () => {
    const msg = "fatal: could not read from 'https://u:p@host/x.git' while cloning";
    assert.match(redactUrlCredentials(msg), /https:\/\/\*\*\*@host\/x\.git/);
    assert.doesNotMatch(redactUrlCredentials(msg), /u:p@/);
  });

  it('leaves a credential-free URL unchanged', () => {
    assert.equal(redactUrlCredentials('https://github.com/o/r.git'), 'https://github.com/o/r.git');
  });

  it('handles non-URL text and empty input', () => {
    assert.equal(redactUrlCredentials('no urls here'), 'no urls here');
    assert.equal(redactUrlCredentials(''), '');
  });
});
