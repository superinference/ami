import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  errorMessage,
  toLoggableError,
  redactPaths,
  REDACT_PLACEHOLDER,
} from '../src/credential-guard';

describe('errorMessage', () => {
  it('returns the .message of an Error instance', () => {
    assert.equal(errorMessage(new Error('boom')), 'boom');
  });

  it('returns the .message of an Error subclass', () => {
    class ENOENTError extends Error {}
    assert.equal(errorMessage(new ENOENTError('no such file')), 'no such file');
  });

  it('stringifies non-Error values', () => {
    assert.equal(errorMessage('plain string'), 'plain string');
    assert.equal(errorMessage(42), '42');
    assert.equal(errorMessage(undefined), 'undefined');
    assert.equal(errorMessage(null), 'null');
    assert.equal(errorMessage({ toString: () => 'custom' }), 'custom');
  });
});

describe('redactPaths urls mode', () => {
  it('leaves URLs intact by default (urls off)', () => {
    const text = 'fetch failed for https://api.example.com/v1/models?key=abc';
    assert.equal(redactPaths(text), text);
  });

  it('collapses network URLs to [URL] when urls is enabled, preserving trailing punctuation', () => {
    const red = redactPaths('see https://internal.example.com/a/b?token=sekret.', { urls: true });
    assert.equal(red, 'see [URL].');
    assert.ok(!red.includes('internal.example.com'));
    assert.ok(!red.includes('sekret'));
  });

  it('redacts http, ws, wss and ftp schemes', () => {
    assert.equal(redactPaths('x http://h/p', { urls: true }), 'x [URL]');
    assert.equal(redactPaths('x ws://h/p', { urls: true }), 'x [URL]');
    assert.equal(redactPaths('x wss://h/p', { urls: true }), 'x [URL]');
    assert.equal(redactPaths('x ftp://h/p', { urls: true }), 'x [URL]');
  });

  it('does NOT touch file:// URLs (left to cwd/home path substitution)', () => {
    const red = redactPaths('opened file:///etc/hosts', { urls: true });
    assert.equal(red, 'opened file:///etc/hosts');
  });

  it('still normalizes cwd/home alongside URL redaction', () => {
    const red = redactPaths('at /work/proj and https://h/p', {
      cwd: '/work/proj',
      home: '/home/alice',
      urls: true,
    });
    assert.equal(red, 'at [CWD] and [URL]');
  });
});

describe('toLoggableError', () => {
  it('scrubs an absolute cwd/home file path out of a persisted error message', () => {
    const err = new Error('ENOENT: no such file, open /home/alice/project/secret-config.ts');
    const red = toLoggableError(err, { cwd: '/home/alice/project', home: '/home/alice' });
    assert.ok(!red.includes('/home/alice'), 'home path must not leak');
    assert.ok(red.includes('[CWD]') || red.includes('[HOME]'));
    // The informative part of the message survives.
    assert.ok(red.includes('ENOENT'));
  });

  it('scrubs an internal URL out of a persisted error message', () => {
    const red = toLoggableError(new Error('connect failed: https://vault.internal.corp/secret'));
    assert.equal(red, 'connect failed: [URL]');
  });

  it('scrubs an embedded credential out of a persisted error message', () => {
    const key = 'sk-' + 'a'.repeat(48);
    const red = toLoggableError(new Error(`auth rejected key=${key}`));
    assert.ok(!red.includes(key), 'secret must not leak');
    assert.ok(red.includes(REDACT_PLACEHOLDER));
  });

  it('normalizes non-Error throwables too', () => {
    assert.equal(toLoggableError('just a string'), 'just a string');
    assert.equal(toLoggableError(123), '123');
  });

  it('scrubs credential, path AND url in one pass', () => {
    const key = 'ghp_' + 'b'.repeat(36);
    const err = new Error(`failed at /home/bob/app.ts calling https://h/x with ${key}`);
    const red = toLoggableError(err, { cwd: '/home/bob', home: '/home/bob' });
    assert.ok(!red.includes('/home/bob'));
    assert.ok(!red.includes('https://h/x'));
    assert.ok(!red.includes(key));
    assert.ok(red.includes('[URL]'));
    assert.ok(red.includes(REDACT_PLACEHOLDER));
  });
});
