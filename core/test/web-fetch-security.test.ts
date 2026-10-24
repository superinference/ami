import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { MAX_RESPONSE_BYTES, BLOCKED_CONTENT_TYPES, isMetadataEndpoint } from '../src/tools/web-utils';

describe('MAX_RESPONSE_BYTES', () => {
  it('equals 50 MiB', () => {
    assert.equal(MAX_RESPONSE_BYTES, 50 * 1024 * 1024);
  });
});

describe('BLOCKED_CONTENT_TYPES', () => {
  it('contains application/octet-stream', () => {
    assert.ok(BLOCKED_CONTENT_TYPES.has('application/octet-stream'));
  });

  it('contains application/x-executable', () => {
    assert.ok(BLOCKED_CONTENT_TYPES.has('application/x-executable'));
  });

  it('contains application/x-sharedlib', () => {
    assert.ok(BLOCKED_CONTENT_TYPES.has('application/x-sharedlib'));
  });

  it('contains application/x-mach-binary', () => {
    assert.ok(BLOCKED_CONTENT_TYPES.has('application/x-mach-binary'));
  });

  it('contains application/vnd.microsoft.portable-executable', () => {
    assert.ok(BLOCKED_CONTENT_TYPES.has('application/vnd.microsoft.portable-executable'));
  });

  it('does NOT contain application/json', () => {
    assert.equal(BLOCKED_CONTENT_TYPES.has('application/json'), false);
  });

  it('does NOT contain text/html', () => {
    assert.equal(BLOCKED_CONTENT_TYPES.has('text/html'), false);
  });
});

describe('isMetadataEndpoint', () => {
  it('detects AWS metadata (169.254.169.254)', () => {
    assert.ok(isMetadataEndpoint('http://169.254.169.254/latest/meta-data/'));
  });

  it('detects GCP metadata (metadata.google.internal)', () => {
    assert.ok(isMetadataEndpoint('http://metadata.google.internal/computeMetadata/v1/'));
  });

  it('detects ECS task metadata (169.254.170.2)', () => {
    assert.ok(isMetadataEndpoint('http://169.254.170.2/v2/metadata'));
  });

  it('detects IMDSv2 IPv6 (fd00:ec2::254)', () => {
    assert.ok(isMetadataEndpoint('http://[fd00:ec2::254]/latest/'));
  });

  it('detects metadata with port', () => {
    assert.ok(isMetadataEndpoint('http://169.254.169.254:80/latest/'));
  });

  it('returns false for https://example.com', () => {
    assert.equal(isMetadataEndpoint('https://example.com'), false);
  });

  it('returns false for https://google.com', () => {
    assert.equal(isMetadataEndpoint('https://google.com'), false);
  });

  it('returns false for empty string', () => {
    assert.equal(isMetadataEndpoint(''), false);
  });

  it('returns false for invalid URL', () => {
    assert.equal(isMetadataEndpoint('not-a-url'), false);
  });

  it('returns false for localhost', () => {
    assert.equal(isMetadataEndpoint('http://localhost:3000'), false);
  });

  it('returns false for similar-looking IP', () => {
    assert.equal(isMetadataEndpoint('http://169.254.169.253/test'), false);
  });
});
