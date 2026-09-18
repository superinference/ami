import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';

import {
  isImageAvailable,
  getImageInfo,
} from '../src/mcp/container';
import type { ImageInfo } from '../src/mcp/container';

const SRC_PATH = path.resolve(__dirname, '..', 'src', 'mcp', 'container.ts');
const SRC = fs.readFileSync(SRC_PATH, 'utf-8');

// ---------------------------------------------------------------------------
// ImageInfo interface
// ---------------------------------------------------------------------------

describe('ImageInfo interface', () => {
  it('declares available field', () => {
    assert.ok(SRC.includes('available: boolean'));
  });

  it('declares optional id field', () => {
    assert.ok(SRC.includes('id?: string'));
  });

  it('declares optional size field', () => {
    assert.ok(SRC.includes('size?: string'));
  });

  it('declares optional created field', () => {
    assert.ok(SRC.includes('created?: string'));
  });

  it('is usable as a type', () => {
    const info: ImageInfo = { available: false };
    assert.equal(info.available, false);
    assert.equal(info.id, undefined);
  });

  it('accepts all optional fields', () => {
    const info: ImageInfo = {
      available: true,
      id: 'sha256:abc123',
      size: '1.3GB',
      created: '2024-01-01',
    };
    assert.equal(info.available, true);
    assert.equal(info.id, 'sha256:abc123');
  });
});

// ---------------------------------------------------------------------------
// isImageAvailable
// ---------------------------------------------------------------------------

describe('isImageAvailable', () => {
  it('is exported as a function', () => {
    assert.equal(typeof isImageAvailable, 'function');
  });

  it('returns false for a nonexistent image with nonexistent runtime', () => {
    const result = isImageAvailable('_nonexistent_runtime_', 'nonexistent/image:latest');
    assert.equal(result, false);
  });

  it('returns false for a nonexistent image', () => {
    const result = isImageAvailable('docker', 'si_test_nonexistent_image_12345:latest');
    assert.equal(result, false);
  });

  it('uses image inspect command', () => {
    assert.ok(SRC.includes('image inspect'));
  });

  it('catches errors and returns false', () => {
    assert.ok(SRC.includes('catch'));
    const result = isImageAvailable('invalid-runtime', 'any-image');
    assert.equal(result, false);
  });
});

// ---------------------------------------------------------------------------
// getImageInfo
// ---------------------------------------------------------------------------

describe('getImageInfo', () => {
  it('is exported as a function', () => {
    assert.equal(typeof getImageInfo, 'function');
  });

  it('returns { available: false } for nonexistent runtime', () => {
    const info = getImageInfo('_nonexistent_runtime_', 'nonexistent/image:latest');
    assert.equal(info.available, false);
    assert.equal(info.id, undefined);
  });

  it('returns { available: false } for nonexistent image', () => {
    const info = getImageInfo('docker', 'si_test_nonexistent_image_12345:latest');
    assert.equal(info.available, false);
  });

  it('uses image inspect --format for structured output', () => {
    assert.ok(SRC.includes('image inspect --format'));
  });

  it('parses output with ||| separator', () => {
    assert.ok(SRC.includes("split('|||')"));
  });
});
