import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { isGeneratedFile, filterGeneratedFiles } from '../src/generated-files';
import { WorkspaceIndexer } from '../src/workspace-indexer';

describe('isGeneratedFile', () => {
  it('matches exact lockfile / shrinkwrap filenames (case-insensitive)', () => {
    for (const f of [
      'package-lock.json', 'yarn.lock', 'pnpm-lock.yaml', 'bun.lockb', 'bun.lock',
      'composer.lock', 'gemfile.lock', 'cargo.lock', 'poetry.lock', 'pipfile.lock',
      'shrinkwrap.json', 'npm-shrinkwrap.json',
    ]) {
      assert.equal(isGeneratedFile(f), true, `${f} should be generated`);
      assert.equal(isGeneratedFile(f.toUpperCase()), true, `${f} upper should be generated`);
      assert.equal(isGeneratedFile(path.join('sub', 'dir', f)), true, `nested ${f}`);
    }
  });

  it('matches single and compound extensions', () => {
    assert.equal(isGeneratedFile('types.d.ts'), true);
    assert.equal(isGeneratedFile('app.min.js'), true);
    assert.equal(isGeneratedFile('style.min.css'), true);
    assert.equal(isGeneratedFile('page.min.html'), true);
    assert.equal(isGeneratedFile('vendor.bundle.js'), true);
    assert.equal(isGeneratedFile('vendor.bundle.css'), true);
    assert.equal(isGeneratedFile('model.generated.ts'), true);
    assert.equal(isGeneratedFile('model.generated.js'), true);
    assert.equal(isGeneratedFile('deps.lock'), true);
    // extname only returns the final segment; compound reconstruction catches .d.ts
    assert.equal(path.extname('types.d.ts'), '.ts');
  });

  it('matches generated/vendored directory substrings on normalized paths', () => {
    for (const p of [
      'dist/app.js', 'build/x.ts', 'out/a.js', 'output/b.ts', 'node_modules/pkg/i.js',
      'vendor/lib.go', 'vendored/lib.go', 'third_party/x.ts', 'third-party/x.ts',
      'external/x.ts', '.next/x.js', '.nuxt/x.js', '.svelte-kit/x.js', 'coverage/x.js',
      'pkg/__pycache__/m.py', '.tox/x.py', 'venv/x.py', '.venv/x.py',
      'target/release/x.rs', 'target/debug/x.rs',
    ]) {
      assert.equal(isGeneratedFile(p), true, `${p} should be generated`);
    }
  });

  it('normalizes leading slashes so absolute-style paths match directory patterns', () => {
    // Leading slashes are stripped then a single POSIX separator is re-prefixed,
    // so an absolute-style path classifies the same as its relative form.
    assert.equal(isGeneratedFile('/node_modules/pkg/x.js'), true);
    assert.equal(isGeneratedFile('///dist/app.js'), true);
    assert.equal(isGeneratedFile('/src/engine.ts'), false);
  });

  it('distinguishes target/release from a bare target dir (substring with slashes)', () => {
    assert.equal(isGeneratedFile('target/release/app.rs'), true);
    assert.equal(isGeneratedFile('target/app.rs'), false);
  });

  it('matches regex filename patterns (protobuf, grpc, swagger, generated, …)', () => {
    for (const f of [
      'a.min.js', 'a-min.css', 'a.bundle.ts', 'a.generated.go', 'a.gen.ts',
      'a.auto.ts', 'a_generated.ts', 'a_gen.ts',
      'service.pb.go', 'service.pb.js', 'service.pb.ts', 'service.pb.py', 'service.pb.rb',
      'service_pb.py', 'service_pb2.py', 'service.pb.h',
      'api.grpc.ts', 'api.swagger.ts', 'api.openapi.ts',
    ]) {
      assert.equal(isGeneratedFile(f), true, `${f} should be generated`);
    }
  });

  it('returns false for ordinary source files', () => {
    for (const f of [
      'src/engine.ts', 'main.go', 'lib.rs', 'app.py', 'Service.java',
      'src/utils/config.ts', 'index.js', 'README.md', 'target/app.rs',
    ]) {
      assert.equal(isGeneratedFile(f), false, `${f} should NOT be generated`);
    }
  });
});

describe('filterGeneratedFiles', () => {
  it('removes generated paths and preserves source paths + order', () => {
    const input = ['src/a.ts', 'pkg/types.d.ts', 'src/b.ts', 'dist/c.js', 'main.go'];
    assert.deepEqual(filterGeneratedFiles(input), ['src/a.ts', 'src/b.ts', 'main.go']);
  });

  it('returns an empty array when every path is generated', () => {
    assert.deepEqual(filterGeneratedFiles(['a.d.ts', 'yarn.lock', 'x.min.js']), []);
  });
});

describe('WorkspaceIndexer excludes generated files from the index (E2E)', () => {
  const dirs: string[] = [];
  function makeWorkspace(): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-genfiles-test-'));
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, 'package.json'), '{}');
    return dir;
  }
  after(() => {
    for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
  });

  it('indexes real source but skips *.d.ts / *.min.js / *.generated.ts / *.pb.ts', async () => {
    const dir = makeWorkspace();
    fs.mkdirSync(path.join(dir, 'src'), { recursive: true });
    // Real source — must be indexed.
    fs.writeFileSync(
      path.join(dir, 'src', 'engine.ts'),
      'export class RealEngine {}\nexport function realStart() {}\n',
    );
    // Generated files that ARE in EXT_TO_LANG (.ts/.js) and NOT excluded by
    // SKIP_DIRS, so only the classifier can keep them out of the index.
    fs.writeFileSync(
      path.join(dir, 'src', 'types.d.ts'),
      'export declare class GeneratedDecl {}\n',
    );
    fs.writeFileSync(
      path.join(dir, 'src', 'app.min.js'),
      'export function GeneratedMinified(){}\n',
    );
    fs.writeFileSync(
      path.join(dir, 'src', 'model.generated.ts'),
      'export class GeneratedModel {}\n',
    );
    fs.writeFileSync(
      path.join(dir, 'src', 'service.pb.ts'),
      'export class GeneratedProto {}\n',
    );

    const indexer = new WorkspaceIndexer(dir);
    const result = await indexer.buildIndex();

    // Only the single real source file is indexed.
    assert.equal(result.fileCount, 1, 'only the non-generated file should be indexed');
    const indexed = indexer.getFiles().map(f => f.relativePath);
    assert.deepEqual(indexed, [path.join('src', 'engine.ts')]);

    // Real symbols are searchable.
    assert.ok(indexer.searchSymbols('RealEngine').some(s => s.name === 'RealEngine'));
    // Generated symbols are absent from the index.
    for (const name of ['GeneratedDecl', 'GeneratedMinified', 'GeneratedModel', 'GeneratedProto']) {
      assert.equal(indexer.findDefinition(name).length, 0, `${name} must be excluded`);
    }
  });
});
