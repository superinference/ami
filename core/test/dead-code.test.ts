/**
 * Dead code detection test — CORE ONLY.
 *
 * Verifies that core source files are internally consistent:
 * - Every source file is reachable in the core import graph (imported/re-exported
 *   by at least one other core file), resolved by actually following relative
 *   module specifiers to on-disk files — not by loose substring matching.
 *
 * Engine/tool-executor wiring checks live in common/test/ since those
 * modules were moved to the common layer.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';

// __dirname is core/test/, so ../src/ is core/src/
const CORE_SRC = path.resolve(__dirname, '..', 'src');
const CORE_INDEX = path.join(CORE_SRC, 'index.ts');

// Unwired features — implemented but not yet integrated into the main flow.
// Each is kept intentionally for future wiring; removing requires a design decision.
const EXCLUDED_MODULES = new Set([
  'utils/repetition.ts',    // repetitive-output detector — wire into bash output processing
  'formatter.ts',           // multi-language file formatter — wire into file_write/file_edit post-hook
  'session-search.ts',      // TF-IDF session search — wire into session resume UI
  'skillbook.ts',           // persistent skill/knowledge store — wire into learning loop
  'tool-confirmation.ts',   // granular tool approval service — wire into permission manager
]);

function readAllSourceFiles(dir: string): string[] {
  const files: string[] = [];
  try {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory() && !entry.name.startsWith('.') && entry.name !== 'node_modules' && entry.name !== 'dist') {
        files.push(...readAllSourceFiles(full));
      } else if (entry.isFile() && (entry.name.endsWith('.ts') || entry.name.endsWith('.tsx'))) {
        files.push(full);
      }
    }
  } catch {}
  return files;
}

// Extract every module specifier a source file references via `from '...'`,
// dynamic `import('...')`, or `require('...')`. Regex literals are used (rather
// than embedding `from './...'` string literals) so the specifiers resolve from
// real import syntax.
function extractSpecifiers(content: string): string[] {
  const specs: string[] = [];
  const patterns = [
    /\bfrom\s+['"]([^'"]+)['"]/g,
    /\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
    /\brequire\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
  ];
  for (const re of patterns) {
    let m: RegExpExecArray | null;
    while ((m = re.exec(content)) !== null) specs.push(m[1]);
  }
  return specs;
}

// Resolve a relative specifier to the on-disk source file it names, following
// the same extension/index resolution the TS/bundler toolchain uses.
function resolveSpecifier(fromFile: string, spec: string): string | null {
  if (!spec.startsWith('.')) return null;
  const base = path.resolve(path.dirname(fromFile), spec);
  const candidates = [
    base,
    base + '.ts',
    base + '.tsx',
    path.join(base, 'index.ts'),
    path.join(base, 'index.tsx'),
  ];
  for (const candidate of candidates) {
    try {
      if (fs.statSync(candidate).isFile()) return path.resolve(candidate);
    } catch {}
  }
  return null;
}

describe('Core dead code detection', () => {
  const srcFiles = readAllSourceFiles(CORE_SRC);

  it('index.ts exists and declares the public export surface', () => {
    assert.ok(fs.existsSync(CORE_INDEX), `index.ts should exist at ${CORE_INDEX}`);
    const specifiers = extractSpecifiers(fs.readFileSync(CORE_INDEX, 'utf-8'));
    const reExported = specifiers
      .map((spec) => resolveSpecifier(CORE_INDEX, spec))
      .filter((resolved): resolved is string => resolved !== null);
    assert.ok(
      reExported.length > 0,
      'index.ts barrel should re-export at least one resolvable core module',
    );
  });

  it('all core source files are reachable in the import graph', () => {
    // Build the set of files that are actually imported/re-exported by some
    // other core file, by resolving each relative specifier to a real path.
    const referenced = new Set<string>();
    for (const file of srcFiles) {
      let content: string;
      try {
        content = fs.readFileSync(file, 'utf-8');
      } catch {
        continue;
      }
      for (const spec of extractSpecifiers(content)) {
        const resolved = resolveSpecifier(file, spec);
        if (resolved && resolved !== path.resolve(file)) referenced.add(resolved);
      }
    }

    const unreferenced = srcFiles
      .filter((f) => {
        if (f.endsWith('index.ts')) return false;
        const rel = path.relative(CORE_SRC, f).replace(/\\/g, '/');
        if (EXCLUDED_MODULES.has(rel)) return false;
        return !referenced.has(path.resolve(f));
      })
      .map((f) => path.relative(CORE_SRC, f));

    if (unreferenced.length > 0) {
      assert.fail(
        `Found ${unreferenced.length} core source files not imported by any other core file:\n` +
        unreferenced.map((f) => `  - ${f}`).join('\n'),
      );
    }
  });

});
