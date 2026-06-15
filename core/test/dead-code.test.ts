/**
 * Dead code detection test — CORE ONLY.
 *
 * Verifies that core source files are internally consistent:
 * - All source files are referenced by at least one other core file
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

describe('Core dead code detection', () => {
  const srcFiles = readAllSourceFiles(CORE_SRC);

  it('index.ts exists and has exports', () => {
    assert.ok(fs.existsSync(CORE_INDEX), `index.ts should exist at ${CORE_INDEX}`);
    const content = fs.readFileSync(CORE_INDEX, 'utf-8');
    assert.ok(content.includes('export'), 'index.ts should have exports');
  });

  it('all core source files are internally referenced', () => {
    const allSrcFiles = srcFiles.filter(f => {
      if (f.endsWith('index.ts')) return false;
      const rel = path.relative(CORE_SRC, f).replace(/\\/g, '/');
      return !EXCLUDED_MODULES.has(rel);
    });
    const unreferenced: string[] = [];

    for (const file of allSrcFiles) {
      const basename = path.basename(file, path.extname(file));
      let referenced = false;
      for (const other of srcFiles) {
        if (other === file) continue;
        try {
          const content = fs.readFileSync(other, 'utf-8');
          if (content.includes(`'${basename}'`) || content.includes(`/${basename}'`) ||
              content.includes(`/${basename}"`) || content.includes(`from './${basename}`)) {
            referenced = true;
            break;
          }
        } catch {}
      }
      if (!referenced) {
        unreferenced.push(path.relative(CORE_SRC, file));
      }
    }

    if (unreferenced.length > 0) {
      assert.fail(
        `Found ${unreferenced.length} core source files not imported by any other core file:\n` +
        unreferenced.map(f => `  - ${f}`).join('\n')
      );
    }
  });

});
