#!/usr/bin/env node
// ===========================================================================
// Accurate core coverage via native ESM.
//
// Ported from superinference/ui scripts/coverage-esm.mjs for the core package so
// the 99% coverage gate runs in THIS public repo on every push (free), instead of
// only in the private superinference/ui CI. Compiles src + tests to native ESM
// (no tsx CJS-interop helpers that inflate branch counts) and runs
// `node --test --experimental-test-coverage`, then reconciles the measured set
// against every runtime source file so a never-imported module can't silently
// drop out of the denominator.
//
// Usage: node scripts/coverage-esm.mjs core [--lines N --branches N --functions N] [--shard N/M]
// ===========================================================================
import { build } from 'esbuild';
import { rmSync } from 'fs';
import { relative } from 'path';
import { pathToFileURL } from 'url';
import { spawnSync } from 'child_process';
import { LAYERS, buildLayerTree, ciChildEnv } from './esm-build.mjs';

const layerName = process.argv[2];
const layer = LAYERS[layerName];
if (!layer) { console.error(`Usage: coverage-esm.mjs core [--lines N --branches N --functions N] [--shard N/M]`); process.exit(2); }

const args = process.argv.slice(3);
function flag(name, def) { const i = args.indexOf(name); return i >= 0 ? Number(args[i + 1]) : def; }
const thrLines = flag('--lines', 0), thrBranches = flag('--branches', 0), thrFns = flag('--functions', 0);

// Sharding: --shard N/M runs only test files where (index % M) + 1 == N.
let shardIdx = 0, shardTotal = 0;
const shardArg = args.indexOf('--shard');
if (shardArg >= 0) {
  const [n, m] = (args[shardArg + 1] || '').split('/').map(Number);
  if (!n || !m || n < 1 || n > m) { console.error('--shard must be N/M (e.g. 2/3)'); process.exit(2); }
  shardIdx = n; shardTotal = m;
}

const built = await buildLayerTree(layerName, { shardIdx, shardTotal });
const { outRoot, srcFiles, testFiles, testMjs, nodeImports: nodeImportPaths } = built;
if (shardTotal > 0) {
  console.error(`ℹ shard ${shardIdx}/${shardTotal}: running ${testFiles.length} test files`);
}

const cov = [
  '--test', '--experimental-test-coverage', '--test-force-exit',
  '--test-coverage-exclude=**/test/**',
  '--test-coverage-exclude=**/node_modules/**',
];
if (shardTotal === 0) {
  if (thrLines) cov.push(`--test-coverage-lines=${thrLines}`);
  if (thrBranches) cov.push(`--test-coverage-branches=${thrBranches}`);
  if (thrFns) cov.push(`--test-coverage-functions=${thrFns}`);
}
const nodeImports = nodeImportPaths.flatMap((p) => ['--import', pathToFileURL(p).href]);

const res = spawnSync('node', [...nodeImports, ...cov, ...testMjs], {
  cwd: layer.base,
  env: ciChildEnv(),
  encoding: 'utf8',
  maxBuffer: 512 * 1024 * 1024,
});
if (res.stdout) process.stdout.write(res.stdout);
if (res.stderr) process.stderr.write(res.stderr);

// ---- Dynamic coverage-denominator completeness gate ----------------------
// Node's --experimental-test-coverage only reports modules actually IMPORTED, so
// a runtime source file no test loads silently drops out of the denominator.
// Reconcile the measured set against EVERY runtime source file under the layer's
// own coverageInclude dir and fail on any gap. Type-only files compile to no
// executable JS and are auto-excluded (detected from a bannerless recompile).
const ownSrc = srcFiles.filter((f) =>
  layer.coverageInclude.some((inc) => relative(layer.base, f).startsWith(inc.replace(/\/$/, ''))));
const isTypeOnly = async (srcAbs) => {
  try {
    const out = await build({ entryPoints: [srcAbs], format: 'esm', platform: 'node', bundle: false, write: false, logLevel: 'silent' });
    const body = out.outputFiles.map((o) => o.text).join('\n')
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .replace(/^\s*\/\/.*$/gm, '')
      .replace(/^\s*import\s[^\n]*$/gm, '')
      .replace(/^\s*export\s*\*\s*from\s*[^\n]*$/gm, '')
      .replace(/^\s*export\s*\{[^}]*\}\s*from\s*[^\n]*$/gm, '')
      .replace(/^\s*export\s*\{[^}]*\}\s*;?\s*$/gm, '')
      .replace(/^[\s;]*$/gm, '')
      .trim();
    return body.length === 0;
  } catch {
    return false;
  }
};
const COVERAGE_ROW = /([\w.\-]+\.mjs)\s*\|\s*[\d.]+\s*\|/;
const measured = new Set(
  (res.stdout || '').split('\n')
    .map((l) => (l.match(COVERAGE_ROW) || [])[1])
    .filter(Boolean));
const externallyCovered = layer.externallyCovered ?? {};
const unmeasured = ownSrc
  .map((f) => ({ abs: f, rel: relative(layer.base, f) }))
  .filter(({ rel }) => !measured.has(rel.split('/').pop().replace(/\.tsx?$/, '.mjs')));
const missing = [];
const external = [];
for (const { abs, rel } of unmeasured) {
  if (externallyCovered[rel]) { external.push(rel); continue; }
  if (!(await isTypeOnly(abs))) missing.push(rel);
}
missing.sort();

rmSync(outRoot, { recursive: true, force: true });

for (const rel of external.sort()) {
  console.error(`ℹ ${rel}: covered out-of-process — ${externallyCovered[rel]}`);
}

if (shardTotal > 0) {
  process.exit(res.status ?? 1);
}

if (missing.length) {
  console.error(`\n❌ Coverage denominator incomplete (${layerName}): ${missing.length} runtime source file(s) were never loaded, so they are absent from the coverage %:`);
  for (const m of missing) console.error(`   - ${m}`);
  console.error('Every runtime module must be exercised by a test (import it directly, or cover it through a harness that loads it). Type-only files are auto-excluded.');
  process.exit(1);
}
process.exit(res.status ?? 1);
