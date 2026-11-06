// ===========================================================================
// Native-ESM test build for the core package.
//
// Ported from superinference/ui scripts/esm-build.mjs, trimmed to the single
// self-contained `core` layer. core is HEADLESS (no ink/react), so unlike the UI
// layers it needs no runtime mock — just compile src + tests to native ESM.
//
// Why native ESM and not `node --import tsx`: tsx transpiles to CommonJS, whose
// CJS-interop helpers (__toESM, __copyProps, …) inject branch arms no test can
// reach, capping measured branch coverage well below the real source. Compiling
// to native ESM removes those helpers so branch coverage reflects the true source.
// ===========================================================================
import { build } from 'esbuild';
import { readFileSync, writeFileSync, readdirSync, rmSync, existsSync, mkdirSync, symlinkSync } from 'fs';
import { join, relative, dirname, resolve as resolvePath } from 'path';

// Package root (the dir holding src/, test/, node_modules/, package.json).
export const ROOT = new URL('..', import.meta.url).pathname.replace(/\/$/, '');

// Single layer. `base` is the build root; `srcDirs` are compiled for coverage;
// `testDir` supplies the test entry points; `coverageInclude` scopes the report.
export const LAYERS = {
  core: {
    base: ROOT,
    srcDirs: ['src'],
    testDir: 'test',
    incompatible: new Set([]),
    testFilter: (f) => f.endsWith('.test.ts'),
    coverageInclude: ['src/'],
  },
};

export function walk(dir, filter, acc = []) {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) walk(p, filter, acc);
    else if (filter(e.name)) acc.push(p);
  }
  return acc;
}

const isCompilable = (n) => (n.endsWith('.ts') || n.endsWith('.tsx')) && !n.endsWith('.d.ts');

/**
 * Compile the layer's src + tests to native ESM and return everything callers
 * need to run them. Pure build; does not spawn node. Call cleanup() when done.
 *
 * @param {string} layerName  core
 * @param {{shardIdx?: number, shardTotal?: number, includeIncompatible?: boolean}} [opts]
 */
export async function buildLayerTree(layerName, opts = {}) {
  const layer = LAYERS[layerName];
  if (!layer) throw new Error(`Unknown layer: ${layerName}`);
  const shardIdx = opts.shardIdx ?? 0;
  const shardTotal = opts.shardTotal ?? 0;

  // Unique build dir per process so concurrent runs don't clobber each other.
  const BUILD_DIRNAME = `.esm-build-${process.pid}`;
  const outRoot = join(layer.base, BUILD_DIRNAME);
  rmSync(outRoot, { recursive: true, force: true });

  const srcFiles = [...layer.srcDirs, ...(layer.extraCompileDirs ?? [])]
    .map((d) => join(layer.base, d))
    .filter((d) => existsSync(d))
    .flatMap((d) => walk(d, isCompilable));
  const incompatible = layer.incompatible ?? new Set();
  const includeIncompatible = opts.includeIncompatible ?? false;
  const allTestTs = walk(join(layer.base, layer.testDir), isCompilable);
  const allTestFiles = allTestTs.filter(
    (f) => layer.testFilter(f.split('/').pop()) && (includeIncompatible || !incompatible.has(f.split('/').pop())));
  const testFiles = shardTotal > 0
    ? allTestFiles.filter((_, i) => (i % shardTotal) + 1 === shardIdx)
    : allTestFiles;

  await build({
    entryPoints: [...srcFiles, ...allTestTs],
    outbase: layer.base,
    outdir: outRoot,
    format: 'esm',
    platform: 'node',
    target: 'node26',
    bundle: false,
    sourcemap: 'inline',
    outExtension: { '.js': '.mjs' },
    banner: {
      js:
        "import{createRequire as __cr}from'module';import{fileURLToPath as __fu}from'url';import{dirname as __dn}from'path';" +
        "const require=__cr(import.meta.url);" +
        `const __filename=__fu(import.meta.url).replace('/${BUILD_DIRNAME}/','/');` +
        "const __dirname=__dn(__filename);",
    },
    logLevel: 'error',
  });

  // Rewrite relative import/export/dynamic-import AND lazy require() specifiers to
  // .mjs so both static ESM resolution and createRequire find the compiled files.
  const REL = /(\bfrom\s*['"])(\.\.?\/[^'"]+?)(['"])/g;
  const DYN = /(\bimport\s*\(\s*['"])(\.\.?\/[^'"]+?)(['"]\s*\))/g;
  const REQ = /(\brequire\s*\(\s*['"])(\.\.?\/[^'"]+?)(['"]\s*\))/g;
  for (const f of walk(outRoot, (n) => n.endsWith('.mjs'))) {
    const fileDir = dirname(f);
    const addExt = (m, a, spec, c) => {
      if (spec.endsWith('.mjs') || spec.endsWith('.json')) return m;
      const extRewritten = spec.replace(/\.(jsx?|tsx?)$/, '.mjs');
      if (extRewritten !== spec) return `${a}${extRewritten}${c}`;
      const abs = resolvePath(fileDir, spec);
      if (existsSync(abs + '.mjs')) return `${a}${spec}.mjs${c}`;
      if (existsSync(resolvePath(abs, 'index.mjs'))) return `${a}${spec}/index.mjs${c}`;
      return `${a}${spec}.mjs${c}`;
    };
    let s = readFileSync(f, 'utf8');
    s = s.replace(REL, addExt).replace(DYN, addExt).replace(REQ, addExt);
    writeFileSync(f, s);
  }

  // Mirror the package's node_modules into the build tree so a compiled .mjs
  // resolves bare specifiers exactly as its original .ts would.
  {
    const realNm = join(layer.base, 'node_modules');
    const linkNm = join(outRoot, 'node_modules');
    if (existsSync(realNm) && !existsSync(linkNm)) {
      mkdirSync(dirname(linkNm), { recursive: true });
      symlinkSync(realNm, linkNm, 'dir');
    }
  }

  const testMjs = testFiles.map(
    (f) => join(outRoot, relative(layer.base, f)).replace(/\.ts$/, '.mjs'));
  const nodeImports = (layer.nodeImports ?? [])
    .map((p) => join(layer.base, p));

  return {
    layer,
    outRoot,
    BUILD_DIRNAME,
    srcFiles,
    testFiles,
    testMjs,
    nodeImports,
    cleanup: () => rmSync(outRoot, { recursive: true, force: true }),
  };
}

/**
 * Child-process env pinned to the CI terminal-color baseline so local runs
 * reproduce GitHub Actions results.
 */
export function ciChildEnv() {
  const env = { ...process.env, NODE_OPTIONS: '--max-old-space-size=8192', TERM: 'dumb' };
  delete env.COLORTERM;
  delete env.FORCE_COLOR;
  return env;
}
