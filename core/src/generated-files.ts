// ---------------------------------------------------------------------------
// Generated / vendored file classifier
//
// A Linguist-style classifier that decides whether a repo-relative path is a
// generated or vendored file (lockfiles, minified/bundled assets, TypeScript
// declaration files, protobuf/gRPC/Swagger/OpenAPI output, build/vendor dirs,
// etc.). Mirrors Claude Code's `utils/generatedFiles.ts` so these files can be
// excluded from workspace indexing — their symbols are machine-generated noise
// that pollute the symbol index and the summary injected into the system prompt.
//
// Classification spans four independent dimensions, each sufficient on its own:
//   1. exact lowercased filename matches (EXCLUDED_FILENAMES);
//   2. exact (incl. compound, e.g. `.min.js`, `.d.ts`) extension matches
//      (EXCLUDED_EXTENSIONS);
//   3. normalized-path directory substring matches (EXCLUDED_DIRECTORIES);
//   4. regex filename patterns (EXCLUDED_FILENAME_PATTERNS).
//
// Cross-platform note: paths are normalized to POSIX separators with any leading
// slashes stripped, and filename/extension comparisons are case-folded, so a
// Windows backslash path and a relative path classify identically. Because
// `path.extname` only returns the final segment, a compound extension is
// reconstructed from the last two dot-parts so `.min.js` / `.d.ts` are matched.
// ---------------------------------------------------------------------------

import { basename, extname, posix, sep } from 'path';

/** Exact file-name matches (compared case-insensitively). */
const EXCLUDED_FILENAMES = new Set([
  'package-lock.json',
  'yarn.lock',
  'pnpm-lock.yaml',
  'bun.lockb',
  'bun.lock',
  'composer.lock',
  'gemfile.lock',
  'cargo.lock',
  'poetry.lock',
  'pipfile.lock',
  'shrinkwrap.json',
  'npm-shrinkwrap.json',
]);

/** Exact extension matches, including compound extensions (case-insensitive). */
const EXCLUDED_EXTENSIONS = new Set([
  '.lock',
  '.min.js',
  '.min.css',
  '.min.html',
  '.bundle.js',
  '.bundle.css',
  '.generated.ts',
  '.generated.js',
  '.d.ts', // TypeScript declaration files
]);

/** Directory substrings (POSIX, slash-delimited) that indicate generated/vendored content. */
const EXCLUDED_DIRECTORIES = [
  '/dist/',
  '/build/',
  '/out/',
  '/output/',
  '/node_modules/',
  '/vendor/',
  '/vendored/',
  '/third_party/',
  '/third-party/',
  '/external/',
  '/.next/',
  '/.nuxt/',
  '/.svelte-kit/',
  '/coverage/',
  '/__pycache__/',
  '/.tox/',
  '/venv/',
  '/.venv/',
  '/target/release/',
  '/target/debug/',
];

/** Regex filename patterns for generated artifacts not caught by the exact sets. */
const EXCLUDED_FILENAME_PATTERNS = [
  /^.*\.min\.[a-z]+$/i, // *.min.*
  /^.*-min\.[a-z]+$/i, // *-min.*
  /^.*\.bundle\.[a-z]+$/i, // *.bundle.*
  /^.*\.generated\.[a-z]+$/i, // *.generated.*
  /^.*\.gen\.[a-z]+$/i, // *.gen.*
  /^.*\.auto\.[a-z]+$/i, // *.auto.*
  /^.*_generated\.[a-z]+$/i, // *_generated.*
  /^.*_gen\.[a-z]+$/i, // *_gen.*
  /^.*\.pb\.(go|js|ts|py|rb)$/i, // Protocol-buffer generated files
  /^.*_pb2?\.py$/i, // Python protobuf files
  /^.*\.pb\.h$/i, // C++ protobuf headers
  /^.*\.grpc\.[a-z]+$/i, // gRPC generated files
  /^.*\.swagger\.[a-z]+$/i, // Swagger generated files
  /^.*\.openapi\.[a-z]+$/i, // OpenAPI generated files
];

/**
 * Classify whether `filePath` is a generated or vendored file that should be
 * excluded from indexing/attribution. Accepts an OS-native or POSIX path,
 * absolute or relative; directory matching is most precise on repo-relative
 * paths (an absolute path whose prefix happens to contain e.g. `/build/` would
 * also match, so callers that index a project should pass workspace-relative
 * paths — see {@link WorkspaceIndexer}).
 */
export function isGeneratedFile(filePath: string): boolean {
  // Normalize separators to POSIX and strip leading slashes so directory
  // substring matching ('/dist/' etc.) works for both backslash and relative paths.
  const normalizedPath =
    posix.sep + filePath.split(sep).join(posix.sep).replace(/^\/+/, '');
  const fileName = basename(filePath).toLowerCase();
  const ext = extname(filePath).toLowerCase();

  // 1. Exact filename matches.
  if (EXCLUDED_FILENAMES.has(fileName)) return true;

  // 2. Extension matches (final segment).
  if (EXCLUDED_EXTENSIONS.has(ext)) return true;

  // 2b. Compound extensions — extname() returns only the last segment, so
  // reconstruct '.' + the last two dot-parts to catch '.min.js' / '.d.ts'.
  const parts = fileName.split('.');
  if (parts.length > 2) {
    const compoundExt = '.' + parts.slice(-2).join('.');
    if (EXCLUDED_EXTENSIONS.has(compoundExt)) return true;
  }

  // 3. Directory substring matches over the normalized path.
  for (const dir of EXCLUDED_DIRECTORIES) {
    if (normalizedPath.includes(dir)) return true;
  }

  // 4. Regex filename patterns.
  for (const pattern of EXCLUDED_FILENAME_PATTERNS) {
    if (pattern.test(fileName)) return true;
  }

  return false;
}

/** Return `files` with every generated/vendored path removed. */
export function filterGeneratedFiles(files: string[]): string[] {
  return files.filter(file => !isGeneratedFile(file));
}
