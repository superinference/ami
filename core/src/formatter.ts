import { execSync } from 'child_process';
import * as path from 'path';

/** @public Formatter configuration for library consumers to define custom formatters. */
export interface FormatterConfig {
  extensions: string[];
  command: string;
  args: string[];
  check?: string;
}

const BUILT_IN_FORMATTERS: FormatterConfig[] = [
  { extensions: ['.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.py', '.pyw'], command: 'ruff', args: ['format'], check: 'ruff --version' },
  { extensions: ['.go'], command: 'gofmt', args: ['-w'], check: 'gofmt --help' },
  { extensions: ['.rs'], command: 'rustfmt', args: [], check: 'rustfmt --version' },
  { extensions: ['.java'], command: 'google-java-format', args: ['-i'], check: 'google-java-format --version' },
  { extensions: ['.rb'], command: 'rubocop', args: ['-a', '--fail-level', 'fatal'], check: 'rubocop --version' },
  { extensions: ['.php'], command: 'php-cs-fixer', args: ['fix'], check: 'php-cs-fixer --version' },
  { extensions: ['.swift'], command: 'swift-format', args: ['format', '-i'], check: 'swift-format --version' },
  { extensions: ['.kt', '.kts'], command: 'ktlint', args: ['-F'], check: 'ktlint --version' },
  { extensions: ['.css', '.scss', '.less'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.json'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.yaml', '.yml'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.md'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.html', '.htm'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.c', '.cpp', '.h', '.hpp'], command: 'clang-format', args: ['-i'], check: 'clang-format --version' },
  { extensions: ['.sh', '.bash'], command: 'shfmt', args: ['-w'], check: 'shfmt --version' },
  { extensions: ['.lua'], command: 'stylua', args: [], check: 'stylua --version' },
  { extensions: ['.zig'], command: 'zig', args: ['fmt'], check: 'zig version' },
  { extensions: ['.dart'], command: 'dart', args: ['format'], check: 'dart --version' },
  { extensions: ['.ex', '.exs'], command: 'mix', args: ['format'], check: 'mix --version' },
];

const formatterCache = new Map<string, boolean>();

function isFormatterAvailable(config: FormatterConfig): boolean {
  if (!config.check) return false;
  const key = config.check;
  if (formatterCache.has(key)) return formatterCache.get(key)!;
  try {
    execSync(config.check, { timeout: 5000, stdio: 'pipe' });
    formatterCache.set(key, true);
    return true;
  } catch {
    formatterCache.set(key, false);
    return false;
  }
}

export function formatFile(filePath: string, cwd: string): { formatted: boolean; error?: string } {
  const ext = path.extname(filePath).toLowerCase();
  const config = BUILT_IN_FORMATTERS.find(f => f.extensions.includes(ext));
  if (!config) return { formatted: false };
  if (!isFormatterAvailable(config)) return { formatted: false };
  try {
    execSync(`${config.command} ${config.args.join(' ')} '${filePath.replace(/'/g, "'\\''")}'`, {
      cwd, timeout: 15000, stdio: 'pipe',
    });
    return { formatted: true };
  } catch (err) {
    return { formatted: false, error: err instanceof Error ? err.message : String(err) };
  }
}

export function getAvailableFormatters(_cwd: string): string[] {
  return BUILT_IN_FORMATTERS
    .filter(f => isFormatterAvailable(f))
    .map(f => `${f.extensions.join(', ')} → ${f.command} ${f.args.join(' ')}`);
}
