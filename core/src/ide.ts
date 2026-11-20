/**
 * JetBrains IDE detection building blocks.
 *
 * Parity building block mirroring the reference's `src/utils/jetbrains.ts`
 * (`ideNameToDirMap` + `buildCommonPluginDirectoryPaths`). AMI is CLI/TUI-focused,
 * so these are self-contained, dependency-injected primitives (no fs side effects
 * in the pure resolvers) that a future IDE-integration consumer can build on:
 *
 *  - `JetBrainsIdeType` — the supported-IDE enum (15 JetBrains IDEs).
 *  - `JETBRAINS_IDE_DIR_MAP` — config table mapping each IDE identifier to its
 *    on-disk directory-name pattern(s).
 *  - `resolveJetBrainsConfigDirectories` — platform-aware candidate-directory
 *    resolution with APPDATA/LOCALAPPDATA env-var fallbacks and XDG-style
 *    (~/.config, ~/.local/share) Linux defaults.
 *
 * The resolvers take an injected `{ platform, homeDir, env }` context so they are
 * deterministic and fully testable across darwin/win32/linux without touching the
 * real machine; `defaultIdeDirectoryContext()` supplies the live runtime values.
 */

import { join } from 'node:path';
import { homedir, platform as osPlatform } from 'node:os';

/**
 * Supported JetBrains IDE identifiers. Matches the reference's `IdeType` JetBrains
 * subset — the VS Code-family identifiers are intentionally omitted (AMI does not
 * pursue VS Code integration).
 */
export const JET_BRAINS_IDE_TYPES = ['pycharm', 'intellij', 'webstorm', 'phpstorm', 'rubymine', 'clion', 'goland', 'rider', 'datagrip', 'appcode', 'dataspell', 'aqua', 'gateway', 'fleet', 'androidstudio'] as const;
export type JetBrainsIdeType = (typeof JET_BRAINS_IDE_TYPES)[number];

/**
 * Config table mapping each supported JetBrains IDE to the on-disk
 * directory-name pattern(s) its settings/plugin directories are prefixed with.
 * A single IDE can map to multiple patterns (e.g. IntelliJ ships both the
 * commercial `IntelliJIdea*` and community `IdeaIC*` directories).
 */
export const JETBRAINS_IDE_DIR_MAP: Readonly<Record<JetBrainsIdeType, readonly string[]>> = Object.freeze({
  pycharm: ['PyCharm'],
  intellij: ['IntelliJIdea', 'IdeaIC'],
  webstorm: ['WebStorm'],
  phpstorm: ['PhpStorm'],
  rubymine: ['RubyMine'],
  clion: ['CLion'],
  goland: ['GoLand'],
  rider: ['Rider'],
  datagrip: ['DataGrip'],
  appcode: ['AppCode'],
  dataspell: ['DataSpell'],
  aqua: ['Aqua'],
  gateway: ['Gateway'],
  fleet: ['Fleet'],
  androidstudio: ['AndroidStudio'],
});

/** All supported JetBrains IDE identifiers, in config-table order. */
export const SUPPORTED_JETBRAINS_IDES: readonly JetBrainsIdeType[] = Object.freeze(
  Object.keys(JETBRAINS_IDE_DIR_MAP) as JetBrainsIdeType[],
);

/**
 * Type guard: is `name` a supported JetBrains IDE identifier? Case-insensitive,
 * mirroring the reference's `ideName.toLowerCase()` lookup.
 */
export function isSupportedJetBrainsIde(name: string): name is JetBrainsIdeType {
  return Object.prototype.hasOwnProperty.call(JETBRAINS_IDE_DIR_MAP, name.toLowerCase());
}

/**
 * Directory-name pattern(s) for an IDE identifier, or `[]` for an unknown IDE.
 * Case-insensitive.
 */
export function getJetBrainsIdeDirPatterns(name: string): readonly string[] {
  return JETBRAINS_IDE_DIR_MAP[name.toLowerCase() as JetBrainsIdeType] ?? [];
}

/** Platform/home/env context injected into the pure directory resolvers. */
export interface IdeDirectoryContext {
  platform: NodeJS.Platform;
  homeDir: string;
  env: Record<string, string | undefined>;
}

/** Live runtime context (`os.platform()`, `os.homedir()`, `process.env`). */
export function defaultIdeDirectoryContext(): IdeDirectoryContext {
  return { platform: osPlatform(), homeDir: homedir(), env: process.env };
}

/**
 * Resolve the candidate base directories under which a JetBrains IDE stores its
 * settings/plugin directories, branching per-platform with APPDATA/LOCALAPPDATA
 * env-var fallbacks (win32) and XDG-style defaults (linux).
 *
 * Pure: performs no filesystem access and returns the candidate list in priority
 * order. Returns `[]` for an unknown IDE or an unsupported platform.
 */
export function resolveJetBrainsConfigDirectories(
  ideName: string,
  ctx: IdeDirectoryContext = defaultIdeDirectoryContext(),
): string[] {
  const patterns = getJetBrainsIdeDirPatterns(ideName);
  if (patterns.length === 0) return [];

  const { platform, homeDir, env } = ctx;
  const directories: string[] = [];

  const appData = env.APPDATA || join(homeDir, 'AppData', 'Roaming');
  const localAppData = env.LOCALAPPDATA || join(homeDir, 'AppData', 'Local');
  const isAndroidStudio = ideName.toLowerCase() === 'androidstudio';

  switch (platform) {
    case 'darwin':
      directories.push(
        join(homeDir, 'Library', 'Application Support', 'JetBrains'),
        join(homeDir, 'Library', 'Application Support'),
      );
      if (isAndroidStudio) {
        directories.push(join(homeDir, 'Library', 'Application Support', 'Google'));
      }
      break;

    case 'win32':
      directories.push(join(appData, 'JetBrains'), join(localAppData, 'JetBrains'), appData);
      if (isAndroidStudio) {
        directories.push(join(localAppData, 'Google'));
      }
      break;

    case 'linux':
      directories.push(
        join(homeDir, '.config', 'JetBrains'),
        join(homeDir, '.local', 'share', 'JetBrains'),
      );
      for (const pattern of patterns) {
        directories.push(join(homeDir, '.' + pattern));
      }
      if (isAndroidStudio) {
        directories.push(join(homeDir, '.config', 'Google'));
      }
      break;

    default:
      break;
  }

  return directories;
}
