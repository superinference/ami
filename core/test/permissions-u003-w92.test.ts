import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getSimpleCommandPrefix,
  suggestAllowPrefix,
  getCompoundCommandPrefixes,
  BASH_COMMAND_SPECS,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w92 — per-first-subcommand DEPTH_RULES compound keys
// (parity: references/claude/claude-code-main/src/utils/shell/specPrefix.ts
//  DEPTH_RULES compound keys `gcloud compute`:6 / `gcloud beta`:6 + the bare
//  `dotnet`:3 entry, resolved by calculateDepth's `${command} ${sub}` lookup)
// ---------------------------------------------------------------------------

test('subcommandDepth lets a deep subcommand tree extend past the command default', () => {
  // gcloud default depth is 4; `gcloud compute` / `gcloud beta` override to 6 so
  // the prefix reaches the real leaf verb instead of stopping mid-tree.
  assert.equal(
    getSimpleCommandPrefix('gcloud compute instances describe mygroup verb now'),
    'gcloud compute instances describe mygroup verb',
  );
  assert.equal(
    getSimpleCommandPrefix('gcloud beta compute networks subnets list a b'),
    'gcloud beta compute networks subnets list',
  );
});

test('subcommandDepth override is scoped to the matching first subcommand only', () => {
  // A non-overridden gcloud subcommand keeps the command default depth (4), so a
  // sibling group is NOT widened by the `compute`/`beta` override.
  assert.equal(getSimpleCommandPrefix('gcloud config list extra tail'), 'gcloud config list extra');
  assert.equal(getSimpleCommandPrefix('gcloud scheduler jobs list more'), 'gcloud scheduler jobs list');
});

test('dotnet reaches the 3rd-word action verb (DEPTH_RULES[dotnet]=3)', () => {
  assert.equal(getSimpleCommandPrefix('dotnet tool install mytool'), 'dotnet tool install');
  assert.equal(getSimpleCommandPrefix('dotnet nuget push pkg.nupkg'), 'dotnet nuget push');
  assert.equal(getSimpleCommandPrefix('dotnet build run foo'), 'dotnet build run');
  // A bare `dotnet build` (nothing deeper) is unchanged.
  assert.equal(getSimpleCommandPrefix('dotnet build'), 'dotnet build');
});

test('BASH_COMMAND_SPECS declares the compound-depth model', () => {
  assert.equal(BASH_COMMAND_SPECS.gcloud!.depth, 4);
  assert.equal(BASH_COMMAND_SPECS.gcloud!.subcommandDepth?.compute, 6);
  assert.equal(BASH_COMMAND_SPECS.gcloud!.subcommandDepth?.beta, 6);
  assert.equal(BASH_COMMAND_SPECS.dotnet!.depth, 3);
  // A command with no subcommandDepth map is left untouched.
  assert.equal(BASH_COMMAND_SPECS.git!.subcommandDepth, undefined);
});

test('no regression: existing per-command depths and flag-value skipping hold', () => {
  assert.equal(getSimpleCommandPrefix('kubectl -n kube-system get pods'), 'kubectl get pods');
  assert.equal(getSimpleCommandPrefix('docker compose up -d'), 'docker compose up');
  assert.equal(getSimpleCommandPrefix('aws s3api get-object --bucket b'), 'aws s3api get-object');
  assert.equal(getSimpleCommandPrefix('npm run build'), 'npm run');
  assert.equal(getSimpleCommandPrefix('git commit -m x'), 'git commit');
});

test('compound-depth flows through suggestAllowPrefix end-to-end', () => {
  // Collection stops at the first flag (`--zone`) even though depth is 6, so the
  // suggested rule is scoped to the four subcommand words.
  assert.equal(
    suggestAllowPrefix('gcloud compute instances list --zone us-central1'),
    'gcloud compute instances list',
  );
  // dotnet rule suggestion reaches the action verb.
  assert.equal(suggestAllowPrefix('dotnet tool install mytool'), 'dotnet tool install');
});

test('compound-command derivation reuses the deeper prefix per subcommand', () => {
  assert.deepEqual(
    getCompoundCommandPrefixes('dotnet tool install x && dotnet tool update y'),
    ['dotnet tool'],
  );
});

test('a rule seeded from the deeper prefix auto-allows the matching command', async () => {
  // End-to-end: derive the scoped prefix the user would "allow always", seed it as
  // a rule, and confirm the ORIGINAL command auto-allows while a sibling verb the
  // deeper prefix excludes still prompts.
  const cmd = 'gcloud compute instances describe --zone us-central1';
  const prefix = suggestAllowPrefix(cmd);
  assert.equal(prefix, 'gcloud compute instances describe');
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'allow', pattern: `${prefix} *` },
  ]);
  assert.equal(await pm.check('bash', { command: cmd }), 'allow');
  // A sibling verb the deeper prefix does NOT cover still prompts.
  assert.equal(
    await pm.check('bash', { command: 'gcloud compute instances delete vm-1' }),
    'ask',
  );
});
