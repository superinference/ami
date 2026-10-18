import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { PermissionManager, isBuildConfigFile } from '../src/permissions';

// ---------------------------------------------------------------------------
// Unit tests for isBuildConfigFile
// ---------------------------------------------------------------------------

describe('isBuildConfigFile — basename matching', () => {
  it('identifies Makefile variants', () => {
    assert.equal(isBuildConfigFile('/project/Makefile'), true);
    assert.equal(isBuildConfigFile('/project/GNUmakefile'), true);
    assert.equal(isBuildConfigFile('/project/makefile'), true);
  });

  it('identifies Dockerfile variants', () => {
    assert.equal(isBuildConfigFile('/project/Dockerfile'), true);
    assert.equal(isBuildConfigFile('/project/docker-compose.yml'), true);
    assert.equal(isBuildConfigFile('/project/docker-compose.yaml'), true);
    assert.equal(isBuildConfigFile('/project/.dockerignore'), true);
  });

  it('identifies CI config files', () => {
    assert.equal(isBuildConfigFile('/project/.github/workflows/ci.yml'), true);
    assert.equal(isBuildConfigFile('/project/.github/workflows/deploy.yaml'), true);
    assert.equal(isBuildConfigFile('/project/.gitlab-ci.yml'), true);
    assert.equal(isBuildConfigFile('/project/.gitlab-ci.yaml'), true);
    assert.equal(isBuildConfigFile('/project/Jenkinsfile'), true);
    assert.equal(isBuildConfigFile('/project/.circleci/config.yml'), true);
  });

  it('identifies Python build config files', () => {
    assert.equal(isBuildConfigFile('/project/pyproject.toml'), true);
    assert.equal(isBuildConfigFile('/project/setup.py'), true);
    assert.equal(isBuildConfigFile('/project/setup.cfg'), true);
  });

  it('identifies Rust build config (Cargo.toml)', () => {
    assert.equal(isBuildConfigFile('/project/Cargo.toml'), true);
  });

  it('identifies Go module files', () => {
    assert.equal(isBuildConfigFile('/project/go.mod'), true);
    assert.equal(isBuildConfigFile('/project/go.sum'), true);
  });

  it('identifies npm/yarn config files', () => {
    assert.equal(isBuildConfigFile('/project/.npmrc'), true);
    assert.equal(isBuildConfigFile('/project/.yarnrc'), true);
    assert.equal(isBuildConfigFile('/project/.yarnrc.yml'), true);
  });

  it('identifies .env files', () => {
    assert.equal(isBuildConfigFile('/project/.env'), true);
    assert.equal(isBuildConfigFile('/project/.env.local'), true);
    assert.equal(isBuildConfigFile('/project/.env.production'), true);
  });

  it('returns false for regular source files', () => {
    assert.equal(isBuildConfigFile('/project/src/index.ts'), false);
    assert.equal(isBuildConfigFile('/project/README.md'), false);
    assert.equal(isBuildConfigFile('/project/app.js'), false);
    assert.equal(isBuildConfigFile('/project/styles.css'), false);
    assert.equal(isBuildConfigFile('/project/data.json'), false);
    assert.equal(isBuildConfigFile('/project/config.ts'), false);
  });

  it('returns false for files with similar names but not exact matches', () => {
    assert.equal(isBuildConfigFile('/project/src/Makefile.ts'), false);
    assert.equal(isBuildConfigFile('/project/myDockerfile'), false);
    assert.equal(isBuildConfigFile('/project/not-a-Jenkinsfile'), false);
  });
});

// ---------------------------------------------------------------------------
// Integration tests: acceptEdits mode + build config guard
// ---------------------------------------------------------------------------

describe('PermissionManager — acceptEdits build-config guard', () => {
  it('auto-allows editing a regular .ts file in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/src/app.ts' });
    assert.equal(result, 'allow');
  });

  it('auto-allows writing a regular .js file in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_write', { file_path: '/project/src/utils.js' });
    assert.equal(result, 'allow');
  });

  it('requires approval for Makefile in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/Makefile' });
    assert.equal(result, 'ask');
  });

  it('requires approval for Dockerfile in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_write', { file_path: '/project/Dockerfile' });
    assert.equal(result, 'ask');
  });

  it('requires approval for .github/workflows/ci.yml in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/.github/workflows/ci.yml' });
    assert.equal(result, 'ask');
  });

  it('requires approval for pyproject.toml in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/pyproject.toml' });
    assert.equal(result, 'ask');
  });

  it('requires approval for .env in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/.env' });
    assert.equal(result, 'ask');
  });

  it('requires approval for .env.production in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_write', { file_path: '/project/.env.production' });
    assert.equal(result, 'ask');
  });

  it('requires approval for Cargo.toml in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/Cargo.toml' });
    assert.equal(result, 'ask');
  });

  it('requires approval for go.mod in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/go.mod' });
    assert.equal(result, 'ask');
  });

  it('requires approval for docker-compose.yml in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/docker-compose.yml' });
    assert.equal(result, 'ask');
  });

  it('requires approval for .gitlab-ci.yml in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_write', { file_path: '/project/.gitlab-ci.yml' });
    assert.equal(result, 'ask');
  });

  it('requires approval for Jenkinsfile in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/Jenkinsfile' });
    assert.equal(result, 'ask');
  });

  it('requires approval for .circleci/config.yml in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/.circleci/config.yml' });
    assert.equal(result, 'ask');
  });

  it('requires approval for setup.py in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/setup.py' });
    assert.equal(result, 'ask');
  });

  it('requires approval for .npmrc in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/.npmrc' });
    assert.equal(result, 'ask');
  });

  it('requires approval for .yarnrc.yml in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('file_edit', { file_path: '/project/.yarnrc.yml' });
    assert.equal(result, 'ask');
  });

  it('requires approval for notebook_edit on build config in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    // notebook_edit uses 'path' not 'file_path'
    const result = await pm.check('notebook_edit', { path: '/project/Makefile' });
    assert.equal(result, 'ask');
  });

  it('requires approval for multi_edit on build config in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('multi_edit', { file_path: '/project/Dockerfile' });
    assert.equal(result, 'ask');
  });
});

// ---------------------------------------------------------------------------
// Non-acceptEdits modes: no special treatment for build config
// ---------------------------------------------------------------------------

describe('PermissionManager — other modes do NOT special-case build config', () => {
  it('ask mode treats build config same as any other file (ask)', async () => {
    const pm = new PermissionManager('ask');
    // Both regular files and build config should return 'ask' in ask mode
    assert.equal(await pm.check('file_edit', { file_path: '/project/Makefile' }), 'ask');
    assert.equal(await pm.check('file_edit', { file_path: '/project/src/app.ts' }), 'ask');
  });

  it('auto-allow mode allows build config (no special guard)', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('file_edit', { file_path: '/project/Makefile' }), 'allow');
    assert.equal(await pm.check('file_edit', { file_path: '/project/Dockerfile' }), 'allow');
  });

  it('deny-all mode denies build config (no special guard needed)', async () => {
    const pm = new PermissionManager('deny-all');
    assert.equal(await pm.check('file_edit', { file_path: '/project/Makefile' }), 'deny');
  });
});
