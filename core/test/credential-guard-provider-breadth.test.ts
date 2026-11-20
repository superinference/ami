import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  containsCredential,
  redactCredentials,
  findCredentials,
} from '../src/credential-guard';

// Parity: the reference's secretScanner SECRET_RULES carries ~35 high-confidence,
// gitleaks-derived provider rules. These are the provider families AMI's
// credential-guard previously MISSED, so a leaked token of these kinds passed
// through redactCredentials / containsCredential / findCredentials unredacted on
// every egress path (tool output, /export transcript, provider errors, logs).
// Fixtures are synthetic token SHAPES (no real secrets).
const PROJ58 = 'A'.repeat(58);

// [fixture, expected type, expected human-readable label]
const CASES: Array<[string, string, string]> = [
  // Azure AD client secret — distinctive `…\dQ~…` infix.
  ['abc8Q~abcdefghijklmnopqrstuvwxyz1234567', 'azure_ad_secret', 'Azure AD Secret'],
  // OpenAI project/service/admin keys (new sk-proj-…T3BlbkFJ… format).
  [`sk-proj-${PROJ58}T3BlbkFJ${PROJ58}`, 'openai_project_key', 'OpenAI Project Key'],
  // SendGrid API token.
  [`SG.${'a'.repeat(66)}`, 'sendgrid_token', 'SendGrid Token'],
  // PyPI upload token.
  [`pypi-AgEIcHlwaS5vcmc${'a'.repeat(60)}`, 'pypi_token', 'PyPI Token'],
  // HashiCorp Terraform Cloud API token.
  [`${'a'.repeat(14)}.atlasv1.${'b'.repeat(65)}`, 'hashicorp_token', 'HashiCorp Token'],
  // Grafana API key (base64 eyJrIjoi…).
  [`eyJrIjoi${'a'.repeat(80)}`, 'grafana_api_key', 'Grafana API Key'],
  // Sentry org token (sntrys_…).
  [
    `sntrys_eyJpYXQiO${'a'.repeat(20)}cmVnaW9uX3VybCI6${'a'.repeat(20)}_${'a'.repeat(43)}`,
    'sentry_org_token',
    'Sentry Org Token',
  ],
];

describe('credential-guard :: provider breadth parity', () => {
  for (const [fixture, type, label] of CASES) {
    it(`detects + redacts ${type}`, () => {
      const text = `value: ${fixture}`;
      assert.equal(containsCredential(text), true, `containsCredential missed ${type}`);

      const creds = findCredentials(text);
      const match = creds.find(c => c.type === type);
      assert.ok(match, `findCredentials missing ${type}; got ${creds.map(c => c.type).join(',')}`);
      assert.equal(match!.label, label, `wrong label for ${type}`);

      const red = redactCredentials(text);
      assert.ok(!red.includes(fixture), `redactCredentials left ${type} token in output`);
      assert.ok(red.includes('[REDACTED]'), `redactCredentials did not emit placeholder for ${type}`);
    });
  }

  it('does not false-positive on ordinary env assignments', () => {
    assert.equal(containsCredential('ACCESS_LEVEL=admin'), false);
    assert.equal(containsCredential('HTTP_PORT=8080'), false);
  });
});
