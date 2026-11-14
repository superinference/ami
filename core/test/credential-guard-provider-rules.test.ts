import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  containsCredential,
  redactCredentials,
  findCredentials,
  getCredentialLabel,
  REDACT_PLACEHOLDER,
} from '../src/credential-guard';

// Fake-but-pattern-matching fixtures for the expanded gitleaks-derived rule set.
const GCP_SA = 'svc-deploy@my-project-123.iam.gserviceaccount.com';
const STRIPE = 'sk_live_' + 'a'.repeat(24);
const STRIPE_RK = 'rk_test_' + 'b'.repeat(20);
const NPM = 'npm_' + 'c'.repeat(36);
const SHOPIFY_AT = 'shpat_' + 'd'.repeat(32);
const SHOPIFY_SS = 'shpss_' + 'e'.repeat(32);
const DO_PAT = 'dop_v1_' + 'f'.repeat(64);
const DO_OAUTH = 'doo_v1_' + '0'.repeat(64);
const HF = 'hf_' + 'g'.repeat(34);
const SENTRY = 'sntryu_' + '1'.repeat(64);
const GRAFANA_CLOUD = 'glc_' + 'h'.repeat(40);
const GRAFANA_SA = 'glsa_' + 'i'.repeat(32) + '_' + 'abcdef01';
const GH_APP = 'ghs_' + 'j'.repeat(36);
const GH_REFRESH = 'ghr_' + 'k'.repeat(36);
const GITLAB_DEPLOY = 'gldt-' + 'l'.repeat(20);
const DATABRICKS = 'dapi' + '2'.repeat(32);
const PULUMI = 'pul-' + '3'.repeat(40);
const POSTMAN = 'PMAK-' + 'a'.repeat(24) + '-' + 'b'.repeat(34);
const TWILIO = 'SK' + '0'.repeat(32);
const SLACK_APP = 'xapp-1-ABC123-456-deadbeef';

const CASES: Array<[string, string, string]> = [
  // [fixture, expected type, label]
  [GCP_SA, 'gcp_service_account', 'GCP Service Account'],
  [STRIPE, 'stripe_key', 'Stripe Key'],
  [STRIPE_RK, 'stripe_key', 'Stripe Key'],
  [NPM, 'npm_token', 'NPM Token'],
  [SHOPIFY_AT, 'shopify_token', 'Shopify Token'],
  [SHOPIFY_SS, 'shopify_token', 'Shopify Token'],
  [DO_PAT, 'digitalocean_token', 'DigitalOcean Token'],
  [DO_OAUTH, 'digitalocean_token', 'DigitalOcean Token'],
  [HF, 'huggingface_token', 'HuggingFace Token'],
  [SENTRY, 'sentry_token', 'Sentry Token'],
  [GRAFANA_CLOUD, 'grafana_token', 'Grafana Token'],
  [GRAFANA_SA, 'grafana_token', 'Grafana Token'],
  [GH_APP, 'github_app_token', 'GitHub App Token'],
  [GH_REFRESH, 'github_refresh_token', 'GitHub Refresh Token'],
  [GITLAB_DEPLOY, 'gitlab_deploy_token', 'GitLab Deploy Token'],
  [DATABRICKS, 'databricks_token', 'Databricks Token'],
  [PULUMI, 'pulumi_token', 'Pulumi Token'],
  [POSTMAN, 'postman_token', 'Postman Token'],
  [TWILIO, 'twilio_key', 'Twilio Key'],
  [SLACK_APP, 'slack_app', 'Slack App'],
];

describe('expanded provider secret rules', () => {
  for (const [fixture, type, label] of CASES) {
    it(`detects ${type}`, () => {
      const text = `config value: ${fixture} end`;
      assert.equal(containsCredential(text), true, `containsCredential failed for ${type}`);
      const creds = findCredentials(text);
      const match = creds.find(c => c.type === type);
      assert.ok(match, `findCredentials missing ${type}; got ${creds.map(c => c.type).join(',')}`);
      assert.equal(match!.label, label);
      // Redaction removes the raw secret.
      const red = redactCredentials(text);
      assert.ok(!red.includes(fixture), `redactCredentials left ${type} in output`);
      assert.ok(red.includes(REDACT_PLACEHOLDER));
    });
  }

  it('does not flag an ordinary gserviceaccount-looking word', () => {
    assert.equal(containsCredential('this is a normal sentence with words'), false);
  });

  it('getCredentialLabel falls back to Title Case for unknown types', () => {
    assert.equal(getCredentialLabel('some_new_provider'), 'Some New Provider');
  });

  it('getCredentialLabel applies brand special-casing', () => {
    assert.equal(getCredentialLabel('aws_access_key'), 'AWS Access Key');
    assert.equal(getCredentialLabel('openai_key'), 'OpenAI Key');
    assert.equal(getCredentialLabel('jwt'), 'JWT');
  });
});
