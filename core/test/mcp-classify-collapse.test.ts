import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  classifyMcpToolForCollapse,
  normalizeCollapseToolName,
} from '../src/mcp/classify-collapse';

describe('normalizeCollapseToolName', () => {
  it('splits camelCase before lowercasing', () => {
    assert.equal(normalizeCollapseToolName('searchJiraIssuesUsingJql'), 'search_jira_issues_using_jql');
  });
  it('converts kebab-case and whitespace to snake_case', () => {
    assert.equal(normalizeCollapseToolName('brave-web-search'), 'brave_web_search');
    assert.equal(normalizeCollapseToolName('List Resources'), 'list_resources');
  });
  it('leaves an already-normalized name unchanged', () => {
    assert.equal(normalizeCollapseToolName('get_issue'), 'get_issue');
  });
});

describe('classifyMcpToolForCollapse — curated allowlist (names a prefix rule misses)', () => {
  it('classifies allowlisted bare search names as search', () => {
    for (const n of ['search', 'find']) {
      const c = classifyMcpToolForCollapse('srv', n);
      assert.equal(c.isSearch, true, n);
      assert.equal(c.isRead, false, n);
    }
  });

  it('classifies allowlisted non-verb-prefixed read names as read', () => {
    for (const n of ['esql', 'recommend', 'whoami', 'count', 'query', 'explain', 'export', 'namespaces_list', 'fetch_object', 'user_info', 'kubectl_get', 'pods_top']) {
      const c = classifyMcpToolForCollapse('srv', n);
      assert.equal(c.isRead, true, n);
      assert.equal(c.isSearch, false, n);
    }
  });

  it('matches allowlist entries through camelCase/kebab normalization', () => {
    assert.equal(classifyMcpToolForCollapse('atlassian', 'searchJiraIssuesUsingJql').isSearch, true);
    assert.equal(classifyMcpToolForCollapse('brave', 'brave-web-search').isSearch, true);
  });
});

describe('classifyMcpToolForCollapse — verb-prefix heuristic (arbitrary servers)', () => {
  it('classifies read-prefixed tools not in the allowlist as read', () => {
    for (const n of ['get_widget', 'list_frobs', 'read_blob', 'fetch_thing', 'describe_box', 'view_doc', 'show_state', 'retrieve_item', 'load_cfg']) {
      const c = classifyMcpToolForCollapse('srv', n);
      assert.equal(c.isRead, true, n);
      assert.equal(c.isSearch, false, n);
    }
  });
  it('classifies search-prefixed tools not in the allowlist as search', () => {
    for (const n of ['search_widgets', 'find_symbol', 'query_rows', 'lookup_row', 'browse_catalog']) {
      const c = classifyMcpToolForCollapse('srv', n);
      assert.equal(c.isSearch, true, n);
      assert.equal(c.isRead, false, n);
    }
  });
});

describe('classifyMcpToolForCollapse — mutations and conservative default', () => {
  it('classifies mutating ops as neither read nor search (stay expanded)', () => {
    for (const n of ['send_message', 'create_issue', 'update_pr', 'merge_branch', 'delete_record']) {
      const c = classifyMcpToolForCollapse('srv', n);
      assert.equal(c.isRead, false, n);
      assert.equal(c.isSearch, false, n);
    }
  });
  it('treats an unknown tool conservatively (neither)', () => {
    const c = classifyMcpToolForCollapse('srv', 'frobnicate');
    assert.equal(c.isRead, false);
    assert.equal(c.isSearch, false);
  });
  it('gives search priority over read when both could match', () => {
    // `search_issues` is in the search allowlist and starts with a search prefix.
    const c = classifyMcpToolForCollapse('gh', 'search_issues');
    assert.equal(c.isSearch, true);
    assert.equal(c.isRead, false);
  });
});
