/**
 * Classify an MCP tool as a search/read operation for transcript UI collapsing.
 *
 * Read/search results are low-signal and safe to fold away; mutations
 * (`send_message`, `create_*`, `update_*`, …) must stay EXPANDED so the user
 * actually sees the effect of a write. Returns `{ isSearch: false, isRead: false }`
 * for anything it cannot prove is a read/search op (CONSERVATIVE default — an
 * unknown tool never collapses).
 *
 * Parity with the reference's `classifyMcpToolForCollapse`
 * (tools/MCPTool/classifyForCollapse.ts). AMI UNIONS two signals:
 *
 *  1. A curated per-tool allowlist (ported verbatim from the reference) keyed on the
 *     normalized tool name — catching the specific low-signal names a generic
 *     verb-prefix rule cannot infer (`search`, `find`, `esql`, `recommend`,
 *     `kubectl_get`, `namespaces_list`, `fetch_object`, `whoami`, …). Tool names
 *     are stable across installs even when the server name varies, so matching is
 *     keyed on the tool name alone after normalizing camelCase/kebab-case.
 *  2. A verb-prefix heuristic (`get_`/`list_`/`search_`/`find_`/…) so AMI
 *     generalizes to arbitrary MCP servers whose tools are NOT in the allowlist —
 *     a strict superset of the reference's allowlist-only behavior.
 *
 * This is the ONE canonical classifier (core building block); the CLI transcript
 * renderer delegates to it rather than re-implementing the heuristic.
 */

/** Whether an MCP tool is a low-signal read/search op (collapsible) or a mutation. */
export interface McpCollapseClass {
  isSearch: boolean;
  isRead: boolean;
}

// Verb prefixes that mark an MCP tool as a SEARCH (query-style, low-signal) op.
const MCP_SEARCH_PREFIXES = ['search_', 'find_', 'query_', 'lookup_', 'browse_'];
// Verb prefixes that mark an MCP tool as a READ (fetch-style, low-signal) op.
const MCP_READ_PREFIXES = ['get_', 'list_', 'read_', 'fetch_', 'describe_', 'view_', 'show_', 'retrieve_', 'load_'];

// prettier-ignore
const SEARCH_TOOLS = new Set([
  'asana_search_tasks', 'asana_typeahead_search', 'brave_local_search', 'brave_web_search',
  'confluence_search', 'deep_search_exa', 'find', 'find_activity', 'find_comments',
  'find_completed_tasks', 'find_dsns', 'find_filters', 'find_labels', 'find_memories_by_name',
  'find_monitors_matching_pattern', 'find_organizations', 'find_project_collaborators',
  'find_projects', 'find_related_articles', 'find_releases', 'find_sections', 'find_slow_spans',
  'find_tasks', 'find_tasks_by_date', 'find_teams', 'firecrawl_search', 'gcal_find_meeting_times',
  'gcal_find_my_free_time', 'gcal_find_user_emails', 'gmail_search_messages', 'google_drive_search',
  'jira_search', 'jira_search_fields', 'linkedin_search_exa', 'lookup_article_by_citation',
  'lookup_jira_account_id', 'obsidian_complex_search', 'obsidian_simple_search', 'people_search_exa',
  'perplexity_search', 'perplexity_search_web', 'pubmed_mesh_lookup', 'pubmed_search', 'search',
  'search_articles', 'search_audit_logs', 'search_catalog', 'search_code',
  'search_confluence_using_cql', 'search_dashboards', 'search_docs', 'search_documentation',
  'search_events', 'search_files', 'search_folders', 'search_issue_events', 'search_issues',
  'search_jira_issues_using_jql', 'search_knowledge', 'search_logs', 'search_memories',
  'search_modules', 'search_monitor_groups', 'search_monitors', 'search_nodes', 'search_orgs',
  'search_papers', 'search_policies', 'search_providers', 'search_pubmed', 'search_pubmed_advanced',
  'search_pubmed_key_words', 'search_pull_requests', 'search_records', 'search_repositories',
  'search_rum_events', 'search_spans', 'search_stripe_documentation', 'search_stripe_resources',
  'search_users', 'slack_search_channels', 'slack_search_public',
  'slack_search_public_and_private', 'slack_search_users', 'tavily_search',
  'web_search_advanced_exa', 'web_search_exa',
]);

// prettier-ignore
const READ_TOOLS = new Set([
  'actions_get', 'actions_list', 'aggregate', 'aggregate_logs', 'aggregate_rum_events',
  'aggregate_spans', 'analyze_log_group', 'analyze_metric', 'analyze_trace', 'asana_get_attachment',
  'asana_get_attachments_for_object', 'asana_get_goal', 'asana_get_goals',
  'asana_get_items_for_portfolio', 'asana_get_parent_goals_for_goal', 'asana_get_portfolio',
  'asana_get_portfolios', 'asana_get_project', 'asana_get_projects', 'asana_get_project_sections',
  'asana_get_projects_for_team', 'asana_get_projects_for_workspace', 'asana_get_project_status',
  'asana_get_project_statuses', 'asana_get_project_task_counts', 'asana_get_stories_for_task',
  'asana_get_task', 'asana_get_tasks', 'asana_get_teams_for_user', 'asana_get_teams_for_workspace',
  'asana_get_team_users', 'asana_get_time_period', 'asana_get_time_periods', 'asana_get_user',
  'asana_get_workspace_users', 'asana_list_workspaces', 'atlassian_user_info', 'bigquery_query',
  'bigquery_schema', 'browser_console_messages', 'browser_cookie_get', 'browser_cookie_list',
  'browser_get_config', 'browser_localstorage_get', 'browser_localstorage_list',
  'browser_network_requests', 'browser_route_list', 'browser_sessionstorage_get',
  'browser_sessionstorage_list', 'browser_snapshot', 'browser_storage_state',
  'browser_take_screenshot', 'check_can_delete_monitor', 'collection_indexes', 'collection_schema',
  'collection_storage_size', 'company_research_exa', 'compare_profiling_metrics',
  'confluence_get_comments', 'confluence_get_labels', 'confluence_get_page',
  'confluence_get_page_children', 'convert_article_ids', 'count', 'crawling_exa', 'db_stats',
  'deep_researcher_check', 'describe_log_groups', 'describe_table', 'directory_tree',
  'download_figma_images', 'download_paper', 'download_pubmed_pdf', 'esql', 'explain',
  'explain_resource', 'export', 'fetch', 'fetch_object', 'fetch_stripe_resources',
  'firecrawl_check_crawl_status', 'firecrawl_crawl', 'firecrawl_extract', 'firecrawl_map',
  'firecrawl_scrape', 'gcal_get_event', 'gcal_list_calendars', 'gcal_list_events',
  'generate_typescript_types', 'get_accessible_atlassian_resources', 'get_active_alarms',
  'get_advisors', 'get_alarm_history', 'get_alert_from_incident', 'get_alert_group',
  'get_annotations', 'get_annotation_tags', 'get_article_metadata', 'get_available_services',
  'get_change_event', 'get_code_context_exa', 'get_code_scanning_alert', 'get_comments',
  'get_commit', 'get_confluence_page', 'get_confluence_page_ancestors',
  'get_confluence_page_descendants', 'get_confluence_page_footer_comments',
  'get_confluence_page_inline_comments', 'get_confluence_spaces', 'get_copyright_status',
  'get_cost', 'get_current_oncall_users', 'get_dashboard', 'get_dashboard_by_uid',
  'get_dashboard_panel_queries', 'get_dashboard_property', 'get_dashboard_summary',
  'get_dataset_info', 'get_datasource', 'get_dependabot_alert', 'get_discussion',
  'get_discussion_comments', 'get_doc', 'get_document', 'get_edge_function',
  'get_escalation_policy', 'get_event_attachment', 'get_event_orchestration', 'get_figma_data',
  'get_file_contents', 'get_file_info', 'get_full_text_article', 'get_gist',
  'get_global_security_advisory', 'get_incident', 'get_incident_workflow', 'get_instance_by_name',
  'get_instance_details', 'get_issue', 'get_issue_details', 'get_issue_status',
  'get_issue_tag_values', 'get_jira_issue', 'get_jira_issue_remote_issue_links',
  'get_jira_project_issue_types_metadata', 'get_job_logs', 'get_label', 'get_latest_release',
  'get_log_entry', 'get_logs', 'get_mappings', 'get_me', 'get_metric_data', 'get_metric_metadata',
  'get_monitor', 'get_neo4j_schema', 'get_notebook', 'get_notification_details', 'get_oncall_shift',
  'get_organization', 'get_outlier_incident', 'get_overview', 'get_pages_in_confluence_space',
  'get_panel_image', 'get_paper_fulltext', 'get_past_incidents', 'get_productivity_stats',
  'get_profiling_metrics', 'get_project', 'get_project_url', 'get_publishable_keys',
  'get_pubmed_article_metadata', 'get_pull_request', 'get_pull_request_comments',
  'get_pull_request_files', 'get_pull_request_reviews', 'get_pull_request_status',
  'get_query_examples', 'get_record', 'get_related_incidents', 'get_release_by_tag',
  'get_repository_tree', 'get_rum_metric', 'get_schedule', 'get_secret_scanning_alert', 'get_self',
  'get_sentry_issue', 'get_sentry_resource', 'get_service', 'get_shards', 'get_sift_analysis',
  'get_sift_investigation', 'get_status_page_post', 'get_storage_config', 'get_stripe_account_info',
  'get_table_info', 'get_tag', 'get_team', 'get_team_members', 'get_teams', 'get_trace_details',
  'get_transitions_for_jira_issue', 'get_user', 'get_user_data', 'get_users',
  'get_visible_jira_projects', 'git_branch', 'git_diff', 'git_diff_staged', 'git_diff_unstaged',
  'git_log', 'git_show', 'git_status', 'gmail_get_profile', 'gmail_list_drafts',
  'gmail_list_labels', 'gmail_read_message', 'gmail_read_thread', 'google_drive_export',
  'google_drive_fetch', 'issue_read', 'jira_batch_get_changelogs', 'jira_download_attachments',
  'jira_get_agile_boards', 'jira_get_board_issues', 'jira_get_issue', 'jira_get_link_types',
  'jira_get_project_issues', 'jira_get_project_versions', 'jira_get_sprint_issues',
  'jira_get_sprints_from_board', 'jira_get_transitions', 'jira_get_user_profile',
  'jira_get_worklog', 'kubectl_context', 'kubectl_describe', 'kubectl_get', 'kubectl_logs',
  'list_alert_groups', 'list_alerts_from_incident', 'list_allowed_directories',
  'list_api_resources', 'list_bases', 'list_branches', 'list_change_events',
  'list_code_scanning_alerts', 'list_collections', 'list_comments', 'list_commits',
  'list_coupons', 'list_customers', 'list_cycles', 'list_dashboards', 'list_databases',
  'list_dataset_ids', 'list_datasources', 'list_dependabot_alerts', 'list_directory',
  'list_directory_with_sizes', 'list_discussion_categories', 'list_discussions', 'list_disputes',
  'list_documents', 'list_edge_functions', 'list_escalation_policies', 'list_event_orchestrations',
  'list_events', 'list_extensions', 'list_gists', 'list_global_security_advisories',
  'list_incident_notes', 'list_incidents', 'list_incident_workflows', 'list_indices',
  'list_instances', 'list_invoices', 'list_issue_events', 'list_issue_labels', 'list_issues',
  'list_issue_statuses', 'list_issue_types', 'list_label', 'list_log_entries',
  'list_loki_label_names', 'list_loki_label_values', 'list_migrations', 'list_monitors',
  'list_my_issues', 'list_notebooks', 'list_notifications', 'list_oncalls',
  'list_oncall_schedules', 'list_oncall_teams', 'list_oncall_users', 'list_organizations',
  'list_org_repository_security_advisories', 'list_papers', 'list_payment_intents', 'list_prices',
  'list_products', 'list_project_labels', 'list_projects', 'list_prometheus_label_names',
  'list_prometheus_label_values', 'list_prometheus_metric_metadata',
  'list_prometheus_metric_names', 'list_pull_requests', 'list_records', 'list_releases',
  'list_repository_security_advisories', 'list_rum_metrics', 'list_schedules',
  'list_schedule_users', 'list_secret_scanning_alerts', 'list_services',
  'list_sift_investigations', 'list_spans', 'list_starred_repositories', 'list_status_pages',
  'list_storage_buckets', 'list_subscriptions', 'list_table_ids', 'list_tables', 'list_tags',
  'list_team_members', 'list_teams', 'list_users', 'list_users_by_org', 'list_workspaces',
  'mongodb_logs', 'namespaces_list', 'nodes_log', 'nodes_top',
  'obsidian_batch_get_file_contents', 'obsidian_get_file_contents', 'obsidian_get_periodic_note',
  'obsidian_get_recent_changes', 'obsidian_get_recent_periodic_notes',
  'obsidian_list_files_in_dir', 'obsidian_list_files_in_vault', 'open_nodes', 'perplexity_ask',
  'perplexity_reason', 'perplexity_research', 'pods_get', 'pods_list', 'pods_list_in_namespace',
  'pods_log', 'pods_top', 'projects_get', 'projects_list', 'pubmed_cite', 'pubmed_fetch',
  'pubmed_pmc_fetch', 'pubmed_related', 'pubmed_spell', 'pull_request_read',
  'puppeteer_screenshot', 'query', 'query_dashboard_widget', 'query_loki_logs',
  'query_loki_patterns', 'query_loki_stats', 'query_metrics', 'query_notebook_cell',
  'query_prometheus', 'query_prometheus_histogram', 'read_documentation', 'read_file',
  'read_graph', 'read_media_file', 'read_multiple_files', 'read_neo4j_cypher', 'read_paper',
  'read_query', 'read_sections', 'read_text_file', 'recommend', 'resources_get', 'resources_list',
  'retrieve_balance', 'run_panel_query', 'slack_get_channel_history', 'slack_get_thread_replies',
  'slack_get_user_profile', 'slack_get_users', 'slack_list_channels', 'slack_read_canvas',
  'slack_read_channel', 'slack_read_thread', 'slack_read_user_profile', 'tavily_crawl',
  'tavily_extract', 'tavily_map', 'tavily_research', 'trace_critical_path', 'user_info',
  'validate_existing_monitor', 'validate_monitor', 'view_attachment', 'whoami',
]);

/** Normalize a raw tool name to snake_case lowercase so camelCase/kebab-case
 *  variants (`searchJiraIssues`, `brave-web-search`) match the allowlist keys.
 *  camelCase boundaries are split BEFORE lowercasing (the reference-parity order). */
export function normalizeCollapseToolName(name: string): string {
  return name
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[-\s]+/g, '_')
    .toLowerCase();
}

/**
 * Classify an MCP tool for transcript collapse. `serverName` is accepted for
 * the reference signature parity (matching is tool-name-keyed, as the reference documents).
 */
export function classifyMcpToolForCollapse(
  _serverName: string,
  toolName: string,
): McpCollapseClass {
  const name = normalizeCollapseToolName(toolName);
  const isSearch =
    SEARCH_TOOLS.has(name) ||
    MCP_SEARCH_PREFIXES.some((p) => name.startsWith(p)) ||
    /(^|_)search($|_)/.test(name);
  const isRead =
    !isSearch &&
    (READ_TOOLS.has(name) ||
      MCP_READ_PREFIXES.some((p) => name.startsWith(p)) ||
      /(^|_)(get|read|list|fetch)($|_)/.test(name));
  return { isSearch, isRead };
}
