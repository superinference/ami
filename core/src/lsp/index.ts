export { LSPClient, getLSPClient, peekLSPClient } from './client';
export type {
  NotificationHandler,
  RequestHandler,
  LspServerState,
  LspServerStatus,
  LspInitializationStatus,
} from './client';
export { LspHealthMonitor, collectLspHealthErrors, LSP_POLL_INTERVAL_MS } from './health-monitor';
export type { LspHealthError, LspHealthMonitorOptions, HealthScheduler } from './health-monitor';
export {
  DEFAULT_LSP_SERVERS,
  getAllLspServers,
  registerLspServers,
  resetLspServers,
  resolveLanguageForExtension,
  languageIdForExtension,
  assertNoAutoFetch,
  validateLspServerConfig,
  loadLspServersFromFile,
} from './config';
export type { LspServerConfig } from './config';
export {
  DiagnosticTracker,
  mapLspSeverity,
  convertPublishDiagnostics,
  areDiagnosticsEqual,
  dedupeDiagnostics,
  normalizeDiagnosticPath,
  diagnosticPathsEqual,
  uriToPath,
  formatLspDisplayPath,
  diagnosticSeveritySymbol,
  formatNewDiagnosticsSummary,
  formatDiagnosticsDetail,
  capDiagnosticVolume,
  capDiagnosticVolumeReport,
  isUncPath,
  lspFileSizeBytes,
  DIAGNOSTIC_SEVERITY_NAMES,
  MAX_LSP_FILE_SIZE_BYTES,
  MAX_DIAGNOSTICS_PER_FILE,
  MAX_TOTAL_DIAGNOSTICS,
  MAX_DIAGNOSTICS_SUMMARY_CHARS,
  MAX_TRACKED_BASELINE_FILES,
} from './diagnostic-tracking';
export type {
  Diagnostic,
  DiagnosticFile,
  DiagnosticSource,
  DiagnosticSeverityName,
  DiagnosticRange,
  DiagnosticPosition,
  PublishDiagnosticsParams,
} from './diagnostic-tracking';
