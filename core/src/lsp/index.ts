export { LSPClient, getLSPClient, peekLSPClient } from './client';
export type {
  NotificationHandler,
  RequestHandler,
  LspServerState,
  LspServerStatus,
  LspInitializationStatus,
  HandlerRegistrationResult,
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
export { getSymbolAtPosition, MAX_READ_BYTES, MAX_SYMBOL_LENGTH } from './symbol-context';
export {
  LSPDiagnosticRegistry,
  getLSPDiagnosticRegistry,
  resetLSPDiagnosticRegistry,
  MAX_DELIVERED_FILES,
} from './diagnostic-registry';
export type {
  PendingLSPDiagnostic,
  RegisterPendingInput,
  DeliveredLSPDiagnostics,
} from './diagnostic-registry';
export {
  DiagnosticTracker,
  mapLspSeverity,
  convertPublishDiagnostics,
  areDiagnosticsEqual,
  createDiagnosticKey,
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
