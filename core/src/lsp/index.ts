export { LSPClient, getLSPClient } from './client';
export type { NotificationHandler, RequestHandler } from './client';
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
  normalizeDiagnosticPath,
  diagnosticPathsEqual,
  uriToPath,
  diagnosticSeveritySymbol,
  formatNewDiagnosticsSummary,
  formatDiagnosticsDetail,
  capDiagnosticVolume,
  isUncPath,
  lspFileSizeBytes,
  DIAGNOSTIC_SEVERITY_NAMES,
  MAX_LSP_FILE_SIZE_BYTES,
  MAX_DIAGNOSTICS_PER_FILE,
  MAX_TOTAL_DIAGNOSTICS,
  MAX_DIAGNOSTICS_SUMMARY_CHARS,
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
