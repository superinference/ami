import * as fs from 'fs';
import * as path from 'path';

export interface AnalyticsEvent {
  timestamp: string;
  type: string;
  data: Record<string, unknown>;
}

const MAX_IN_MEMORY_EVENTS = 1000;

export class AnalyticsTracker {
  private events: AnalyticsEvent[] = [];
  private logFile: string | null;
  private dirInitialized = false;

  constructor(logDir?: string) {
    this.logFile = logDir ? `${logDir}/analytics.jsonl` : null;
  }

  log(type: string, data: Record<string, unknown> = {}): void {
    const event: AnalyticsEvent = {
      timestamp: new Date().toISOString(),
      type,
      data,
    };
    this.events.push(event);
    if (this.events.length > MAX_IN_MEMORY_EVENTS) {
      this.events = this.events.slice(-MAX_IN_MEMORY_EVENTS);
    }

    if (this.logFile) {
      try {
        if (!this.dirInitialized) {
          fs.mkdirSync(path.dirname(this.logFile), { recursive: true });
          this.dirInitialized = true;
        }
        fs.appendFileSync(this.logFile, JSON.stringify(event) + '\n');
      } catch {
        /* best effort */
      }
    }
  }

  getEvents(): AnalyticsEvent[] {
    return [...this.events];
  }

  getEventsByType(type: string): AnalyticsEvent[] {
    return this.events.filter(e => e.type === type);
  }

  getSummary(): Record<string, number> {
    const counts: Record<string, number> = {};
    for (const e of this.events) {
      counts[e.type] = (counts[e.type] || 0) + 1;
    }
    return counts;
  }

  reset(): void {
    this.events = [];
  }
}

/**
 * Generate a compact summary of tool results for a turn.
 *
 * Each tool result is condensed to one line: the tool name, OK/ERROR status,
 * and the first line of output (up to 100 chars).
 */
export function generateToolSummary(
  toolResults: Array<{ toolName: string; output: string; isError: boolean }>,
): string {
  const summaries = toolResults.map(r => {
    const shortOutput = r.output.split('\n')[0].substring(0, 100);
    return `${r.toolName}: ${r.isError ? 'ERROR' : 'OK'} — ${shortOutput}`;
  });
  return summaries.join('\n');
}
