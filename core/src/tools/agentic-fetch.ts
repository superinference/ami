import { ToolDefinition, ToolResult, ToolContext, ExitReason } from '../types';

/** Hard cap on pages the research subagent may fetch (defense-in-depth bound). */
const MAX_PAGES_LIMIT = 10;
/** Wall-clock budget for the whole research sub-engine run before it is aborted. */
const RESEARCH_TIMEOUT_MS = 120_000;
/** ExitReasons that mean the subengine stopped early without a real answer. */
const EXHAUSTION_REASONS: ReadonlySet<ExitReason> = new Set<ExitReason>([
  'max_turns',
  'max_steps',
  'budget_exceeded',
]);

export const agenticFetchTool: ToolDefinition = {
  name: 'agentic_fetch',
  description: 'Delegate web research to a subagent. The subagent searches the web and fetches relevant pages to answer your question, keeping the results out of your main context.',
  inputSchema: {
    type: 'object',
    properties: {
      query: { type: 'string', minLength: 2, description: 'The research question or topic.' },
      max_pages: { type: 'number', minimum: 1, maximum: MAX_PAGES_LIMIT, description: `Maximum pages to fetch (default 3, max ${MAX_PAGES_LIMIT}).` },
    },
    required: ['query'],
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    // --- Input validation / bounds (reject before spending any sub-engine turn) ---
    const query = typeof input.query === 'string' ? input.query.trim() : '';
    if (query.length < 2) {
      return { output: 'Error: `query` is required and must be at least 2 characters.', isError: true };
    }

    let maxPages = 3;
    if (input.max_pages !== undefined && input.max_pages !== null) {
      const n = Number(input.max_pages);
      if (!Number.isFinite(n)) {
        return { output: 'Error: `max_pages` must be a finite number.', isError: true };
      }
      maxPages = Math.min(Math.max(1, Math.floor(n)), MAX_PAGES_LIMIT);
    }

    if (!context._engineFactory) {
      return { output: 'Error: Engine factory not available for agentic fetch.', isError: true };
    }
    if (!context._providerConfig) {
      return { output: 'Error: Provider configuration not available for agentic fetch.', isError: true };
    }

    // --- Abort + wall-clock timeout plumbing ---
    // A local controller lets us (a) forward the caller's abort (ESC / interrupt)
    // into the sub-engine and (b) enforce a wall-clock budget so a hung research
    // run cannot spin forever spending turns and tokens.
    const controller = new AbortController();
    const onParentAbort = () => controller.abort();
    if (context.abortSignal.aborted) {
      controller.abort();
    } else {
      context.abortSignal.addEventListener('abort', onParentAbort, { once: true });
    }
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, RESEARCH_TIMEOUT_MS);
    timer.unref?.();

    const subEngine = context._engineFactory({
      provider: context._providerConfig,
      cwd: context.cwd,
      permissionMode: 'auto-allow',
      // Per-domain permission boundary: the research subagent runs autonomously
      // (auto-allow) but STILL inherits the parent's permission rules + prompt
      // handler, so operator deny rules (e.g. a web_fetch/web_search domain
      // denylist) keep applying to the subagent's fetches — in auto-allow mode an
      // explicit deny still fires (permissions.ts). Mirrors task.ts's subagent
      // threading; no parallel permission path.
      permissionRules: context._parentPermissionRules,
      permissionPromptHandler: context._permissionPromptHandler,
      maxTurns: 5,
      tokenBudget: 16000,
      abortController: controller,
    });

    const currentDate = new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long' });
    const prompt = [
      `Search the web for: "${query}"`,
      '',
      `Today is ${currentDate}. For any recency-sensitive query, prefer the current year and the most up-to-date sources.`,
      '',
      `Use web_search to find relevant pages, then web_fetch up to ${maxPages} of the most relevant results. If a fetched URL redirects to a different host, re-fetch the final redirect URL. For GitHub URLs, prefer the 'gh' CLI (via bash) over web_fetch when it gives cleaner results.`,
      '',
      'Synthesize the key findings into a concise summary. End your answer with a "Sources:" section listing every page you fetched as a markdown hyperlink ([title](url)), one per line.',
    ].join('\n');

    let result = '';
    let errorMessage = '';
    let exitReason: ExitReason | undefined;
    let caught: unknown;

    try {
      for await (const event of subEngine.submit(prompt)) {
        if (controller.signal.aborted) break;
        if (event.type === 'text_delta') result += event.text;
        else if (event.type === 'error') errorMessage = event.error;
        else if (event.type === 'done') exitReason = event.exitReason;
      }
    } catch (err) {
      caught = err;
    } finally {
      clearTimeout(timer);
      context.abortSignal.removeEventListener('abort', onParentAbort);
      subEngine.shutdown?.();
    }

    // --- Structured result discrimination ---
    if (timedOut) {
      return {
        output: result
          ? `${result}\n\n[Web research timed out after ${RESEARCH_TIMEOUT_MS / 1000}s; partial results above.]`
          : `[Web research for "${query}" timed out after ${RESEARCH_TIMEOUT_MS / 1000}s with no results.]`,
        isError: true,
      };
    }
    if (context.abortSignal.aborted) {
      return {
        output: result
          ? `${result}\n\n[Web research aborted before completion; partial results above.]`
          : `[Web research for "${query}" was aborted.]`,
        isError: true,
      };
    }
    if (caught) {
      const msg = caught instanceof Error ? caught.message : String(caught);
      return { output: `Error during web research: ${msg}`, isError: true };
    }
    if (errorMessage && !result) {
      return { output: `Web research failed: ${errorMessage}`, isError: true };
    }
    if (!result) {
      if (exitReason && EXHAUSTION_REASONS.has(exitReason)) {
        return { output: `[Web research for "${query}" ended early (${exitReason}) before producing a summary; try a narrower query.]` };
      }
      return { output: `[No results found for: ${query}]` };
    }
    return { output: result };
  },
};
