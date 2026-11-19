import { ToolDefinition, ToolResult } from '../types';
import { isEnvTruthy } from '../privacy-level';

/**
 * Opt-in env toggles for the output-gating ("channel") mode and its backing
 * `user_message` tool. OFF by default so the default tool pool is unchanged; the
 * canonical AMI name plus the Claude-config compatibility alias are both honored.
 */
export const OUTPUT_GATE_ENV = 'AMI_OUTPUT_GATE';
export const OUTPUT_GATE_ENV_COMPAT = 'CLAUDE_CODE_BRIEF_ONLY';

/**
 * True when output-gating mode is OPT-IN enabled for this process (env flag). This
 * gates BOTH the registration of the {@link userMessageTool} and the availability of
 * the `/output-gate` slash command — mirroring Claude Code's `brief` command
 * `isEnabled()` entitlement check (commands/brief.ts) and its two-way
 * `setUserMsgOptIn` tool-availability sync.
 *
 * DISTINCT from AMI's existing `/brief` (which toggles the terse `concise` output
 * STYLE — a presentation overlay) and `/output-style` (presentation): this gate
 * governs the output CHANNEL, not terseness. When the mode is active the model's
 * plain prose is hidden from the user and all user-facing output must flow through
 * the {@link userMessageTool} — a capability AMI otherwise lacks.
 */
export function isOutputGateEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[OUTPUT_GATE_ENV]) || isEnvTruthy(env[OUTPUT_GATE_ENV_COMPAT]);
}

/** Hard cap on a single user-facing message so a runaway emission cannot bloat the
 *  transcript / terminal. Over-length content is truncated with an explicit marker. */
export const USER_MESSAGE_MAX_CHARS = 100_000;

/**
 * `user_message` — the dedicated user-facing output CHANNEL for output-gating mode
 * (parity: Claude Code's `SendUserMessage` / brief-mode tool). When the engine's
 * output-gating mode is active, the model's plain-text completions are hidden from
 * the user and every user-facing message must be emitted through this tool, so the
 * conversation surface is driven by structured tool emissions rather than free prose.
 *
 * The tool's own job is minimal and pure: validate + normalize the `message` and
 * return it as the result output. The engine surfaces that output to the user
 * through AMI's existing tool-result rendering (no new event type, no frontend
 * restyle), keeping TUI↔CLI parity automatic (both frontends render from the same
 * engine output). Gated OFF by default via {@link isOutputGateEnabled} so it never
 * clutters the default tool pool.
 */
export const userMessageTool: ToolDefinition = {
  name: 'user_message',
  description:
    'Send a message to the user. In output-gating mode this is the ONLY channel the ' +
    'user sees — your plain-text replies are hidden, so emit every user-facing ' +
    'message (questions, results, status) through this tool. Call it once per ' +
    'distinct message you want the user to read.',
  inputSchema: {
    type: 'object',
    properties: {
      message: {
        type: 'string',
        description: 'The exact text to show the user.',
      },
    },
    required: ['message'],
  },
  isReadOnly: true,
  // Two-way sync with the opt-in gate: the tool is registered (and therefore
  // advertisable) only when output-gating mode is enabled for the process. The
  // engine further restricts it to turns where the mode is actually ON.
  isEnabled: () => isOutputGateEnabled(),

  async execute(input: Record<string, unknown>): Promise<ToolResult> {
    const raw = input.message;
    if (typeof raw !== 'string' || raw.trim() === '') {
      return { output: 'Error: `message` is required and must be a non-empty string.', isError: true };
    }
    let message = raw;
    if (message.length > USER_MESSAGE_MAX_CHARS) {
      message = message.slice(0, USER_MESSAGE_MAX_CHARS) + '\n[... message truncated ...]';
    }
    return { output: message, isError: false, metadata: { userFacing: true } };
  },
};
