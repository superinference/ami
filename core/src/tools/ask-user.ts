import { ToolDefinition } from '../types';

export const askUserQuestionTool: ToolDefinition = {
  name: 'AskUserQuestion',
  description:
    'Ask the user a question when you need clarification, need them to choose between options, ' +
    'or need their input before proceeding. Use this instead of guessing. ' +
    'Ask 1-4 questions, each with 2-4 clear options. An "Other"/free-text choice is provided ' +
    'automatically, so you do not need to add one — users can always type a custom answer. ' +
    'Put the option you recommend FIRST and append "(Recommended)" to its label. ' +
    'Keep each question header short (<=12 chars). ' +
    'The optional per-option preview field (and a question\'s previewFormat) is for single-select ' +
    'questions only — use it for side-by-side mockups/code/visual comparisons, NOT for simple ' +
    'questions; HTML previews must be a safe fragment (no <script>/<style>).',
  inputSchema: {
    type: 'object',
    properties: {
      questions: {
        type: 'array',
        description: 'Array of 1-4 questions to ask the user.',
        minItems: 1,
        maxItems: 4,
        items: {
          type: 'object',
          properties: {
            question: {
              type: 'string',
              description:
                'The complete question to ask the user. Should be clear, specific, and end with a ' +
                'question mark. Example: "Which library should we use for date formatting?" If ' +
                'multiSelect is true, phrase it accordingly, e.g. "Which features do you want to enable?".',
            },
            header: {
              type: 'string',
              maxLength: 12,
              description: 'Short label displayed as a chip/tag (max 12 chars, e.g. "Auth method").',
            },
            options: {
              type: 'array',
              description: 'Available answer options (2-4 choices). Put the recommended option first and append "(Recommended)".',
              minItems: 2,
              maxItems: 4,
              items: {
                type: 'object',
                properties: {
                  key: {
                    type: 'string',
                    description: 'Optional stable machine key for this option, distinct from the human label (the recorded answer value).',
                  },
                  label: {
                    type: 'string',
                    description: 'Short display text for this option (1-5 words).',
                  },
                  description: {
                    type: 'string',
                    description: 'Explanation of what this option means or what will happen if chosen. Useful for providing context about trade-offs or implications.',
                  },
                  preview: {
                    type: 'string',
                    description:
                      'Optional preview content rendered when this option is focused. Single-select ' +
                      'only. Use for concrete artifacts the user must compare visually — ASCII/HTML ' +
                      'mockups of UI layouts, code snippets of alternative implementations, diagram ' +
                      'or configuration variations. When any option has a preview the UI switches to ' +
                      'a side-by-side layout (option list on the left, preview on the right). Do NOT ' +
                      'use previews for simple preference questions where the label and description ' +
                      'already suffice.',
                  },
                },
                required: ['label', 'description'],
                additionalProperties: false,
              },
            },
            previewFormat: {
              type: 'string',
              enum: ['markdown', 'html'],
              description: 'Format for option previews (single-select only). HTML must be a safe fragment (no <script>/<style>).',
            },
            multiSelect: {
              type: 'boolean',
              description: 'Allow multiple options to be selected. Use when the user may legitimately pick several (e.g. "which checks to run"). Default false; previews are not supported with multiSelect.',
              default: false,
            },
          },
          required: ['question', 'options'],
          additionalProperties: false,
        },
      },
      question: {
        type: 'string',
        description: 'Single question (shorthand — use "questions" array for multi-question).',
      },
      options: {
        type: 'array',
        description: 'Options for single-question shorthand (2-4 choices; recommended option first).',
        minItems: 2,
        maxItems: 4,
        items: {
          type: 'object',
          properties: {
            key: { type: 'string', description: 'Optional stable machine key distinct from the label.' },
            label: { type: 'string' },
            description: { type: 'string' },
          },
          required: ['label', 'description'],
          additionalProperties: false,
        },
      },
      answers: {
        type: 'object',
        description: 'Pre-filled user answers keyed by question text.',
        additionalProperties: { type: 'string' },
      },
      annotations: {
        type: 'object',
        description: 'Per-question annotations from the user (e.g., notes on preview selections). Keyed by question text.',
        additionalProperties: {
          type: 'object',
          properties: {
            notes: { type: 'string', description: 'Free-text notes the user added to their selection.' },
            preview: { type: 'string', description: 'The preview content of the selected option, if the question used previews.' },
          },
        },
      },
      metadata: {
        type: 'object',
        description: 'Optional metadata for tracking and analytics.',
        properties: {
          source: { type: 'string', description: 'Identifier for the source of this question (e.g., "remember" for /remember command).' },
        },
      },
      allowFreeText: {
        type: 'boolean',
        description: 'Whether the user can type a custom answer instead of choosing an option. Defaults to true.',
      },
    },
    required: [],
    // Strict input object: reject unknown top-level keys (parity with Claude's
    // z.strictObject). `ToolInputSchema.additionalProperties` now declares this.
    additionalProperties: false,
  },
  isReadOnly: true,
  // Cap the (user/model-derived) answer+annotation text that flows back into the
  // model context (parity with Claude's `maxResultSizeChars: 100_000`). The tool is
  // handled specially by the engine, which reads this field to bound the result.
  maxResultSizeChars: 100_000,
  async execute(_input, _context) {
    // Handled specially by the engine — not executed through normal tool pipeline
    return { output: 'AskUserQuestion is handled by the engine directly.', isError: false };
  },
};

/** Hard cap on the serialized AskUserQuestion result (parity with Claude's
 *  `maxResultSizeChars: 100_000`). The engine reads {@link askUserQuestionTool}'s
 *  `maxResultSizeChars` so this constant is the single source of the default. */
export const ASK_USER_MAX_RESULT_CHARS = askUserQuestionTool.maxResultSizeChars ?? 100_000;

/**
 * Bound an AskUserQuestion tool result to `cap` characters before it flows back
 * into the model context. Oversized answer/annotation payloads are truncated with
 * an explicit marker rather than returned unbounded (parity with Claude's
 * `maxResultSizeChars`). Returns the input unchanged when within the cap.
 */
export function capAskUserResult(output: string, cap: number = ASK_USER_MAX_RESULT_CHARS): string {
  if (cap <= 0 || output.length <= cap) return output;
  const marker = `\n… [truncated ${output.length - cap} chars over the ${cap}-char AskUserQuestion result limit]`;
  // If even the marker alone would overflow the cap, hard-truncate without it so
  // the result is always <= cap.
  if (marker.length >= cap) return output.slice(0, cap);
  return output.slice(0, cap - marker.length) + marker;
}

// ---------------------------------------------------------------------------
// Input validation (parity with Claude's AskUserQuestionTool refine + validateInput)
//
// The schema's minItems/maxItems/additionalProperties are advisory — many
// providers do not enforce JSON-Schema array bounds or refinements, so the
// engine must fail-closed on untrusted, model-generated input before surfacing
// it to the user. These helpers are the canonical validation the engine runs on
// every AskUserQuestion tool call.
// ---------------------------------------------------------------------------

/** Model-facing message when uniqueness is violated (matches Claude's refine). */
export const ASK_USER_UNIQUENESS_MESSAGE =
  'Question texts must be unique, option labels must be unique within each question';

/** Cardinality bounds (parity with Claude's zod `.min(1).max(4)` on questions and `.min(2).max(4)` on options). */
export const ASK_USER_MIN_QUESTIONS = 1;
export const ASK_USER_MAX_QUESTIONS = 4;
export const ASK_USER_MIN_OPTIONS = 2;
export const ASK_USER_MAX_OPTIONS = 4;

/**
 * Max header/chip width (parity with Claude's `ASK_USER_QUESTION_TOOL_CHIP_WIDTH = 12`).
 * The schema advertises `maxLength: 12`, but JSON-Schema `maxLength` is advisory —
 * providers do not reliably enforce it, so the validator fail-closes here too.
 */
export const ASK_USER_MAX_HEADER = 12;

/**
 * Model-facing message when a preview is supplied on a multi-select question.
 * Previews are a single-select-only affordance (side-by-side focused preview),
 * so a preview on a multiSelect question is rejected (parity with Claude's
 * prompt constraint "previews are only supported for single-select questions").
 */
export const ASK_USER_PREVIEW_MULTISELECT_MESSAGE =
  'Previews are only supported for single-select questions (not multiSelect).';

interface AskUserOptionLike {
  label?: string;
  description?: string;
  preview?: string;
}

interface AskUserQuestionLike {
  question?: string;
  header?: string;
  options?: AskUserOptionLike[];
  previewFormat?: string;
  multiSelect?: boolean;
}

interface AskUserQuestionInputLike {
  questions?: AskUserQuestionLike[];
  question?: string;
  header?: string;
  options?: AskUserOptionLike[];
  previewFormat?: string;
  multiSelect?: boolean;
}

/**
 * Validate an option's HTML preview fragment (only meaningful when the
 * question's previewFormat is 'html'). Returns a model-facing error string, or
 * null when the preview is safe/absent. Ported 1:1 from Claude's
 * validateHtmlPreview: a preview must be a fragment (no full-document tags), must
 * not carry executable/style tags, and must actually contain a tag.
 */
export function validateHtmlPreview(preview: string | undefined): string | null {
  if (preview === undefined) return null;
  if (/<\s*(html|body|!doctype)\b/i.test(preview)) {
    return 'preview must be an HTML fragment, not a full document (no <html>, <body>, or <!DOCTYPE>)';
  }
  if (/<\s*(script|style)\b/i.test(preview)) {
    return 'preview must not contain <script> or <style> tags. Use inline styles via the style attribute if needed.';
  }
  if (!/<[a-z][^>]*>/i.test(preview)) {
    return 'preview must contain HTML (previewFormat is set to "html"). Wrap content in a tag like <div> or <pre>.';
  }
  return null;
}

/**
 * Validate a parsed AskUserQuestion tool input. Returns a model-facing error
 * string when the input is invalid, or null when it is acceptable. Enforces:
 *   - cardinality bounds: 1-4 questions, each with 2-4 options (parity with
 *     Claude's zod `.min/.max`; providers do not reliably enforce JSON-Schema
 *     `minItems`/`maxItems`, so the engine must fail-closed here),
 *   - header/chip width (<= 12 chars, parity with Claude's chip width),
 *   - unique question texts across the call,
 *   - unique option labels within each question,
 *   - previews only on single-select questions (not multiSelect),
 *   - HTML-fragment safety for previews when previewFormat === 'html'.
 * Accepts both the multi-question (`questions[]`) and single-question shorthand
 * (`question`/`options`) shapes the engine supports. An absent `options` array
 * falls back to the engine's default choices, so cardinality is only enforced
 * when the model actually supplies an options array.
 */
export function validateAskUserQuestionInput(input: AskUserQuestionInputLike): string | null {
  const qList: AskUserQuestionLike[] =
    input.questions && input.questions.length > 0
      ? input.questions
      : [{
          question: input.question,
          header: input.header,
          options: input.options,
          previewFormat: input.previewFormat,
          multiSelect: input.multiSelect,
        }];

  // Cardinality: too many questions. (min 1 is always satisfied — the shorthand
  // fallback yields a single question — so only the upper bound can be violated.)
  if (qList.length > ASK_USER_MAX_QUESTIONS) {
    return `Provide ${ASK_USER_MIN_QUESTIONS}-${ASK_USER_MAX_QUESTIONS} questions (got ${qList.length}).`;
  }

  for (const q of qList) {
    if (
      Array.isArray(q.options) &&
      (q.options.length < ASK_USER_MIN_OPTIONS || q.options.length > ASK_USER_MAX_OPTIONS)
    ) {
      return `Question "${q.question ?? ''}" must have ${ASK_USER_MIN_OPTIONS}-${ASK_USER_MAX_OPTIONS} options (got ${q.options.length}).`;
    }

    // Header/chip width (parity with Claude's ASK_USER_QUESTION_TOOL_CHIP_WIDTH).
    if (typeof q.header === 'string' && q.header.length > ASK_USER_MAX_HEADER) {
      return `Question "${q.question ?? ''}" header must be <= ${ASK_USER_MAX_HEADER} chars (got ${q.header.length}).`;
    }
  }

  const texts = qList
    .map(q => q.question)
    .filter((t): t is string => typeof t === 'string');
  if (texts.length !== new Set(texts).size) {
    return ASK_USER_UNIQUENESS_MESSAGE;
  }

  for (const q of qList) {
    const labels = (q.options ?? [])
      .map(o => o.label)
      .filter((l): l is string => typeof l === 'string');
    if (labels.length !== new Set(labels).size) {
      return ASK_USER_UNIQUENESS_MESSAGE;
    }

    // Previews are a single-select-only affordance: reject a preview (or an
    // html/markdown previewFormat) on a multiSelect question.
    if (q.multiSelect === true && (q.previewFormat !== undefined || (q.options ?? []).some(o => o.preview !== undefined))) {
      return ASK_USER_PREVIEW_MULTISELECT_MESSAGE;
    }

    if (q.previewFormat === 'html') {
      for (const opt of q.options ?? []) {
        const err = validateHtmlPreview(opt.preview);
        if (err) {
          return `Option "${opt.label}" in question "${q.question}": ${err}`;
        }
      }
    }
  }

  return null;
}
