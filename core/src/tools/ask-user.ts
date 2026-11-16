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
              description: 'The question to ask the user. Should be clear and specific.',
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
                    description: 'Explanation of what this option means or what will happen.',
                  },
                  preview: {
                    type: 'string',
                    description: 'Optional preview content rendered when this option is focused. Single-select only. Use for mockups, code snippets, or visual comparisons.',
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
    // z.strictObject). `ToolInputSchema` does not yet declare this field, so the
    // runtime schema carries it while the rest of the literal stays type-checked.
    // @ts-expect-error -- additionalProperties is a valid JSON Schema keyword here.
    additionalProperties: false,
  },
  isReadOnly: true,
  async execute(_input, _context) {
    // Handled specially by the engine — not executed through normal tool pipeline
    return { output: 'AskUserQuestion is handled by the engine directly.', isError: false };
  },
};

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

interface AskUserOptionLike {
  label?: string;
  description?: string;
  preview?: string;
}

interface AskUserQuestionLike {
  question?: string;
  options?: AskUserOptionLike[];
  previewFormat?: string;
}

interface AskUserQuestionInputLike {
  questions?: AskUserQuestionLike[];
  question?: string;
  options?: AskUserOptionLike[];
  previewFormat?: string;
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
 *   - unique question texts across the call,
 *   - unique option labels within each question,
 *   - HTML-fragment safety for previews when previewFormat === 'html'.
 * Accepts both the multi-question (`questions[]`) and single-question shorthand
 * (`question`/`options`) shapes the engine supports.
 */
export function validateAskUserQuestionInput(input: AskUserQuestionInputLike): string | null {
  const qList: AskUserQuestionLike[] =
    input.questions && input.questions.length > 0
      ? input.questions
      : [{ question: input.question, options: input.options, previewFormat: input.previewFormat }];

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
