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
  },
  isReadOnly: true,
  async execute(_input, _context) {
    // Handled specially by the engine — not executed through normal tool pipeline
    return { output: 'AskUserQuestion is handled by the engine directly.', isError: false };
  },
};
