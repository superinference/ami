import { ToolDefinition } from '../types';

export const askUserQuestionTool: ToolDefinition = {
  name: 'AskUserQuestion',
  description:
    'Ask the user a question when you need clarification, need them to choose between options, ' +
    'or need their input before proceeding. Use this instead of guessing. ' +
    'Present 2-4 clear options per question. Users can also type a custom answer.',
  inputSchema: {
    type: 'object',
    properties: {
      questions: {
        type: 'array',
        description: 'Array of 1-4 questions to ask the user.',
        items: {
          type: 'object',
          properties: {
            question: {
              type: 'string',
              description: 'The question to ask the user. Should be clear and specific.',
            },
            header: {
              type: 'string',
              description: 'Short label displayed as a chip/tag (max 12 chars, e.g. "Auth method").',
            },
            options: {
              type: 'array',
              description: 'Available answer options (2-4 choices).',
              items: {
                type: 'object',
                properties: {
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
                    description: 'Optional preview content rendered when this option is focused. Use for mockups, code snippets, or visual comparisons. Rendered as markdown in a monospace box.',
                  },
                },
                required: ['label', 'description'],
              },
            },
            multiSelect: {
              type: 'boolean',
              description: 'Allow multiple options to be selected. Default false.',
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
        description: 'Options for single-question shorthand.',
        items: {
          type: 'object',
          properties: {
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
