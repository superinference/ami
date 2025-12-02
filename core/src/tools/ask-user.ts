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
      question: {
        type: 'string',
        description: 'The question to ask the user. Should be clear and specific.',
      },
      options: {
        type: 'array',
        description: 'Available answer options (2-4 choices). Each has a label and description.',
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
          },
          required: ['label', 'description'],
        },
      },
      allowFreeText: {
        type: 'boolean',
        description: 'Whether the user can type a custom answer instead of choosing an option. Defaults to true.',
      },
    },
    required: ['question', 'options'],
  },
  isReadOnly: true,
  async execute(_input, _context) {
    // Handled specially by the engine — not executed through normal tool pipeline
    return { output: 'AskUserQuestion is handled by the engine directly.', isError: false };
  },
};
