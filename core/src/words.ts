/**
 * Memorable, collision-resistant word-slug generator for human-friendly ids
 * (plan files, session labels, etc). Uses cryptographically strong randomness
 * (crypto.randomBytes) rather than Math.random for better distribution.
 *
 * Two formats:
 *   generateWordSlug()      -> "adjective-verb-noun"  e.g. "gleaming-brewing-phoenix"
 *   generateShortWordSlug() -> "adjective-noun"       e.g. "graceful-unicorn"
 *
 * Corpora are curated for readability (no offensive terms) and sized for low
 * collision probability (adjectives x verbs x nouns ~= 9.7M combinations).
 */
import { randomBytes } from 'crypto';

const ADJECTIVES: readonly string[] = [
  'abundant', 'ancient', 'bright', 'calm', 'cheerful', 'clever', 'cozy', 'curious', 'dapper',
  'dazzling', 'deep', 'delightful', 'eager', 'elegant', 'enchanted', 'fancy', 'fluffy',
  'gentle', 'gleaming', 'golden', 'graceful', 'happy', 'hidden', 'humble', 'jolly', 'joyful',
  'keen', 'kind', 'lively', 'lovely', 'lucky', 'luminous', 'magical', 'majestic', 'mellow',
  'merry', 'mighty', 'misty', 'noble', 'peaceful', 'playful', 'polished', 'precious', 'proud',
  'quiet', 'quirky', 'radiant', 'rosy', 'serene', 'shiny', 'silly', 'sleepy', 'smooth',
  'snazzy', 'snug', 'snuggly', 'soft', 'sparkling', 'spicy', 'splendid', 'sprightly', 'starry',
  'steady', 'sunny', 'swift', 'tender', 'tidy', 'toasty', 'tranquil', 'twinkly', 'valiant',
  'vast', 'velvet', 'vivid', 'warm', 'whimsical', 'wild', 'wise', 'witty', 'wondrous', 'zany',
  'zesty', 'zippy', 'breezy', 'bubbly', 'buzzing', 'cheeky', 'cosmic', 'cozy', 'crispy',
  'crystalline', 'cuddly', 'drifting', 'dreamy', 'effervescent', 'ethereal', 'fizzy',
  'flickering', 'floating', 'floofy', 'fluttering', 'foamy', 'frolicking', 'fuzzy', 'giggly',
  'glimmering', 'glistening', 'glittery', 'glowing', 'goofy', 'groovy', 'harmonic', 'hazy',
  'humming', 'iridescent', 'jaunty', 'jazzy', 'jiggly', 'melodic', 'moonlit', 'mossy', 'nifty',
  'peppy', 'prancy', 'purrfect', 'purring', 'quizzical', 'rippling', 'rustling', 'shimmering',
  'shimmying', 'snappy', 'snoopy', 'squishy', 'swirling', 'ticklish', 'tingly', 'twinkling',
  'velvety', 'wiggly', 'wobbly', 'woolly', 'zazzy', 'abstract', 'adaptive', 'agile', 'async',
  'atomic', 'binary', 'cached', 'compiled', 'composed', 'compressed', 'concurrent', 'cryptic',
  'curried', 'declarative', 'delegated', 'distributed', 'dynamic', 'eager', 'elegant',
  'encapsulated', 'enumerated', 'eventual', 'expressive', 'federated', 'functional', 'generic',
  'greedy', 'hashed', 'idempotent', 'immutable', 'imperative', 'indexed', 'inherited',
  'iterative', 'lazy', 'lexical', 'linear', 'linked', 'logical', 'memoized', 'modular',
  'mutable', 'nested', 'optimized', 'parallel', 'parsed', 'partitioned', 'piped',
  'polymorphic', 'pure', 'reactive', 'recursive', 'refactored', 'reflective', 'replicated',
  'resilient', 'robust', 'scalable', 'sequential', 'serialized', 'sharded', 'sorted', 'staged',
  'stateful', 'stateless', 'streamed', 'structured', 'synchronous', 'synthetic', 'temporal',
  'transient', 'typed', 'unified', 'validated', 'vectorized', 'virtual'
];

const NOUNS: readonly string[] = [
  'aurora', 'avalanche', 'blossom', 'breeze', 'brook', 'bubble', 'canyon', 'cascade', 'cloud',
  'clover', 'comet', 'coral', 'cosmos', 'creek', 'crescent', 'crystal', 'dawn', 'dewdrop',
  'dusk', 'eclipse', 'ember', 'feather', 'fern', 'firefly', 'flame', 'flurry', 'fog', 'forest',
  'frost', 'galaxy', 'garden', 'glacier', 'glade', 'grove', 'harbor', 'horizon', 'island',
  'lagoon', 'lake', 'leaf', 'lightning', 'meadow', 'meteor', 'mist', 'moon', 'moonbeam',
  'mountain', 'nebula', 'nova', 'ocean', 'orbit', 'pebble', 'petal', 'pine', 'planet', 'pond',
  'puddle', 'quasar', 'rain', 'rainbow', 'reef', 'ripple', 'river', 'shore', 'sky',
  'snowflake', 'spark', 'spring', 'star', 'stardust', 'starlight', 'storm', 'stream', 'summit',
  'sun', 'sunbeam', 'sunrise', 'sunset', 'thunder', 'tide', 'twilight', 'valley', 'volcano',
  'waterfall', 'wave', 'willow', 'wind', 'alpaca', 'axolotl', 'badger', 'bear', 'beaver',
  'bee', 'bird', 'bumblebee', 'bunny', 'cat', 'chipmunk', 'crab', 'crane', 'deer', 'dolphin',
  'dove', 'dragon', 'dragonfly', 'duckling', 'eagle', 'elephant', 'falcon', 'finch',
  'flamingo', 'fox', 'frog', 'giraffe', 'goose', 'hamster', 'hare', 'hedgehog', 'hippo',
  'hummingbird', 'jellyfish', 'kitten', 'koala', 'ladybug', 'lark', 'lemur', 'llama',
  'lobster', 'lynx', 'manatee', 'meerkat', 'moth', 'narwhal', 'newt', 'octopus', 'otter',
  'owl', 'panda', 'parrot', 'peacock', 'pelican', 'penguin', 'phoenix', 'piglet', 'platypus',
  'pony', 'porcupine', 'puffin', 'puppy', 'quail', 'quokka', 'rabbit', 'raccoon', 'raven',
  'robin', 'salamander', 'seahorse', 'seal', 'sloth', 'snail', 'sparrow', 'sphinx', 'squid',
  'squirrel', 'starfish', 'swan', 'tiger', 'toucan', 'turtle', 'unicorn', 'walrus', 'whale',
  'wolf', 'wombat', 'wren', 'yeti', 'zebra', 'acorn', 'anchor', 'balloon', 'beacon', 'biscuit',
  'blanket', 'bonbon', 'book', 'boot', 'cake', 'candle', 'candy', 'castle', 'charm', 'clock',
  'cocoa', 'cookie', 'crayon', 'crown', 'cupcake', 'donut', 'dream', 'fairy', 'fiddle',
  'flask', 'flute', 'fountain', 'gadget', 'gem', 'gizmo', 'globe', 'goblet', 'hammock', 'harp',
  'haven', 'hearth', 'honey', 'journal', 'kazoo', 'kettle', 'key', 'kite', 'lantern', 'lemon',
  'lighthouse', 'locket', 'lollipop', 'mango', 'map', 'marble', 'marshmallow', 'melody',
  'mitten', 'mochi', 'muffin', 'music', 'nest', 'noodle', 'oasis', 'origami', 'pancake',
  'parasol', 'peach', 'pearl', 'pebble', 'pie', 'pillow', 'pinwheel', 'pixel', 'pizza', 'plum',
  'popcorn', 'pretzel', 'prism', 'pudding', 'pumpkin', 'puzzle', 'quiche', 'quill', 'quilt',
  'riddle', 'rocket', 'rose', 'scone', 'scroll', 'shell', 'sketch', 'snowglobe', 'sonnet',
  'sparkle', 'spindle', 'sprout', 'sundae', 'swing', 'taco', 'teacup', 'teapot', 'thimble',
  'toast', 'token', 'tome', 'tower', 'treasure', 'treehouse', 'trinket', 'truffle', 'tulip',
  'umbrella', 'waffle', 'wand', 'whisper', 'whistle', 'widget', 'wreath', 'zephyr', 'abelson',
  'adleman', 'aho', 'allen', 'babbage', 'bachman', 'backus', 'barto', 'bengio', 'bentley',
  'blum', 'boole', 'brooks', 'catmull', 'cerf', 'cherny', 'church', 'clarke', 'cocke', 'codd',
  'conway', 'cook', 'corbato', 'cray', 'curry', 'dahl', 'diffie', 'dijkstra', 'dongarra',
  'eich', 'emerson', 'engelbart', 'feigenbaum', 'floyd', 'gosling', 'graham', 'gray',
  'hamming', 'hanrahan', 'hartmanis', 'hejlsberg', 'hellman', 'hennessy', 'hickey', 'hinton',
  'hoare', 'hollerith', 'hopcroft', 'hopper', 'iverson', 'kahan', 'kahn', 'karp', 'kay',
  'kernighan', 'knuth', 'kurzweil', 'lamport', 'lampson', 'lecun', 'lerdorf', 'liskov',
  'lovelace', 'matsumoto', 'mccarthy', 'metcalfe', 'micali', 'milner', 'minsky', 'moler',
  'moore', 'naur', 'neumann', 'newell', 'nygaard', 'papert', 'parnas', 'pascal', 'patterson',
  'pearl', 'perlis', 'pike', 'pnueli', 'rabin', 'reddy', 'ritchie', 'rivest', 'rossum',
  'russell', 'scott', 'sedgewick', 'shamir', 'shannon', 'sifakis', 'simon', 'stallman',
  'stearns', 'steele', 'stonebraker', 'stroustrup', 'sutherland', 'sutton', 'tarjan',
  'thacker', 'thompson', 'torvalds', 'turing', 'ullman', 'valiant', 'wadler', 'wall',
  'wigderson', 'wilkes', 'wilkinson', 'wirth', 'wozniak', 'yao'
];

const VERBS: readonly string[] = [
  'baking', 'beaming', 'booping', 'bouncing', 'brewing', 'bubbling', 'chasing', 'churning',
  'coalescing', 'conjuring', 'cooking', 'crafting', 'crunching', 'cuddling', 'dancing',
  'dazzling', 'discovering', 'doodling', 'dreaming', 'drifting', 'enchanting', 'exploring',
  'finding', 'floating', 'fluttering', 'foraging', 'forging', 'frolicking', 'gathering',
  'giggling', 'gliding', 'greeting', 'growing', 'hatching', 'herding', 'honking', 'hopping',
  'hugging', 'humming', 'imagining', 'inventing', 'jingling', 'juggling', 'jumping',
  'kindling', 'knitting', 'launching', 'leaping', 'mapping', 'marinating', 'meandering',
  'mixing', 'moseying', 'munching', 'napping', 'nibbling', 'noodling', 'orbiting', 'painting',
  'percolating', 'petting', 'plotting', 'pondering', 'popping', 'prancing', 'purring',
  'puzzling', 'questing', 'riding', 'roaming', 'rolling', 'sauteeing', 'scribbling', 'seeking',
  'shimmying', 'singing', 'skipping', 'sleeping', 'snacking', 'sniffing', 'snuggling',
  'soaring', 'sparking', 'spinning', 'splashing', 'sprouting', 'squishing', 'stargazing',
  'stirring', 'strolling', 'swimming', 'swinging', 'tickling', 'tinkering', 'toasting',
  'tumbling', 'twirling', 'waddling', 'wandering', 'watching', 'weaving', 'whistling',
  'wibbling', 'wiggling', 'wishing', 'wobbling', 'wondering', 'yawning', 'zooming'
];

/**
 * Cryptographically strong, UNBIASED integer in `[0, max)`.
 *
 * Draws 32 bits from `crypto.randomBytes` and uses rejection sampling: values
 * in the final, partial bucket (`>= floor(2^32 / max) * max`) are discarded and
 * re-drawn so a plain `value % max` can't skew the distribution toward the low
 * end when `max` is not a power of two. Exported as the canonical random-index
 * selection helper so other modules (e.g. skillbook entry-id generation) pick
 * elements/characters securely instead of reaching for `Math.random`.
 */
export function randomInt(max: number): number {
  if (!Number.isInteger(max) || max <= 0) {
    throw new RangeError('randomInt: max must be a positive integer');
  }
  // Largest multiple of `max` that fits in a uint32; anything at/above `limit`
  // lives in the biased partial bucket and is rejected.
  const limit = Math.floor(0x1_0000_0000 / max) * max;
  let value: number;
  do {
    value = randomBytes(4).readUInt32BE(0);
  } while (value >= limit);
  return value % max;
}

/** Pick a random element from a non-empty array using strong, unbiased randomness. */
export function pickRandom<T>(array: readonly T[]): T {
  return array[randomInt(array.length)]!;
}

/**
 * Curated vocabulary of common English words each API-verified to tokenize as
 * EXACTLY one token under the usual BPE vocabularies. Grouped by part of speech
 * (articles/pronouns, verbs, nouns/adjectives, prepositions/conjunctions,
 * adverbs, and common tech words) purely for readability — at runtime it is a
 * flat pool. This single-token guarantee is what lets {@link generateLoremIpsum}
 * treat "one word == one token", so generated filler can hit a requested token
 * budget accurately rather than merely approximating character counts.
 */
export const ONE_TOKEN_WORDS: readonly string[] = [
  // Articles & pronouns
  'the', 'a', 'an', 'I', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him',
  'her', 'us', 'them', 'my', 'your', 'his', 'its', 'our', 'this', 'that', 'what',
  'who',
  // Common verbs
  'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does',
  'did', 'will', 'would', 'can', 'could', 'may', 'might', 'must', 'shall',
  'should', 'make', 'made', 'get', 'got', 'go', 'went', 'come', 'came', 'see',
  'saw', 'know', 'take', 'think', 'look', 'want', 'use', 'find', 'give', 'tell',
  'work', 'call', 'try', 'ask', 'need', 'feel', 'seem', 'leave', 'put',
  // Common nouns & adjectives
  'time', 'year', 'day', 'way', 'man', 'thing', 'life', 'hand', 'part', 'place',
  'case', 'point', 'fact', 'good', 'new', 'first', 'last', 'long', 'great',
  'little', 'own', 'other', 'old', 'right', 'big', 'high', 'small', 'large',
  'next', 'early', 'young', 'few', 'public', 'bad', 'same', 'able',
  // Prepositions & conjunctions
  'in', 'on', 'at', 'to', 'for', 'of', 'with', 'from', 'by', 'about', 'like',
  'through', 'over', 'before', 'between', 'under', 'since', 'without', 'and', 'or',
  'but', 'if', 'than', 'because', 'as', 'until', 'while', 'so', 'though', 'both',
  'each', 'when', 'where', 'why', 'how',
  // Common adverbs
  'not', 'now', 'just', 'more', 'also', 'here', 'there', 'then', 'only', 'very',
  'well', 'back', 'still', 'even', 'much', 'too', 'such', 'never', 'again', 'most',
  'once', 'off', 'away', 'down', 'out', 'up',
  // Tech / common words
  'test', 'code', 'data', 'file', 'line', 'text', 'word', 'number', 'system',
  'program', 'set', 'run', 'value', 'name', 'type', 'state', 'end', 'start',
];

/**
 * Generate token-budgeted filler ("lorem ipsum") text of approximately
 * `targetTokens` tokens, assembled only from {@link ONE_TOKEN_WORDS} so the
 * "one word == one token" approximation holds and the output lands near the
 * requested budget. Useful for long-context testing (filling a conversation to a
 * known size). Words are drawn with the module's unbiased, cryptographically
 * strong {@link pickRandom}/{@link randomInt} rather than `Math.random`.
 *
 * Shape mirrors natural prose: sentences of 10–20 words ending in a period, with
 * a paragraph break (~20% chance) between sentences. A non-positive, NaN, or
 * infinite budget yields an empty string.
 */
export function generateLoremIpsum(targetTokens: number): string {
  if (!Number.isFinite(targetTokens) || targetTokens <= 0) return '';
  const target = Math.floor(targetTokens);
  let tokens = 0;
  let result = '';

  while (tokens < target) {
    // Sentence: 10–20 words.
    const sentenceLength = 10 + randomInt(11);
    let wordsInSentence = 0;

    for (let i = 0; i < sentenceLength && tokens < target; i++) {
      result += pickRandom(ONE_TOKEN_WORDS);
      tokens++;
      wordsInSentence++;
      result += i === sentenceLength - 1 || tokens >= target ? '. ' : ' ';
    }

    // Paragraph break every few sentences (~20% chance per sentence).
    if (wordsInSentence > 0 && randomInt(5) === 0 && tokens < target) {
      result += '\n\n';
    }
  }

  return result.trim();
}

/** Generate a random "adjective-verb-noun" slug (e.g. "cosmic-pondering-lighthouse"). */
export function generateWordSlug(): string {
  return `${pickRandom(ADJECTIVES)}-${pickRandom(VERBS)}-${pickRandom(NOUNS)}`;
}

/** Generate a shorter "adjective-noun" slug (e.g. "graceful-unicorn"). */
export function generateShortWordSlug(): string {
  return `${pickRandom(ADJECTIVES)}-${pickRandom(NOUNS)}`;
}

/**
 * Generate a {@link generateWordSlug} that does not collide with an existing
 * artifact. The caller supplies a `taken` predicate (e.g. "a plan file with this
 * slug already exists on disk"); candidates are regenerated until one is free or
 * `maxRetries` attempts are exhausted, in which case the last candidate is
 * returned anyway so the caller always gets a usable name. Keeping the existence
 * check in the caller leaves this module free of I/O (pure building block).
 *
 * @param taken predicate returning true when a candidate slug is already in use
 * @param maxRetries maximum number of collision retries before giving up (default 10)
 */
export function generateUniqueSlug(
  taken: (slug: string) => boolean,
  maxRetries = 10,
): string {
  let slug = generateWordSlug();
  for (let i = 0; i < maxRetries && taken(slug); i++) {
    slug = generateWordSlug();
  }
  return slug;
}
