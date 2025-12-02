export function detectRepetition(text: string): { isRepetitive: boolean; truncateAt: number } {
  if (text.length < 200) return { isRepetitive: false, truncateAt: -1 };

  // Strategy 1: consecutive identical lines (original check)
  const lines = text.split('\n');
  if (lines.length >= 12) {
    let consecutiveCount = 1;
    for (let i = 1; i < lines.length; i++) {
      if (lines[i] === lines[i - 1] && lines[i].trim().length > 0) {
        consecutiveCount++;
        if (consecutiveCount >= 10) {
          const startLine = i - consecutiveCount + 1;
          const truncateAt = lines.slice(0, startLine + 1).join('\n').length;
          return { isRepetitive: true, truncateAt };
        }
      } else {
        consecutiveCount = 1;
      }
    }
  }

  // Strategy 2: repeated phrase detection (catches inline repetition)
  // Look for any phrase of 20+ chars repeated 5+ times
  const minPhraseLen = 20;
  const minRepeats = 5;
  const searchWindow = Math.min(text.length, 4000);
  const tail = text.slice(-searchWindow);

  for (let phraseLen = minPhraseLen; phraseLen <= 80; phraseLen += 10) {
    const phrase = tail.slice(-phraseLen);
    if (phrase.trim().length < minPhraseLen) continue;
    let count = 0;
    let pos = 0;
    while ((pos = tail.indexOf(phrase, pos)) !== -1) {
      count++;
      pos += 1;
      if (count >= minRepeats) {
        const firstOccurrence = text.indexOf(phrase);
        return { isRepetitive: true, truncateAt: firstOccurrence + phrase.length };
      }
    }
  }

  return { isRepetitive: false, truncateAt: -1 };
}
