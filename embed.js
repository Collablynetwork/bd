import { openaiClient, OPENAI_EMBEDDING_MODEL } from './config.js';

export async function embedText(text) {
  const input = String(text || '').trim().slice(0, 8000);
  if (!input) {
    return [];
  }

  const response = await openaiClient.embeddings.create({
    model: OPENAI_EMBEDDING_MODEL,
    input,
  });

  return response.data[0]?.embedding || [];
}
