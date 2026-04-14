import { CLIENT_KNOWLEDGE_DOC_ID, docsClient, KNOWLEDGE_DOC_ID } from './config.js';
import {
  addKnowledgeItem,
  findProjectProfileForTelegramUser,
  hasKnowledgeExternalKey,
  searchKnowledgeByEmbedding,
  searchPartnerProfilesByEmbedding,
} from './db.js';
import { embedText } from './embed.js';
import { sha256 } from './utils.js';

const DOC_SOURCES = [
  { docId: KNOWLEDGE_DOC_ID, sourceType: 'doc', scope: 'services', title: 'Service Knowledge' },
  { docId: CLIENT_KNOWLEDGE_DOC_ID, sourceType: 'doc', scope: 'client_faq', title: 'Client Knowledge' },
].filter((item) => item.docId);

export async function importKnowledgeDocs() {
  let inserted = 0;

  for (const source of DOC_SOURCES) {
    const doc = await docsClient.documents.get({ documentId: source.docId });
    const paragraphs = (doc.data.body?.content || [])
      .map((block) => block.paragraph?.elements?.map((item) => item.textRun?.content || '').join(''))
      .map((item) => item?.trim())
      .filter(Boolean);

    for (const paragraph of paragraphs) {
      const externalKey = `doc:${source.docId}:${sha256(paragraph)}`;
      if (hasKnowledgeExternalKey(externalKey)) {
        continue;
      }

      const embedding = await embedText(paragraph);
      addKnowledgeItem({
        externalKey,
        sourceType: source.sourceType,
        scope: source.scope,
        title: source.title,
        content: paragraph,
        tags: [source.scope],
        embedding,
      });
      inserted += 1;
    }
  }

  return inserted;
}

export async function getRelevantKnowledge(queryText, limit = 6) {
  const queryEmbedding = await embedText(queryText);
  if (!queryEmbedding.length) {
    return { queryEmbedding, knowledge: [] };
  }

  const knowledge = searchKnowledgeByEmbedding(queryEmbedding, limit);
  return { queryEmbedding, knowledge };
}

export async function findPartnerCandidates(targetProfile, limit = 5) {
  let queryEmbedding = targetProfile?.embedding || [];
  if (!queryEmbedding.length && targetProfile?.overview) {
    queryEmbedding = await embedText(targetProfile.overview);
  }

  if (!queryEmbedding.length) {
    return [];
  }

  return searchPartnerProfilesByEmbedding(
    queryEmbedding,
    targetProfile.telegram_user_id,
    limit,
    targetProfile
  );
}

export function getProjectProfileForUser({ telegramUserId = null, telegramUsername = '', chatId = null } = {}) {
  return findProjectProfileForTelegramUser({ telegramUserId, telegramUsername, chatId });
}
