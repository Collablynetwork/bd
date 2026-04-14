import path from 'node:path';
import { initDb } from './db.js';
import { writeHistoryExportFiles } from './history.js';
import {
  findProjectProfilesByQuery,
  getProjectProfileByTelegramId,
  getProjectProfileByTelegramUsername,
} from './db.js';
import { extractNumericTelegramId, normalizeTelegramUsername } from './utils.js';

const args = parseArgs(process.argv.slice(2));

initDb();

try {
  const target = resolveTarget(args.target || 'all');
  const result = writeHistoryExportFiles({
    outputDir: args.out || path.join(process.cwd(), 'exports'),
    canonicalProjectId: target.canonicalProjectId,
    chatId: target.chatId,
    label: target.label,
  });

  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  console.error('History export failed:', error.message);
  process.exit(1);
}

function resolveTarget(input) {
  const value = String(input || 'all').trim();
  if (!value || value.toLowerCase() === 'all') {
    return {
      canonicalProjectId: null,
      chatId: null,
      label: 'all-history',
    };
  }

  if (value.toLowerCase().startsWith('chat:')) {
    const chatId = extractNumericTelegramId(value.slice(5));
    if (!chatId) {
      throw new Error('Invalid chat target. Use chat:<chat_id>.');
    }

    return {
      canonicalProjectId: null,
      chatId,
      label: `chat-${chatId}`,
    };
  }

  const numericId = extractNumericTelegramId(value);
  if (numericId) {
    const profile = getProjectProfileByTelegramId(numericId);
    return {
      canonicalProjectId: profile?.telegram_user_id || numericId,
      chatId: null,
      label: profile?.project_name || `project-${numericId}`,
    };
  }

  const username = normalizeTelegramUsername(value);
  if (username) {
    const profile = getProjectProfileByTelegramUsername(username);
    if (!profile) {
      throw new Error(`Project not found for @${username}`);
    }
    return {
      canonicalProjectId: profile.telegram_user_id,
      chatId: null,
      label: profile.project_name || username,
    };
  }

  const profile = findProjectProfilesByQuery(value, 1)[0];
  if (!profile) {
    throw new Error(`Project not found for query: ${value}`);
  }

  return {
    canonicalProjectId: profile.telegram_user_id,
    chatId: null,
    label: profile.project_name || value,
  };
}

function parseArgs(argv) {
  const result = {};

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) {
      continue;
    }

    const key = token.slice(2);
    const value = argv[index + 1] && !argv[index + 1].startsWith('--')
      ? argv[index + 1]
      : 'true';

    result[key] = value;
    if (value !== 'true') {
      index += 1;
    }
  }

  return result;
}
