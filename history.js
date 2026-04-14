import fs from 'node:fs';
import path from 'node:path';
import {
  exportHistorySnapshot,
  findProjectProfilesByQuery,
  getProjectProfileByTelegramId,
  getProjectProfileByTelegramUsername,
  isTeamMember,
  linkProjectChat,
  upsertConversation,
  upsertProjectAlias,
} from './db.js';
import {
  buildSyntheticTelegramId,
  compactText,
  extractNumericTelegramId,
  normalizeTelegramUsername,
  nowIso,
  sha256,
} from './utils.js';

const EXPORT_HEADERS = [
  'LocalId',
  'ChatId',
  'ChatTitle',
  'ChatType',
  'TelegramMessageId',
  'ReplyToMessageId',
  'SenderId',
  'SenderName',
  'SenderUsername',
  'SenderRole',
  'MessageText',
  'MessageDateISO',
  'CapturedAt',
];

export function importHistoricalConversations({
  filePath,
  projectQuery = '',
  chatTitle = '',
  chatId = null,
} = {}) {
  const resolvedFilePath = path.resolve(filePath || '');
  if (!resolvedFilePath || !fs.existsSync(resolvedFilePath)) {
    throw new Error(`History file not found: ${resolvedFilePath || filePath}`);
  }

  const content = fs.readFileSync(resolvedFilePath, 'utf8');
  const parsed = parseHistoryContent(content, resolvedFilePath, {
    chatTitle,
    chatId,
  });
  const projectProfile = resolveProjectQuery(projectQuery);

  let imported = 0;
  let skipped = 0;
  const touchedChatIds = new Set();
  const observedProjectAliases = [];

  parsed.rows.forEach((row, index) => {
    const normalized = normalizeImportedRow(row, {
      index,
      fallbackChatId: parsed.chatId,
      fallbackChatTitle: parsed.chatTitle,
      fallbackChatType: parsed.chatType,
      fileSeed: resolvedFilePath,
      projectProfile,
    });

    if (!normalized.messageText) {
      skipped += 1;
      return;
    }

    upsertConversation(normalized);
    touchedChatIds.add(normalized.chatId);
    imported += 1;

    if (normalized.senderRole === 'project') {
      observedProjectAliases.push({
        senderId: normalized.senderId,
        senderUsername: normalized.senderUsername,
      });
    }
  });

  if (projectProfile) {
    for (const linkedChatId of touchedChatIds) {
      linkProjectChat({
        canonicalProjectId: projectProfile.telegram_user_id,
        chatId: linkedChatId,
        chatTitle: parsed.chatTitle,
      });
    }

    upsertProjectAlias({
      canonicalProjectId: projectProfile.telegram_user_id,
      aliasType: 'project_name',
      aliasValue: projectProfile.project_name,
      sourceType: 'history_import',
    });

    for (const alias of observedProjectAliases) {
      if (alias.senderId > 0) {
        upsertProjectAlias({
          canonicalProjectId: projectProfile.telegram_user_id,
          aliasType: 'telegram_id',
          aliasValue: alias.senderId,
          sourceType: 'history_import',
        });
      }

      if (alias.senderUsername) {
        upsertProjectAlias({
          canonicalProjectId: projectProfile.telegram_user_id,
          aliasType: 'telegram_username',
          aliasValue: alias.senderUsername,
          sourceType: 'history_import',
        });
      }
    }
  }

  return {
    filePath: resolvedFilePath,
    imported,
    skipped,
    format: parsed.format,
    chatTitle: parsed.chatTitle,
    chatIds: [...touchedChatIds],
    projectMatched: projectProfile?.project_name || '',
  };
}

export function writeHistoryExportFiles({
  outputDir,
  canonicalProjectId = null,
  chatId = null,
  label = 'history-export',
} = {}) {
  const snapshot = exportHistorySnapshot({ canonicalProjectId, chatId });
  const exportDir = path.resolve(outputDir || path.join(process.cwd(), 'exports'));
  fs.mkdirSync(exportDir, { recursive: true });

  const timestamp = nowIso().replace(/[:.]/g, '-');
  const safeLabel = sanitizeFileToken(label);
  const jsonPath = path.join(exportDir, `${safeLabel}-${timestamp}.json`);
  const csvPath = path.join(exportDir, `${safeLabel}-${timestamp}.csv`);

  fs.writeFileSync(jsonPath, JSON.stringify(snapshot, null, 2));
  fs.writeFileSync(csvPath, toCsv([
    EXPORT_HEADERS,
    ...snapshot.conversations.map((row) => ([
      row.id,
      row.chat_id,
      row.chat_title,
      row.chat_type,
      row.telegram_message_id,
      row.reply_to_message_id || '',
      row.sender_id,
      row.sender_name || '',
      row.sender_username || '',
      row.sender_role,
      row.message_text,
      row.message_date_iso,
      row.created_at,
    ])),
  ]));

  return {
    jsonPath,
    csvPath,
    conversationCount: snapshot.conversations.length,
    suggestionCount: snapshot.suggestions.length,
  };
}

function parseHistoryContent(content, filePath, options) {
  const extension = path.extname(filePath).toLowerCase();
  if (extension === '.csv') {
    const rows = parseCsv(content);
    return {
      format: 'csv',
      rows,
      chatTitle: options.chatTitle || path.basename(filePath, extension),
      chatType: 'group',
      chatId: options.chatId ? Number(options.chatId) : buildSyntheticTelegramId(`history-csv:${filePath}`),
    };
  }

  if (extension === '.jsonl' || extension === '.ndjson') {
    const rows = content
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
    return {
      format: extension.slice(1),
      rows,
      chatTitle: options.chatTitle || path.basename(filePath, extension),
      chatType: 'group',
      chatId: options.chatId ? Number(options.chatId) : buildSyntheticTelegramId(`history-jsonl:${filePath}`),
    };
  }

  const parsed = JSON.parse(content);
  if (Array.isArray(parsed)) {
    return {
      format: 'json-array',
      rows: parsed,
      chatTitle: options.chatTitle || path.basename(filePath, extension || '.json'),
      chatType: 'group',
      chatId: options.chatId ? Number(options.chatId) : buildSyntheticTelegramId(`history-json-array:${filePath}`),
    };
  }

  if (Array.isArray(parsed.messages)) {
    return {
      format: 'telegram-json',
      rows: parsed.messages,
      chatTitle: options.chatTitle || parsed.name || path.basename(filePath, extension || '.json'),
      chatType: parsed.type || 'group',
      chatId: options.chatId ? Number(options.chatId) : (extractNumericTelegramId(parsed.id) || buildSyntheticTelegramId(`history-telegram:${parsed.name || filePath}`)),
    };
  }

  if (Array.isArray(parsed.data)) {
    return {
      format: 'json-data-array',
      rows: parsed.data,
      chatTitle: options.chatTitle || parsed.chatTitle || path.basename(filePath, extension || '.json'),
      chatType: parsed.chatType || 'group',
      chatId: options.chatId ? Number(options.chatId) : (extractNumericTelegramId(parsed.chatId) || buildSyntheticTelegramId(`history-json-data:${filePath}`)),
    };
  }

  throw new Error(`Unsupported history format for file: ${filePath}`);
}

function normalizeImportedRow(row, {
  index,
  fallbackChatId,
  fallbackChatTitle,
  fallbackChatType,
  fileSeed,
  projectProfile,
}) {
  const senderUsername = normalizeTelegramUsername(
    row.sender_username
    || row.username
    || row.from_username
    || row.author_username
    || row.from
  );
  const senderName = compactText(
    row.sender_name
    || row.from
    || row.author
    || row.name
    || (senderUsername ? `@${senderUsername}` : '')
    || 'Unknown sender'
  );
  const senderId = (
    extractNumericTelegramId(row.sender_id)
    || extractNumericTelegramId(row.from_id)
    || extractNumericTelegramId(row.user_id)
    || extractNumericTelegramId(row.author_id)
    || buildSyntheticTelegramId(`history-sender:${fileSeed}:${senderUsername || senderName}`)
  );
  const explicitRole = String(
    row.sender_role
    || row.role
    || row.actor_role
    || ''
  ).trim().toLowerCase();
  const senderRole = explicitRole === 'team' || explicitRole === 'project'
    ? explicitRole
    : inferSenderRole({
        senderId,
        senderUsername,
        senderName,
        projectProfile,
      });

  const normalizedChatTitle = compactText(
    row.chat_title
    || row.chat_name
    || row.group
    || fallbackChatTitle
  ) || 'Imported chat';
  const normalizedChatId = (
    extractNumericTelegramId(row.chat_id)
    || extractNumericTelegramId(row.group_id)
    || fallbackChatId
    || buildSyntheticTelegramId(`history-chat:${fileSeed}:${normalizedChatTitle}`)
  );
  const messageText = buildImportedMessageText(row);

  return {
    chatId: normalizedChatId,
    chatTitle: normalizedChatTitle,
    chatType: row.chat_type || fallbackChatType || 'group',
    telegramMessageId: (
      extractNumericTelegramId(row.telegram_message_id)
      || extractNumericTelegramId(row.message_id)
      || extractNumericTelegramId(row.id)
      || index + 1
    ),
    replyToMessageId: (
      extractNumericTelegramId(row.reply_to_message_id)
      || extractNumericTelegramId(row.reply_to_id)
      || null
    ),
    senderId,
    senderName,
    senderUsername,
    senderRole,
    messageText,
    messageDateIso: normalizeImportedDate(
      row.message_date_iso
      || row.date
      || row.timestamp
      || row.created_at
    ),
  };
}

function buildImportedMessageText(row) {
  const text = flattenTextValue(
    row.message_text
    ?? row.text
    ?? row.caption
    ?? row.body
    ?? row.content
    ?? ''
  );
  const attachmentSummary = buildImportedAttachmentSummary(row);
  return compactText([text, attachmentSummary].filter(Boolean).join('\n'));
}

function flattenTextValue(value) {
  if (Array.isArray(value)) {
    return compactText(value.map((item) => {
      if (typeof item === 'string') {
        return item;
      }
      if (item && typeof item === 'object') {
        return item.text || item.href || '';
      }
      return '';
    }).join(''));
  }

  if (value && typeof value === 'object') {
    return compactText(value.text || value.caption || '');
  }

  return compactText(String(value || ''));
}

function buildImportedAttachmentSummary(row) {
  const types = [];
  if (row.document || row.file || row.file_name) {
    types.push('document');
  }
  if (row.photo || row.image) {
    types.push('photo');
  }
  if (row.video) {
    types.push('video');
  }
  if (row.voice || row.audio) {
    types.push('audio');
  }
  if (row.sticker) {
    types.push('sticker');
  }
  if (row.media_type) {
    types.push(String(row.media_type));
  }

  const fileName = compactText(
    row.file_name
    || row.file
    || row.document?.file_name
    || ''
  );

  if (!types.length && !fileName) {
    return '';
  }

  return compactText([
    `[Attachment: ${[...new Set(types)].join(', ') || 'file'}]`,
    fileName ? `File: ${fileName}` : '',
  ].filter(Boolean).join(' '));
}

function inferSenderRole({
  senderId,
  senderUsername,
  senderName,
  projectProfile,
}) {
  if (senderId > 0 && isTeamMember(senderId)) {
    return 'team';
  }

  if (projectProfile) {
    if (projectProfile.telegram_username && senderUsername === projectProfile.telegram_username) {
      return 'project';
    }

    if (projectProfile.telegram_user_id > 0 && senderId === projectProfile.telegram_user_id) {
      return 'project';
    }
  }

  if (/collably/i.test(senderName)) {
    return 'team';
  }

  return 'project';
}

function normalizeImportedDate(value) {
  const date = new Date(value || nowIso());
  if (Number.isNaN(date.getTime())) {
    return nowIso();
  }

  return date.toISOString();
}

function resolveProjectQuery(query) {
  const value = String(query || '').trim();
  if (!value) {
    return null;
  }

  const byId = extractNumericTelegramId(value);
  if (byId) {
    return getProjectProfileByTelegramId(byId);
  }

  const byUsername = normalizeTelegramUsername(value);
  if (byUsername) {
    return getProjectProfileByTelegramUsername(byUsername);
  }

  return findProjectProfilesByQuery(value, 1)[0] || null;
}

function parseCsv(content) {
  const rows = [];
  let current = '';
  let currentRow = [];
  let inQuotes = false;

  for (let index = 0; index < content.length; index += 1) {
    const char = content[index];
    const nextChar = content[index + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      currentRow.push(current);
      current = '';
      continue;
    }

    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && nextChar === '\n') {
        index += 1;
      }
      currentRow.push(current);
      rows.push(currentRow);
      current = '';
      currentRow = [];
      continue;
    }

    current += char;
  }

  if (current || currentRow.length) {
    currentRow.push(current);
    rows.push(currentRow);
  }

  const [headers = [], ...dataRows] = rows.filter((row) => row.some((cell) => String(cell || '').trim() !== ''));
  return dataRows.map((row) => {
    const result = {};
    headers.forEach((header, cellIndex) => {
      result[String(header || '').trim()] = row[cellIndex] || '';
    });
    return result;
  });
}

function toCsv(rows) {
  return rows.map((row) => row.map(escapeCsvCell).join(',')).join('\n');
}

function escapeCsvCell(value) {
  const stringValue = String(value ?? '');
  if (/[",\n]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }

  return stringValue;
}

function sanitizeFileToken(value) {
  return String(value || 'history-export')
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    || `history-${sha256(value).slice(0, 8)}`;
}
