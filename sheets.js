import {
  CLIENT_DATA_SHEET_ID,
  CLIENT_DATA_SHEET_NAME,
  SERVICE_DATASET_SHEET_ID,
  SERVICE_DATASET_SHEET_NAME,
  SHEETS_EXPORT_DELAY_HOURS,
  SPREADSHEET_ID,
  sheetsClient,
} from './config.js';
import {
  addKnowledgeItem,
  getProjectProfileByTelegramId,
  upsertProjectAlias,
  getUnsyncedConversations,
  getUnsyncedKnowledge,
  getUnsyncedSuggestions,
  markConversationsSynced,
  markKnowledgeSynced,
  markSuggestionsSynced,
  upsertProjectProfile,
} from './db.js';
import { embedText } from './embed.js';
import {
  chunkArray,
  compactText,
  dedupeStrings,
  deriveProjectStatus,
  extractNumericTelegramId,
  fieldLookup,
  normalizeTelegramUsername,
  nowIso,
  projectOverviewFromFields,
  sha256,
  buildSyntheticTelegramId,
  subtractHours,
} from './utils.js';

const CONVERSATION_HEADERS = [
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

const SUGGESTION_HEADERS = [
  'SuggestionId',
  'ChatId',
  'ChatTitle',
  'ChatLink',
  'ClientMessageId',
  'ClientSenderId',
  'ClientSenderName',
  'ClientText',
  'AISuggestion',
  'ServiceAngle',
  'Reason',
  'Confidence',
  'Status',
  'ActualReplyText',
  'ActionTakenById',
  'ActionTakenByName',
  'ActionSource',
  'ActionTakenAt',
  'ReminderStage',
  'CreatedAt',
  'UpdatedAt',
];

const KNOWLEDGE_HEADERS = [
  'KnowledgeId',
  'ExternalKey',
  'SourceType',
  'Scope',
  'Title',
  'Content',
  'Tags',
  'ChatId',
  'RelatedProjectTelegramId',
  'CreatedBy',
  'CreatedAt',
];

export async function ensureSheet(title, headers) {
  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const exists = (meta.data.sheets || []).some((sheet) => sheet.properties?.title === title);
  if (exists) {
    return;
  }

  await sheetsClient.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        {
          addSheet: {
            properties: { title },
          },
        },
      ],
    },
  });

  if (headers?.length) {
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${title}!A1`,
      valueInputOption: 'RAW',
      requestBody: { values: [headers] },
    });
  }
}

export async function appendRows(title, rows) {
  if (!rows.length) {
    return;
  }

  for (const chunk of chunkArray(rows, 200)) {
    await sheetsClient.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: `${title}!A1`,
      valueInputOption: 'RAW',
      requestBody: { values: chunk },
    });
  }
}

export async function syncProjectProfilesFromSheet() {
  const sheetTitle = await resolveClientSheetTitle();
  const response = await sheetsClient.spreadsheets.values.get({
    spreadsheetId: CLIENT_DATA_SHEET_ID,
    range: `${sheetTitle}!A:ZZ`,
  });

  const [headers = [], ...rows] = response.data.values || [];
  let syncedCount = 0;

  for (const [index, row] of rows.entries()) {
    const rawFields = {};
    headers.forEach((header, cellIndex) => {
      if (!header) {
        return;
      }
      const value = String(row[cellIndex] || '').trim();
      if (value) {
        rawFields[String(header).trim()] = value;
      }
    });

    const telegramContactValue = fieldLookup(rawFields, ['telegram', 'contact'])
      || fieldLookup(rawFields, ['telegram', 'id']);
    const telegramUsername = normalizeTelegramUsername(telegramContactValue);
    const numericTelegramId = extractNumericTelegramId(telegramContactValue);
    const telegramUserId = numericTelegramId || (telegramUsername
      ? buildSyntheticTelegramId(`telegram-username:${telegramUsername}`)
      : 0);

    if (!telegramUserId && !telegramUsername) {
      continue;
    }

    const projectName = fieldLookup(rawFields, ['project', 'name']) || `Project ${telegramUserId}`;
    const categories = dedupeStrings(
      Object.entries(rawFields)
        .filter(([key]) => key.toLowerCase().includes('category'))
        .map(([, value]) => value)
    );
    const targets = dedupeStrings(
      Object.entries(rawFields)
        .filter(([key]) => key.toLowerCase().includes('target'))
        .map(([, value]) => value)
    );
    const overview = projectOverviewFromFields({
      'Project Name': projectName,
      ...rawFields,
    });
    const contentHash = sha256(JSON.stringify(rawFields));
    const existing = getProjectProfileByTelegramId(telegramUserId);
    const embedding = existing?.content_hash === contentHash
      ? existing.embedding
      : (overview ? await embedText(overview) : []);

    upsertProjectProfile({
      telegramUserId,
      telegramUsername,
      telegramContactValue,
      projectName,
      projectStatus: deriveProjectStatus(rawFields),
      leadStage: existing?.lead_stage || '',
      overview,
      categories,
      targets,
      rawFields,
      embedding,
      contentHash,
      sourceRowNumber: index + 2,
      lastFormSyncAt: nowIso(),
    });

    upsertProjectAlias({
      canonicalProjectId: telegramUserId,
      aliasType: 'project_name',
      aliasValue: projectName,
      sourceType: 'form_sync',
    });

    if (numericTelegramId) {
      upsertProjectAlias({
        canonicalProjectId: telegramUserId,
        aliasType: 'telegram_id',
        aliasValue: numericTelegramId,
        sourceType: 'form_sync',
      });
    }

    if (telegramUsername) {
      upsertProjectAlias({
        canonicalProjectId: telegramUserId,
        aliasType: 'telegram_username',
        aliasValue: telegramUsername,
        sourceType: 'form_sync',
      });
    }

    syncedCount += 1;
  }

  return syncedCount;
}

export async function importServiceDatasetFromSheet() {
  if (!SERVICE_DATASET_SHEET_ID) {
    return {
      enabled: false,
      imported: 0,
      sheetsProcessed: 0,
    };
  }

  let imported = 0;
  const sheetTitles = await resolveServiceDatasetSheetTitles();

  for (const sheetTitle of sheetTitles) {
    const response = await sheetsClient.spreadsheets.values.get({
      spreadsheetId: SERVICE_DATASET_SHEET_ID,
      range: `${sheetTitle}!A:ZZ`,
    });

    const [headers = [], ...rows] = response.data.values || [];

    for (const [index, row] of rows.entries()) {
      const rawFields = {};
      headers.forEach((header, cellIndex) => {
        if (!header) {
          return;
        }

        const value = String(row[cellIndex] || '').trim();
        if (value) {
          rawFields[String(header).trim()] = value;
        }
      });

      if (!Object.keys(rawFields).length) {
        continue;
      }

      const providerName = pickServiceField(rawFields, [
        ['provider'],
        ['resource'],
        ['company'],
        ['partner'],
        ['vendor'],
        ['name'],
        ['exchange'],
        ['firm'],
      ]) || `Service row ${index + 2}`;
      const category = pickServiceField(rawFields, [
        ['category'],
        ['type'],
        ['service', 'category'],
        ['segment'],
        ['vertical'],
      ]);
      const cost = pickServiceField(rawFields, [
        ['cost'],
        ['price'],
        ['pricing'],
        ['fee'],
        ['budget'],
      ]);
      const terms = pickServiceField(rawFields, [
        ['term'],
        ['contract'],
        ['condition'],
        ['payment'],
      ]);
      const services = pickServiceField(rawFields, [
        ['service'],
        ['offer'],
        ['support'],
        ['product'],
      ]);

      const orderedLines = [
        `Source Sheet: ${sheetTitle}`,
        `Provider: ${providerName}`,
        category ? `Category: ${category}` : '',
        services ? `Services: ${services}` : '',
        cost ? `Cost: ${cost}` : '',
        terms ? `Terms: ${terms}` : '',
        ...Object.entries(rawFields)
          .filter(([key]) => !isPrimaryServiceField(key))
          .map(([key, value]) => `${key}: ${value}`),
      ].filter(Boolean);

      const content = compactText(orderedLines.join('\n'));
      const title = [providerName, category].filter(Boolean).join(' - ') || providerName;
      const tags = dedupeStrings([
        'service-catalog',
        slugTag(sheetTitle),
        slugTag(category),
        slugTag(providerName),
        ...Object.entries(rawFields)
          .filter(([key, value]) => /launchpad|cex|market maker|marketmaker|otc|audit|listing|maker/i.test(`${key} ${value}`))
          .map(([key]) => slugTag(key)),
      ]).filter(Boolean);

      const externalKey = `service-dataset:${SERVICE_DATASET_SHEET_ID}:${sheetTitle}:${index + 2}`;
      const embedding = await embedText(content);

      addKnowledgeItem({
        externalKey,
        sourceType: 'service_dataset',
        scope: 'service_catalog',
        title,
        content,
        tags,
        embedding,
      });
      imported += 1;
    }
  }

  return {
    enabled: true,
    imported,
    sheetsProcessed: sheetTitles.length,
  };
}

export async function exportPendingDataToSheets() {
  const cutoffIso = subtractHours(new Date(), SHEETS_EXPORT_DELAY_HOURS);

  await ensureSheet('ConversationArchive', CONVERSATION_HEADERS);
  await ensureSheet('SuggestionArchive', SUGGESTION_HEADERS);
  await ensureSheet('KnowledgeArchive', KNOWLEDGE_HEADERS);

  const conversations = getUnsyncedConversations(cutoffIso);
  const suggestions = getUnsyncedSuggestions(cutoffIso);
  const knowledgeItems = getUnsyncedKnowledge(cutoffIso);

  if (conversations.length) {
    await appendRows('ConversationArchive', conversations.map((row) => [
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
    ]));
    markConversationsSynced(conversations.map((row) => row.id));
  }

  if (suggestions.length) {
    await appendRows('SuggestionArchive', suggestions.map((row) => [
      row.id,
      row.chat_id,
      row.chat_title,
      row.chat_link || '',
      row.client_message_id,
      row.client_sender_id,
      row.client_sender_name || '',
      row.client_text,
      row.ai_response,
      row.service_angle || '',
      row.reason || '',
      row.confidence || '',
      row.status,
      row.actual_reply_text || '',
      row.action_taken_by_id || '',
      row.action_taken_by_name || '',
      row.action_source || '',
      row.action_taken_at || '',
      row.reminder_stage,
      row.created_at,
      row.updated_at,
    ]));
    markSuggestionsSynced(suggestions.map((row) => row.id));
  }

  if (knowledgeItems.length) {
    await appendRows('KnowledgeArchive', knowledgeItems.map((row) => [
      row.id,
      row.external_key || '',
      row.source_type,
      row.scope,
      row.title || '',
      row.content,
      row.tags_json || '[]',
      row.chat_id || '',
      row.related_project_tg_id || '',
      row.created_by || '',
      row.created_at,
    ]));
    markKnowledgeSynced(knowledgeItems.map((row) => row.id));
  }

  return {
    conversations: conversations.length,
    suggestions: suggestions.length,
    knowledgeItems: knowledgeItems.length,
  };
}

async function resolveClientSheetTitle() {
  return resolveSheetTitle(CLIENT_DATA_SHEET_ID, CLIENT_DATA_SHEET_NAME);
}

async function resolveSheetTitle(spreadsheetId, explicitTitle = '') {
  if (explicitTitle) {
    return explicitTitle;
  }

  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId });
  return meta.data.sheets?.[0]?.properties?.title || 'Sheet1';
}

async function resolveServiceDatasetSheetTitles() {
  if (SERVICE_DATASET_SHEET_NAME) {
    return SERVICE_DATASET_SHEET_NAME
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
  }

  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: SERVICE_DATASET_SHEET_ID });
  return (meta.data.sheets || [])
    .map((sheet) => sheet.properties?.title)
    .filter(Boolean);
}

function pickServiceField(rawFields, patternSets) {
  for (const patterns of patternSets) {
    const value = fieldLookup(rawFields, patterns);
    if (value) {
      return value;
    }
  }

  return '';
}

function isPrimaryServiceField(key) {
  const normalized = key.toLowerCase();
  return [
    'provider',
    'resource',
    'company',
    'partner',
    'vendor',
    'name',
    'exchange',
    'firm',
    'category',
    'type',
    'segment',
    'vertical',
    'cost',
    'price',
    'pricing',
    'fee',
    'budget',
    'term',
    'contract',
    'condition',
    'payment',
    'service',
    'offer',
    'support',
    'product',
  ].some((token) => normalized.includes(token));
}

function slugTag(value = '') {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}
