import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import cosine from 'compute-cosine-similarity';
import { DB_PATH, FOLLOW_UP_STEP_HOURS, TEAM_IDS } from './config.js';
import { addHours, jsonParse, jsonStringify, normalizeTelegramUsername, nowIso } from './utils.js';

fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

let db = openDatabaseWithRecovery(DB_PATH);

export function initDb() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS conversations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chat_id INTEGER NOT NULL,
      chat_title TEXT NOT NULL,
      chat_type TEXT NOT NULL,
      telegram_message_id INTEGER NOT NULL,
      reply_to_message_id INTEGER,
      sender_id INTEGER NOT NULL,
      sender_name TEXT,
      sender_username TEXT,
      sender_role TEXT NOT NULL,
      message_text TEXT NOT NULL,
      message_date_iso TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      sheet_synced_at TEXT,
      UNIQUE(chat_id, telegram_message_id)
    );

    CREATE TABLE IF NOT EXISTS suggestions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chat_id INTEGER NOT NULL,
      chat_title TEXT NOT NULL,
      chat_link TEXT,
      client_message_id INTEGER NOT NULL,
      client_sender_id INTEGER NOT NULL,
      client_sender_name TEXT,
      client_text TEXT NOT NULL,
      ai_response TEXT NOT NULL,
      service_angle TEXT,
      reason TEXT,
      confidence TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      actual_reply_text TEXT,
      action_taken_by_id INTEGER,
      action_taken_by_name TEXT,
      action_source TEXT,
      action_taken_at TEXT,
      reminder_stage INTEGER NOT NULL DEFAULT 0,
      last_reminder_at TEXT,
      next_reminder_at TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      sheet_synced_at TEXT
    );

    CREATE TABLE IF NOT EXISTS suggestion_deliveries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      suggestion_id INTEGER NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
      admin_id INTEGER NOT NULL,
      dm_chat_id INTEGER NOT NULL,
      dm_message_id INTEGER NOT NULL,
      delivery_type TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      UNIQUE(dm_chat_id, dm_message_id)
    );

    CREATE TABLE IF NOT EXISTS knowledge_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      external_key TEXT UNIQUE,
      source_type TEXT NOT NULL,
      scope TEXT NOT NULL,
      title TEXT,
      content TEXT NOT NULL,
      tags_json TEXT,
      embedding_json TEXT,
      chat_id INTEGER,
      related_project_tg_id INTEGER,
      created_by INTEGER,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      last_used_at TEXT,
      active INTEGER NOT NULL DEFAULT 1,
      sheet_synced_at TEXT
    );

    CREATE TABLE IF NOT EXISTS project_profiles (
      telegram_user_id INTEGER PRIMARY KEY,
      telegram_username TEXT,
      telegram_contact_value TEXT,
      project_name TEXT,
      project_status TEXT,
      lead_stage TEXT,
      overview TEXT NOT NULL,
      categories_json TEXT,
      targets_json TEXT,
      raw_fields_json TEXT NOT NULL,
      embedding_json TEXT,
      content_hash TEXT NOT NULL,
      source_row_number INTEGER,
      last_form_sync_at TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS operator_instructions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      scope TEXT NOT NULL DEFAULT 'global',
      target_value TEXT,
      target_label TEXT,
      instruction_type TEXT NOT NULL,
      content TEXT NOT NULL,
      active INTEGER NOT NULL DEFAULT 1,
      created_by INTEGER,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS announcement_reminders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      external_key TEXT UNIQUE,
      project_reference TEXT,
      project_label TEXT,
      chat_id INTEGER,
      chat_title TEXT,
      chat_link TEXT,
      announcement_text TEXT NOT NULL,
      announcement_at TEXT NOT NULL,
      remind_at TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      sent_at TEXT,
      created_by INTEGER,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS sync_state (
      key TEXT PRIMARY KEY,
      value TEXT
    );

    CREATE TABLE IF NOT EXISTS team_members (
      telegram_user_id INTEGER PRIMARY KEY,
      active INTEGER NOT NULL DEFAULT 1,
      added_by INTEGER,
      added_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS project_aliases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      canonical_project_id INTEGER NOT NULL,
      alias_type TEXT NOT NULL,
      alias_value TEXT NOT NULL,
      source_type TEXT,
      active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      UNIQUE(alias_type, alias_value)
    );

    CREATE TABLE IF NOT EXISTS project_chat_links (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      canonical_project_id INTEGER NOT NULL,
      chat_id INTEGER NOT NULL,
      chat_title TEXT,
      active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      UNIQUE(canonical_project_id, chat_id)
    );

    CREATE TABLE IF NOT EXISTS suggestion_reviews (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      suggestion_id INTEGER NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
      reviewer_id INTEGER,
      verdict TEXT NOT NULL,
      note TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS source_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_key TEXT NOT NULL UNIQUE,
      source_type TEXT NOT NULL,
      title TEXT,
      content_text TEXT,
      content_json TEXT,
      metadata_json TEXT,
      refreshed_at TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE INDEX IF NOT EXISTS idx_conversations_chat_id ON conversations(chat_id, message_date_iso);
    CREATE INDEX IF NOT EXISTS idx_conversations_sender_id ON conversations(sender_id, message_date_iso);
    CREATE INDEX IF NOT EXISTS idx_suggestions_due ON suggestions(status, next_reminder_at);
    CREATE INDEX IF NOT EXISTS idx_knowledge_scope ON knowledge_items(scope, created_at);
    CREATE INDEX IF NOT EXISTS idx_operator_instructions_active ON operator_instructions(active, scope, target_value);
    CREATE INDEX IF NOT EXISTS idx_announcement_due ON announcement_reminders(status, remind_at);
    CREATE INDEX IF NOT EXISTS idx_project_aliases_lookup ON project_aliases(alias_type, alias_value, active);
    CREATE INDEX IF NOT EXISTS idx_project_chat_links_lookup ON project_chat_links(chat_id, active);
    CREATE INDEX IF NOT EXISTS idx_suggestion_reviews_lookup ON suggestion_reviews(suggestion_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_source_snapshots_type ON source_snapshots(source_type, refreshed_at);
  `);

  ensureColumn('project_profiles', 'telegram_username', 'TEXT');
  ensureColumn('project_profiles', 'telegram_contact_value', 'TEXT');
  ensureColumn('project_profiles', 'lead_stage', 'TEXT');
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_project_profiles_username ON project_profiles(telegram_username);
  `);

  seedTeamMembers(TEAM_IDS);
}

export function getDbPath() {
  return DB_PATH;
}

function openDatabaseWithRecovery(dbPath) {
  try {
    return openConfiguredDatabase(dbPath);
  } catch (error) {
    if (!isMalformedDbError(error)) {
      throw error;
    }

    const backupPrefix = `${dbPath}.corrupt-${Date.now()}`;
    backupCorruptDatabaseFiles(dbPath, backupPrefix);
    console.warn(`Detected malformed SQLite database. Created backups with prefix: ${backupPrefix}`);
    return openConfiguredDatabase(dbPath);
  }
}

function openConfiguredDatabase(dbPath) {
  const database = new Database(dbPath);
  database.pragma('journal_mode = WAL');
  database.pragma('foreign_keys = ON');
  verifyDatabaseIntegrity(database);
  return database;
}

function verifyDatabaseIntegrity(database) {
  const rows = database.pragma('quick_check');
  const firstRow = Array.isArray(rows) && rows.length ? rows[0] : null;
  const firstValue = firstRow ? Object.values(firstRow)[0] : '';
  if (String(firstValue || '').toLowerCase() !== 'ok') {
    throw new Error(`SQLite integrity check failed: ${firstValue || 'unknown error'}`);
  }
}

function backupCorruptDatabaseFiles(dbPath, backupPrefix) {
  for (const suffix of ['', '-wal', '-shm']) {
    const source = `${dbPath}${suffix}`;
    if (!fs.existsSync(source)) {
      continue;
    }

    const target = `${backupPrefix}${suffix}`;
    fs.renameSync(source, target);
  }
}

function isMalformedDbError(error) {
  return String(error?.message || '').toLowerCase().includes('malformed');
}

export function addTeamMember(telegramUserId, addedBy = null) {
  db.prepare(`
    INSERT INTO team_members (
      telegram_user_id,
      active,
      added_by,
      added_at
    ) VALUES (?, 1, ?, ?)
    ON CONFLICT(telegram_user_id) DO UPDATE SET
      active = 1,
      added_by = COALESCE(excluded.added_by, team_members.added_by)
  `).run(telegramUserId, addedBy, nowIso());
}

export function getTeamMemberIds() {
  return db.prepare(`
    SELECT telegram_user_id
    FROM team_members
    WHERE active = 1
    ORDER BY telegram_user_id ASC
  `).all().map((row) => row.telegram_user_id);
}

export function isTeamMember(telegramUserId) {
  const row = db.prepare(`
    SELECT 1
    FROM team_members
    WHERE telegram_user_id = ?
      AND active = 1
    LIMIT 1
  `).get(telegramUserId);

  return Boolean(row);
}

export function upsertProjectAlias({
  canonicalProjectId,
  aliasType,
  aliasValue,
  sourceType = 'runtime',
}) {
  const normalizedAliasValue = normalizeAliasValue(aliasType, aliasValue);
  if (!canonicalProjectId || !aliasType || !normalizedAliasValue) {
    return false;
  }

  db.prepare(`
    INSERT INTO project_aliases (
      canonical_project_id,
      alias_type,
      alias_value,
      source_type,
      active,
      created_at,
      updated_at
    ) VALUES (?, ?, ?, ?, 1, ?, ?)
    ON CONFLICT(alias_type, alias_value) DO UPDATE SET
      canonical_project_id = excluded.canonical_project_id,
      source_type = excluded.source_type,
      active = 1,
      updated_at = excluded.updated_at
  `).run(
    canonicalProjectId,
    aliasType,
    normalizedAliasValue,
    sourceType,
    nowIso(),
    nowIso()
  );

  return true;
}

export function linkProjectChat({
  canonicalProjectId,
  chatId,
  chatTitle = '',
}) {
  if (!canonicalProjectId || !chatId) {
    return false;
  }

  db.prepare(`
    INSERT INTO project_chat_links (
      canonical_project_id,
      chat_id,
      chat_title,
      active,
      created_at,
      updated_at
    ) VALUES (?, ?, ?, 1, ?, ?)
    ON CONFLICT(canonical_project_id, chat_id) DO UPDATE SET
      chat_title = excluded.chat_title,
      active = 1,
      updated_at = excluded.updated_at
  `).run(
    canonicalProjectId,
    chatId,
    chatTitle || '',
    nowIso(),
    nowIso()
  );

  return true;
}

export function updateProjectLeadStage(canonicalProjectId, leadStage) {
  db.prepare(`
    UPDATE project_profiles
    SET lead_stage = ?,
        updated_at = ?
    WHERE telegram_user_id = ?
  `).run(leadStage || '', nowIso(), canonicalProjectId);

  return getProjectProfileByTelegramId(canonicalProjectId);
}

export function addSuggestionReview({
  suggestionId,
  reviewerId = null,
  verdict,
  note = '',
}) {
  const result = db.prepare(`
    INSERT INTO suggestion_reviews (
      suggestion_id,
      reviewer_id,
      verdict,
      note,
      created_at
    ) VALUES (?, ?, ?, ?, ?)
  `).run(
    suggestionId,
    reviewerId,
    verdict,
    note || '',
    nowIso()
  );

  return getSuggestionReviewById(result.lastInsertRowid);
}

export function getSuggestionReviewById(reviewId) {
  return db.prepare(`
    SELECT *
    FROM suggestion_reviews
    WHERE id = ?
  `).get(reviewId) || null;
}

export function listSuggestionReviews(suggestionId) {
  return db.prepare(`
    SELECT *
    FROM suggestion_reviews
    WHERE suggestion_id = ?
    ORDER BY created_at DESC, id DESC
  `).all(suggestionId);
}

export function getKnowledgeItemById(knowledgeId) {
  const row = db.prepare(`
    SELECT *
    FROM knowledge_items
    WHERE id = ?
  `).get(knowledgeId);

  return row ? {
    ...row,
    tags: jsonParse(row.tags_json, []),
    embedding: jsonParse(row.embedding_json, []),
  } : null;
}

export function searchKnowledgeByText(query, limit = 12) {
  const tokens = buildSearchTokens(query).slice(0, 5);
  if (!tokens.length) {
    return [];
  }

  const clauses = [];
  const params = [];
  for (const token of tokens) {
    const likeValue = `%${token}%`;
    clauses.push(`
      LOWER(COALESCE(title, '')) LIKE ?
      OR LOWER(COALESCE(content, '')) LIKE ?
      OR LOWER(COALESCE(tags_json, '')) LIKE ?
    `);
    params.push(likeValue, likeValue, likeValue);
  }

  return db.prepare(`
    SELECT *
    FROM knowledge_items
    WHERE active = 1
      AND (${clauses.map((clause) => `(${clause})`).join(' OR ')})
    ORDER BY created_at DESC, id DESC
    LIMIT ?
  `).all(...params, limit).map((row) => ({
    ...row,
    tags: jsonParse(row.tags_json, []),
  }));
}

export function deactivateKnowledgeItem(knowledgeId) {
  const result = db.prepare(`
    UPDATE knowledge_items
    SET active = 0
    WHERE id = ?
      AND active = 1
  `).run(knowledgeId);

  return result.changes > 0;
}

export function upsertSourceSnapshot({
  sourceKey,
  sourceType,
  title = '',
  contentText = '',
  contentJson = null,
  metadata = null,
}) {
  db.prepare(`
    INSERT INTO source_snapshots (
      source_key,
      source_type,
      title,
      content_text,
      content_json,
      metadata_json,
      refreshed_at,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(source_key) DO UPDATE SET
      source_type = excluded.source_type,
      title = excluded.title,
      content_text = excluded.content_text,
      content_json = excluded.content_json,
      metadata_json = excluded.metadata_json,
      refreshed_at = excluded.refreshed_at
  `).run(
    sourceKey,
    sourceType,
    title || '',
    contentText || '',
    contentJson ? jsonStringify(contentJson) : null,
    metadata ? jsonStringify(metadata) : null,
    nowIso(),
    nowIso()
  );
}

export function getSourceSnapshot(sourceKey) {
  const row = db.prepare(`
    SELECT *
    FROM source_snapshots
    WHERE source_key = ?
  `).get(sourceKey);

  return row ? hydrateSourceSnapshot(row) : null;
}

export function listSourceSnapshots(sourceType = '', limit = 100) {
  const rows = sourceType
    ? db.prepare(`
        SELECT *
        FROM source_snapshots
        WHERE source_type = ?
        ORDER BY refreshed_at DESC, id DESC
        LIMIT ?
      `).all(sourceType, limit)
    : db.prepare(`
        SELECT *
        FROM source_snapshots
        ORDER BY refreshed_at DESC, id DESC
        LIMIT ?
      `).all(limit);

  return rows.map(hydrateSourceSnapshot);
}

export function addOperatorInstruction({
  scope = 'global',
  targetValue = null,
  targetLabel = '',
  instructionType,
  content,
  createdBy = null,
}) {
  const result = db.prepare(`
    INSERT INTO operator_instructions (
      scope,
      target_value,
      target_label,
      instruction_type,
      content,
      created_by,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(
    scope,
    targetValue,
    targetLabel || '',
    instructionType,
    content,
    createdBy,
    nowIso()
  );

  return getOperatorInstructionById(result.lastInsertRowid);
}

export function getOperatorInstructionById(instructionId) {
  return db.prepare(`
    SELECT *
    FROM operator_instructions
    WHERE id = ?
  `).get(instructionId) || null;
}

export function listOperatorInstructions(limit = 100) {
  return db.prepare(`
    SELECT *
    FROM operator_instructions
    WHERE active = 1
    ORDER BY scope ASC, created_at DESC, id DESC
    LIMIT ?
  `).all(limit);
}

export function removeOperatorInstruction(instructionId) {
  const result = db.prepare(`
    UPDATE operator_instructions
    SET active = 0
    WHERE id = ?
      AND active = 1
  `).run(instructionId);

  return result.changes > 0;
}

export function getApplicableInstructions({
  projectTelegramUserId = null,
  projectTelegramUsername = '',
} = {}) {
  const clauses = [`scope = 'global'`];
  const params = [];
  const normalizedUsername = normalizeTelegramUsername(projectTelegramUsername);

  if (projectTelegramUserId) {
    clauses.push(`target_value = ?`);
    params.push(`telegram_id:${projectTelegramUserId}`);
  }

  if (normalizedUsername) {
    clauses.push(`target_value = ?`);
    params.push(`telegram_username:${normalizedUsername}`);
  }

  return db.prepare(`
    SELECT *
    FROM operator_instructions
    WHERE active = 1
      AND (${clauses.join(' OR ')})
    ORDER BY CASE WHEN scope = 'global' THEN 0 ELSE 1 END ASC, created_at ASC, id ASC
  `).all(...params);
}

export function upsertAnnouncementReminder({
  externalKey,
  projectReference = '',
  projectLabel = '',
  chatId = null,
  chatTitle = '',
  chatLink = '',
  announcementText,
  announcementAt,
  remindAt,
  createdBy = null,
}) {
  db.prepare(`
    INSERT INTO announcement_reminders (
      external_key,
      project_reference,
      project_label,
      chat_id,
      chat_title,
      chat_link,
      announcement_text,
      announcement_at,
      remind_at,
      status,
      sent_at,
      created_by,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)
    ON CONFLICT(external_key) DO UPDATE SET
      project_reference = excluded.project_reference,
      project_label = excluded.project_label,
      chat_id = excluded.chat_id,
      chat_title = excluded.chat_title,
      chat_link = excluded.chat_link,
      announcement_text = excluded.announcement_text,
      announcement_at = excluded.announcement_at,
      remind_at = excluded.remind_at,
      status = CASE
        WHEN announcement_reminders.sent_at IS NULL
          OR excluded.announcement_at != announcement_reminders.announcement_at
          OR excluded.remind_at != announcement_reminders.remind_at
        THEN 'pending'
        ELSE announcement_reminders.status
      END,
      sent_at = CASE
        WHEN excluded.announcement_at != announcement_reminders.announcement_at THEN NULL
        ELSE announcement_reminders.sent_at
      END
  `).run(
    externalKey,
    projectReference || '',
    projectLabel || '',
    chatId,
    chatTitle || '',
    chatLink || '',
    announcementText,
    announcementAt,
    remindAt,
    createdBy,
    nowIso()
  );

  return getAnnouncementReminderByExternalKey(externalKey);
}

export function getAnnouncementReminderByExternalKey(externalKey) {
  return db.prepare(`
    SELECT *
    FROM announcement_reminders
    WHERE external_key = ?
  `).get(externalKey) || null;
}

export function getDueAnnouncementReminders(cutoffIso) {
  return db.prepare(`
    SELECT *
    FROM announcement_reminders
    WHERE status = 'pending'
      AND remind_at <= ?
      AND sent_at IS NULL
    ORDER BY remind_at ASC, id ASC
  `).all(cutoffIso);
}

export function markAnnouncementReminderSent(reminderId) {
  const sentAt = nowIso();
  db.prepare(`
    UPDATE announcement_reminders
    SET status = 'sent',
        sent_at = ?
    WHERE id = ?
  `).run(sentAt, reminderId);
}

export function listUpcomingAnnouncementReminders(limit = 25) {
  return db.prepare(`
    SELECT *
    FROM announcement_reminders
    WHERE status = 'pending'
      AND announcement_at >= ?
    ORDER BY announcement_at ASC, id ASC
    LIMIT ?
  `).all(nowIso(), limit);
}

export function upsertConversation(record) {
  const stmt = db.prepare(`
    INSERT INTO conversations (
      chat_id,
      chat_title,
      chat_type,
      telegram_message_id,
      reply_to_message_id,
      sender_id,
      sender_name,
      sender_username,
      sender_role,
      message_text,
      message_date_iso,
      created_at
    ) VALUES (
      @chatId,
      @chatTitle,
      @chatType,
      @telegramMessageId,
      @replyToMessageId,
      @senderId,
      @senderName,
      @senderUsername,
      @senderRole,
      @messageText,
      @messageDateIso,
      @createdAt
    )
    ON CONFLICT(chat_id, telegram_message_id) DO UPDATE SET
      reply_to_message_id = excluded.reply_to_message_id,
      sender_name = excluded.sender_name,
      sender_username = excluded.sender_username,
      sender_role = excluded.sender_role,
      message_text = excluded.message_text,
      message_date_iso = excluded.message_date_iso
  `);

  stmt.run({
    ...record,
    createdAt: nowIso(),
  });
}

export function getRecentConversation(chatId, limit = 12) {
  const rows = db.prepare(`
    SELECT *
    FROM conversations
    WHERE chat_id = ?
    ORDER BY message_date_iso DESC, id DESC
    LIMIT ?
  `).all(chatId, limit);

  return rows.reverse();
}

export function searchConversationSnippetsByChat(chatId, query, limit = 12) {
  if (!chatId) {
    return [];
  }

  const tokens = buildSearchTokens(query).slice(0, 5);
  if (!tokens.length) {
    return getRecentConversation(chatId, limit);
  }

  const clauses = [];
  const params = [chatId];
  for (const token of tokens) {
    const likeValue = `%${token}%`;
    clauses.push(`
      LOWER(COALESCE(message_text, '')) LIKE ?
      OR LOWER(COALESCE(chat_title, '')) LIKE ?
      OR LOWER(COALESCE(sender_name, '')) LIKE ?
      OR LOWER(COALESCE(sender_username, '')) LIKE ?
    `);
    params.push(likeValue, likeValue, likeValue, likeValue);
  }

  const rows = db.prepare(`
    SELECT *
    FROM conversations
    WHERE chat_id = ?
      AND (${clauses.map((clause) => `(${clause})`).join(' OR ')})
    ORDER BY message_date_iso DESC, id DESC
    LIMIT ?
  `).all(...params, limit);

  return rows.reverse();
}

export function getRecentConversationForSender(senderId, limit = 12) {
  return db.prepare(`
    SELECT *
    FROM conversations
    WHERE sender_id = ?
    ORDER BY message_date_iso DESC, id DESC
    LIMIT ?
  `).all(senderId, limit).reverse();
}

export function getRecentConversationForProject({
  projectTelegramUserId = null,
  projectTelegramUsername = '',
  limit = 12,
} = {}) {
  const chatIds = getProjectConversationChatIds({
    projectTelegramUserId,
    projectTelegramUsername,
  });
  if (!chatIds.length) {
    return [];
  }

  return getRecentConversationForChatIds(chatIds, limit);
}

export function searchProjectConversationSnippets({
  projectTelegramUserId = null,
  projectTelegramUsername = '',
  query = '',
  limit = 12,
} = {}) {
  const chatIds = getProjectConversationChatIds({
    projectTelegramUserId,
    projectTelegramUsername,
  });
  if (!chatIds.length) {
    return [];
  }

  const tokens = buildSearchTokens(query).slice(0, 5);
  if (!tokens.length) {
    return getRecentConversationForChatIds(chatIds, limit);
  }

  const chatPlaceholders = chatIds.map(() => '?').join(', ');
  const clauses = [];
  const params = [...chatIds];

  for (const token of tokens) {
    const likeValue = `%${token}%`;
    clauses.push(`
      LOWER(COALESCE(message_text, '')) LIKE ?
      OR LOWER(COALESCE(chat_title, '')) LIKE ?
      OR LOWER(COALESCE(sender_name, '')) LIKE ?
      OR LOWER(COALESCE(sender_username, '')) LIKE ?
    `);
    params.push(likeValue, likeValue, likeValue, likeValue);
  }

  const rows = db.prepare(`
    SELECT *
    FROM conversations
    WHERE chat_id IN (${chatPlaceholders})
      AND (${clauses.map((clause) => `(${clause})`).join(' OR ')})
    ORDER BY message_date_iso DESC, id DESC
    LIMIT ?
  `).all(...params, limit);

  return rows.reverse();
}

export function searchConversationSnippets(query, limit = 12) {
  const tokens = buildSearchTokens(query).slice(0, 5);
  if (!tokens.length) {
    return [];
  }

  const clauses = [];
  const params = [];
  for (const token of tokens) {
    const likeValue = `%${token}%`;
    clauses.push(`
      LOWER(COALESCE(message_text, '')) LIKE ?
      OR LOWER(COALESCE(chat_title, '')) LIKE ?
      OR LOWER(COALESCE(sender_name, '')) LIKE ?
      OR LOWER(COALESCE(sender_username, '')) LIKE ?
    `);
    params.push(likeValue, likeValue, likeValue, likeValue);
  }

  return db.prepare(`
    SELECT *
    FROM conversations
    WHERE ${clauses.map((clause) => `(${clause})`).join(' OR ')}
    ORDER BY message_date_iso DESC, id DESC
    LIMIT ?
  `).all(...params, limit).reverse();
}

export function getRecentKnowledgeForProject(telegramUserId, limit = 8, telegramUsername = '') {
  const relatedIds = collectRelatedProjectIds({
    telegramUserId,
    telegramUsername,
  });
  if (!relatedIds.length) {
    return [];
  }

  return db.prepare(`
    SELECT *
    FROM knowledge_items
    WHERE related_project_tg_id IN (${relatedIds.map(() => '?').join(', ')})
      AND active = 1
    ORDER BY created_at DESC, id DESC
    LIMIT ?
  `).all(...relatedIds, limit).map((row) => ({
    ...row,
    tags: jsonParse(row.tags_json, []),
  }));
}

export function createSuggestion(record) {
  const createdAt = nowIso();
  const nextReminderAt = addHours(createdAt, FOLLOW_UP_STEP_HOURS[0]);
  const stmt = db.prepare(`
    INSERT INTO suggestions (
      chat_id,
      chat_title,
      chat_link,
      client_message_id,
      client_sender_id,
      client_sender_name,
      client_text,
      ai_response,
      service_angle,
      reason,
      confidence,
      next_reminder_at,
      created_at,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    record.chatId,
    record.chatTitle,
    record.chatLink || '',
    record.clientMessageId,
    record.clientSenderId,
    record.clientSenderName || '',
    record.clientText,
    record.aiResponse,
    record.serviceAngle || '',
    record.reason || '',
    record.confidence || '',
    nextReminderAt,
    createdAt,
    createdAt
  );

  return getSuggestionById(result.lastInsertRowid);
}

export function getSuggestionById(suggestionId) {
  return db.prepare(`SELECT * FROM suggestions WHERE id = ?`).get(suggestionId) || null;
}

export function getPendingSuggestionByClientMessage(chatId, clientMessageId) {
  return db.prepare(`
    SELECT *
    FROM suggestions
    WHERE chat_id = ?
      AND client_message_id = ?
      AND (
        status = 'pending'
        OR actual_reply_text IS NULL
        OR actual_reply_text = ''
      )
    ORDER BY id DESC
    LIMIT 1
  `).get(chatId, clientMessageId) || null;
}

export function recordSuggestionDelivery(record) {
  db.prepare(`
    INSERT OR IGNORE INTO suggestion_deliveries (
      suggestion_id,
      admin_id,
      dm_chat_id,
      dm_message_id,
      delivery_type
    ) VALUES (?, ?, ?, ?, ?)
  `).run(
    record.suggestionId,
    record.adminId,
    record.dmChatId,
    record.dmMessageId,
    record.deliveryType
  );
}

export function getSuggestionDeliveries(suggestionId) {
  return db.prepare(`
    SELECT *
    FROM suggestion_deliveries
    WHERE suggestion_id = ?
    ORDER BY id ASC
  `).all(suggestionId);
}

export function markSuggestionHandled({
  suggestionId,
  actionTakenById = null,
  actionTakenByName = '',
  actionSource = '',
  actualReplyText = '',
}) {
  const handledAt = nowIso();
  db.prepare(`
    UPDATE suggestions
    SET status = 'action_taken',
        actual_reply_text = CASE
          WHEN ? != '' THEN ?
          ELSE actual_reply_text
        END,
        action_taken_by_id = COALESCE(?, action_taken_by_id),
        action_taken_by_name = CASE
          WHEN ? != '' THEN ?
          ELSE action_taken_by_name
        END,
        action_source = CASE
          WHEN ? != '' THEN ?
          ELSE action_source
        END,
        action_taken_at = COALESCE(action_taken_at, ?),
        next_reminder_at = NULL,
        updated_at = ?
    WHERE id = ?
  `).run(
    actualReplyText,
    actualReplyText,
    actionTakenById,
    actionTakenByName,
    actionTakenByName,
    actionSource,
    actionSource,
    handledAt,
    handledAt,
    suggestionId
  );

  return getSuggestionById(suggestionId);
}

export function getDueSuggestionsForReminder(cutoffIso) {
  return db.prepare(`
    SELECT *
    FROM suggestions
    WHERE status = 'pending'
      AND next_reminder_at IS NOT NULL
      AND next_reminder_at <= ?
    ORDER BY next_reminder_at ASC, id ASC
  `).all(cutoffIso);
}

export function markReminderSent(suggestionId, reminderNumber) {
  const reminderSentAt = nowIso();
  const nextReminderAt = reminderNumber >= FOLLOW_UP_STEP_HOURS.length
    ? null
    : addHours(reminderSentAt, FOLLOW_UP_STEP_HOURS[reminderNumber]);

  db.prepare(`
    UPDATE suggestions
    SET reminder_stage = ?,
        last_reminder_at = ?,
        next_reminder_at = ?,
        updated_at = ?
    WHERE id = ?
  `).run(reminderNumber, reminderSentAt, nextReminderAt, reminderSentAt, suggestionId);
}

export function getRecentApprovedReplies(limit = 5) {
  return db.prepare(`
    SELECT *
    FROM suggestions
    WHERE actual_reply_text IS NOT NULL
      AND actual_reply_text != ''
    ORDER BY action_taken_at DESC, id DESC
    LIMIT ?
  `).all(limit);
}

export function searchApprovedReplyExamples(query, { limit = 6, excludeChatId = null } = {}) {
  const tokens = buildSearchTokens(query).slice(0, 5);
  if (!tokens.length) {
    return [];
  }

  const clauses = [];
  const params = [];
  for (const token of tokens) {
    const likeValue = `%${token}%`;
    clauses.push(`
      LOWER(COALESCE(client_text, '')) LIKE ?
      OR LOWER(COALESCE(actual_reply_text, '')) LIKE ?
      OR LOWER(COALESCE(ai_response, '')) LIKE ?
      OR LOWER(COALESCE(chat_title, '')) LIKE ?
    `);
    params.push(likeValue, likeValue, likeValue, likeValue);
  }

  const chatClause = excludeChatId == null ? '' : 'AND chat_id != ?';
  const rows = db.prepare(`
    SELECT *
    FROM suggestions
    WHERE actual_reply_text IS NOT NULL
      AND actual_reply_text != ''
      ${chatClause}
      AND (${clauses.map((clause) => `(${clause})`).join(' OR ')})
    ORDER BY action_taken_at DESC, id DESC
    LIMIT ?
  `).all(...(excludeChatId == null ? params : [excludeChatId, ...params]), limit);

  return rows;
}

export function hasKnowledgeExternalKey(externalKey) {
  const row = db.prepare(`
    SELECT 1
    FROM knowledge_items
    WHERE external_key = ?
    LIMIT 1
  `).get(externalKey);

  return Boolean(row);
}

export function addKnowledgeItem(record) {
  const stmt = db.prepare(`
    INSERT INTO knowledge_items (
      external_key,
      source_type,
      scope,
      title,
      content,
      tags_json,
      embedding_json,
      chat_id,
      related_project_tg_id,
      created_by,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(external_key) DO UPDATE SET
      title = excluded.title,
      content = excluded.content,
      tags_json = excluded.tags_json,
      embedding_json = excluded.embedding_json,
      chat_id = excluded.chat_id,
      related_project_tg_id = excluded.related_project_tg_id,
      created_by = excluded.created_by,
      active = 1
  `);

  stmt.run(
    record.externalKey || null,
    record.sourceType,
    record.scope,
    record.title || '',
    record.content,
    jsonStringify(record.tags || []),
    record.embedding ? jsonStringify(record.embedding) : null,
    record.chatId || null,
    record.relatedProjectTgId || null,
    record.createdBy || null,
    nowIso()
  );
}

export function searchKnowledgeByEmbedding(queryEmbedding, limit = 6) {
  const rows = db.prepare(`
    SELECT *
    FROM knowledge_items
    WHERE active = 1
      AND embedding_json IS NOT NULL
  `).all();

  return rows
    .map((row) => {
      const embedding = jsonParse(row.embedding_json, []);
      if (!embedding.length) {
        return null;
      }

      const score = cosine(queryEmbedding, embedding);
      if (typeof score !== 'number' || Number.isNaN(score)) {
        return null;
      }

      return {
        ...row,
        tags: jsonParse(row.tags_json, []),
        score,
      };
    })
    .filter(Boolean)
    .sort((left, right) => right.score - left.score)
    .slice(0, limit);
}

export function upsertProjectProfile(profile) {
  db.prepare(`
    INSERT INTO project_profiles (
      telegram_user_id,
      telegram_username,
      telegram_contact_value,
      project_name,
      project_status,
      lead_stage,
      overview,
      categories_json,
      targets_json,
      raw_fields_json,
      embedding_json,
      content_hash,
      source_row_number,
      last_form_sync_at,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(telegram_user_id) DO UPDATE SET
      telegram_username = excluded.telegram_username,
      telegram_contact_value = excluded.telegram_contact_value,
      project_name = excluded.project_name,
      project_status = excluded.project_status,
      lead_stage = CASE
        WHEN excluded.lead_stage IS NOT NULL AND excluded.lead_stage != ''
        THEN excluded.lead_stage
        ELSE project_profiles.lead_stage
      END,
      overview = excluded.overview,
      categories_json = excluded.categories_json,
      targets_json = excluded.targets_json,
      raw_fields_json = excluded.raw_fields_json,
      embedding_json = excluded.embedding_json,
      content_hash = excluded.content_hash,
      source_row_number = excluded.source_row_number,
      last_form_sync_at = excluded.last_form_sync_at,
      updated_at = excluded.updated_at
  `).run(
    profile.telegramUserId,
    profile.telegramUsername || '',
    profile.telegramContactValue || '',
    profile.projectName || '',
    profile.projectStatus || '',
    profile.leadStage || null,
    profile.overview,
    jsonStringify(profile.categories || []),
    jsonStringify(profile.targets || []),
    jsonStringify(profile.rawFields || {}),
    profile.embedding ? jsonStringify(profile.embedding) : null,
    profile.contentHash,
    profile.sourceRowNumber || null,
    profile.lastFormSyncAt || nowIso(),
    nowIso()
  );
}

export function getProjectProfileByTelegramId(telegramUserId) {
  const row = db.prepare(`
    SELECT *
    FROM project_profiles
    WHERE telegram_user_id = ?
  `).get(telegramUserId);

  return row ? hydrateProjectProfile(row) : null;
}

export function getProjectProfileByAlias(aliasType, aliasValue) {
  const normalizedAliasValue = normalizeAliasValue(aliasType, aliasValue);
  if (!normalizedAliasValue) {
    return null;
  }

  const row = db.prepare(`
    SELECT p.*
    FROM project_aliases a
    JOIN project_profiles p
      ON p.telegram_user_id = a.canonical_project_id
    WHERE a.alias_type = ?
      AND a.alias_value = ?
      AND a.active = 1
    LIMIT 1
  `).get(aliasType, normalizedAliasValue);

  return row ? hydrateProjectProfile(row) : null;
}

export function getProjectProfileByTelegramUsername(telegramUsername) {
  const normalized = normalizeTelegramUsername(telegramUsername);
  if (!normalized) {
    return null;
  }

  const row = db.prepare(`
    SELECT *
    FROM project_profiles
    WHERE LOWER(COALESCE(telegram_username, '')) = ?
    LIMIT 1
  `).get(normalized);

  return row ? hydrateProjectProfile(row) : null;
}

export function findProjectProfileForTelegramUser({
  telegramUserId = null,
  telegramUsername = '',
  chatId = null,
} = {}) {
  if (telegramUserId) {
    const byId = getProjectProfileByTelegramId(telegramUserId);
    if (byId) {
      return byId;
    }

    const aliasById = getProjectProfileByAlias('telegram_id', telegramUserId);
    if (aliasById) {
      return aliasById;
    }
  }

  const byUsername = getProjectProfileByTelegramUsername(telegramUsername);
  if (byUsername) {
    return byUsername;
  }

  const aliasByUsername = getProjectProfileByAlias('telegram_username', telegramUsername);
  if (aliasByUsername) {
    return aliasByUsername;
  }

  if (chatId) {
    return getProjectProfileByChatId(chatId);
  }

  return null;
}

export function getProjectProfileByChatId(chatId) {
  if (!chatId) {
    return null;
  }

  const row = db.prepare(`
    SELECT p.*
    FROM project_chat_links l
    JOIN project_profiles p
      ON p.telegram_user_id = l.canonical_project_id
    WHERE l.chat_id = ?
      AND l.active = 1
    ORDER BY l.updated_at DESC, l.id DESC
    LIMIT 1
  `).get(chatId);

  return row ? hydrateProjectProfile(row) : null;
}

export function findProjectProfilesByQuery(query, limit = 5) {
  const tokens = buildSearchTokens(query).slice(0, 5);
  if (!tokens.length) {
    return [];
  }

  const clauses = [];
  const params = [];
  for (const token of tokens) {
    const likeValue = `%${token}%`;
    clauses.push(`
      LOWER(COALESCE(project_name, '')) LIKE ?
      OR LOWER(COALESCE(overview, '')) LIKE ?
      OR LOWER(COALESCE(telegram_username, '')) LIKE ?
      OR LOWER(COALESCE(alias_value, '')) LIKE ?
    `);
    params.push(likeValue, likeValue, likeValue, likeValue);
  }

  return db.prepare(`
    SELECT DISTINCT p.*
    FROM project_profiles p
    LEFT JOIN project_aliases a
      ON a.canonical_project_id = p.telegram_user_id
     AND a.active = 1
    WHERE ${clauses.map((clause) => `(${clause})`).join(' OR ')}
    LIMIT ?
  `).all(...params, limit).map(hydrateProjectProfile);
}

export function getLatestProjectChatContext({
  projectTelegramUserId = null,
  projectTelegramUsername = '',
} = {}) {
  const normalizedUsername = normalizeTelegramUsername(projectTelegramUsername);
  const predicates = [];
  const params = [];

  if (projectTelegramUserId) {
    predicates.push('sender_id = ?');
    params.push(projectTelegramUserId);
  }

  if (normalizedUsername) {
    predicates.push(`LOWER(COALESCE(sender_username, '')) = ?`);
    params.push(normalizedUsername);
  }

  if (!predicates.length) {
    return null;
  }

  const row = db.prepare(`
    SELECT *
    FROM conversations
    WHERE sender_role = 'project'
      AND (${predicates.join(' OR ')})
    ORDER BY message_date_iso DESC, id DESC
    LIMIT 1
  `).get(...params);

  return row || null;
}

export function getLatestTeamChatContext(teamMemberId) {
  if (!teamMemberId) {
    return null;
  }

  return db.prepare(`
    SELECT *
    FROM conversations
    WHERE sender_role = 'team'
      AND sender_id = ?
      AND chat_type != 'private'
    ORDER BY message_date_iso DESC, id DESC
    LIMIT 1
  `).get(teamMemberId) || null;
}

export function getLatestSuggestionContextForAdmin(adminId) {
  if (!adminId) {
    return null;
  }

  return db.prepare(`
    SELECT s.*, d.created_at AS delivery_created_at
    FROM suggestion_deliveries d
    JOIN suggestions s
      ON s.id = d.suggestion_id
    WHERE d.admin_id = ?
    ORDER BY d.created_at DESC, d.id DESC
    LIMIT 1
  `).get(adminId) || null;
}

export function searchPartnerProfilesByEmbedding(queryEmbedding, excludeTelegramUserId, limit = 5, targetProfile = null) {
  const rows = db.prepare(`
    SELECT *
    FROM project_profiles
    WHERE telegram_user_id != ?
      AND embedding_json IS NOT NULL
  `).all(excludeTelegramUserId);

  const targetCategories = new Set(targetProfile?.categories || []);
  const targetTargets = new Set(targetProfile?.targets || []);

  return rows
    .map((row) => {
      const profile = hydrateProjectProfile(row);
      const embedding = profile.embedding || [];
      const similarity = cosine(queryEmbedding, embedding);
      if (typeof similarity !== 'number' || Number.isNaN(similarity)) {
        return null;
      }

      const categoryOverlap = profile.categories.filter((item) => targetCategories.has(item)).length;
      const targetOverlap = profile.targets.filter((item) => targetTargets.has(item)).length;
      const score = similarity + (categoryOverlap * 0.08) + (targetOverlap * 0.05);

      return {
        ...profile,
        score,
      };
    })
    .filter(Boolean)
    .sort((left, right) => right.score - left.score)
    .slice(0, limit);
}

export function getConversationsForKnowledgeWindow(startIso, endIso) {
  return db.prepare(`
    SELECT *
    FROM conversations
    WHERE message_date_iso >= ?
      AND message_date_iso < ?
    ORDER BY chat_id ASC, message_date_iso ASC, id ASC
  `).all(startIso, endIso);
}

export function getConversationsByChatIds(chatIds) {
  if (!Array.isArray(chatIds) || !chatIds.length) {
    return [];
  }

  const placeholders = chatIds.map(() => '?').join(', ');
  return db.prepare(`
    SELECT *
    FROM conversations
    WHERE chat_id IN (${placeholders})
    ORDER BY message_date_iso ASC, id ASC
  `).all(...chatIds);
}

export function getUnsyncedConversations(cutoffIso, limit = 500) {
  return db.prepare(`
    SELECT *
    FROM conversations
    WHERE sheet_synced_at IS NULL
      AND created_at <= ?
    ORDER BY id ASC
    LIMIT ?
  `).all(cutoffIso, limit);
}

export function markConversationsSynced(ids, syncedAt = nowIso()) {
  if (!ids.length) {
    return;
  }

  const placeholders = ids.map(() => '?').join(', ');
  db.prepare(`
    UPDATE conversations
    SET sheet_synced_at = ?
    WHERE id IN (${placeholders})
  `).run(syncedAt, ...ids);
}

export function getUnsyncedSuggestions(cutoffIso, limit = 500) {
  return db.prepare(`
    SELECT *
    FROM suggestions
    WHERE sheet_synced_at IS NULL
      AND created_at <= ?
    ORDER BY id ASC
    LIMIT ?
  `).all(cutoffIso, limit);
}

export function markSuggestionsSynced(ids, syncedAt = nowIso()) {
  if (!ids.length) {
    return;
  }

  const placeholders = ids.map(() => '?').join(', ');
  db.prepare(`
    UPDATE suggestions
    SET sheet_synced_at = ?
    WHERE id IN (${placeholders})
  `).run(syncedAt, ...ids);
}

export function getUnsyncedKnowledge(cutoffIso, limit = 500) {
  return db.prepare(`
    SELECT *
    FROM knowledge_items
    WHERE sheet_synced_at IS NULL
      AND created_at <= ?
    ORDER BY id ASC
    LIMIT ?
  `).all(cutoffIso, limit);
}

export function markKnowledgeSynced(ids, syncedAt = nowIso()) {
  if (!ids.length) {
    return;
  }

  const placeholders = ids.map(() => '?').join(', ');
  db.prepare(`
    UPDATE knowledge_items
    SET sheet_synced_at = ?
    WHERE id IN (${placeholders})
  `).run(syncedAt, ...ids);
}

export function getState(key) {
  const row = db.prepare(`SELECT value FROM sync_state WHERE key = ?`).get(key);
  return row?.value ?? null;
}

export function setState(key, value) {
  db.prepare(`
    INSERT INTO sync_state (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `).run(key, value);
}

export function exportKnowledgeSnapshot() {
  return {
    conversations: db.prepare(`
      SELECT *
      FROM conversations
      ORDER BY id ASC
    `).all(),
    suggestions: db.prepare(`
      SELECT *
      FROM suggestions
      ORDER BY id ASC
    `).all(),
    knowledgeItems: db.prepare(`
      SELECT *
      FROM knowledge_items
      ORDER BY id ASC
    `).all(),
    projectProfiles: db.prepare(`
      SELECT *
      FROM project_profiles
      ORDER BY project_name ASC, telegram_user_id ASC
    `).all().map(hydrateProjectProfile),
    projectAliases: db.prepare(`
      SELECT *
      FROM project_aliases
      ORDER BY canonical_project_id ASC, alias_type ASC, alias_value ASC
    `).all(),
    projectChatLinks: db.prepare(`
      SELECT *
      FROM project_chat_links
      ORDER BY canonical_project_id ASC, chat_id ASC
    `).all(),
    suggestionReviews: db.prepare(`
      SELECT *
      FROM suggestion_reviews
      ORDER BY suggestion_id ASC, created_at ASC, id ASC
    `).all(),
    sourceSnapshots: db.prepare(`
      SELECT *
      FROM source_snapshots
      ORDER BY source_type ASC, source_key ASC
    `).all().map(hydrateSourceSnapshot),
  };
}

export function exportHistorySnapshot({
  canonicalProjectId = null,
  chatId = null,
} = {}) {
  let conversations = [];
  let projectProfile = null;
  let linkedChats = [];

  if (canonicalProjectId) {
    projectProfile = getProjectProfileByTelegramId(canonicalProjectId);
    linkedChats = db.prepare(`
      SELECT *
      FROM project_chat_links
      WHERE canonical_project_id = ?
        AND active = 1
      ORDER BY updated_at DESC, id DESC
    `).all(canonicalProjectId);
    const chatIds = getProjectConversationChatIds({
      projectTelegramUserId: projectProfile?.telegram_user_id || canonicalProjectId,
      projectTelegramUsername: projectProfile?.telegram_username || '',
    });
    conversations = getConversationsByChatIds(chatIds);
  } else if (chatId) {
    conversations = getConversationsByChatIds([chatId]);
    projectProfile = getProjectProfileByChatId(chatId);
    linkedChats = db.prepare(`
      SELECT *
      FROM project_chat_links
      WHERE chat_id = ?
        AND active = 1
      ORDER BY updated_at DESC, id DESC
    `).all(chatId);
  } else {
    conversations = db.prepare(`
      SELECT *
      FROM conversations
      ORDER BY message_date_iso ASC, id ASC
    `).all();
  }

  const suggestionChatIds = [...new Set(conversations.map((row) => row.chat_id))];
  const suggestions = suggestionChatIds.length
    ? db.prepare(`
        SELECT *
        FROM suggestions
        WHERE chat_id IN (${suggestionChatIds.map(() => '?').join(', ')})
        ORDER BY created_at ASC, id ASC
      `).all(...suggestionChatIds)
    : [];

  return {
    projectProfile,
    linkedChats,
    conversations,
    suggestions,
  };
}

export function listProjectProfiles() {
  return db.prepare(`
    SELECT *
    FROM project_profiles
    ORDER BY project_name ASC, telegram_user_id ASC
  `).all().map(hydrateProjectProfile);
}

export function listProjectConversationSummaries() {
  return db.prepare(`
    SELECT
      p.telegram_user_id AS canonical_project_id,
      p.project_name,
      l.chat_id,
      COALESCE(l.chat_title, MAX(c.chat_title)) AS chat_title,
      COUNT(c.id) AS message_count,
      MAX(CASE WHEN c.sender_role = 'project' THEN c.message_date_iso END) AS last_project_message_at,
      MAX(CASE WHEN c.sender_role = 'team' THEN c.message_date_iso END) AS last_team_message_at
    FROM project_profiles p
    LEFT JOIN project_chat_links l
      ON l.canonical_project_id = p.telegram_user_id
     AND l.active = 1
    LEFT JOIN conversations c
      ON c.chat_id = l.chat_id
    GROUP BY p.telegram_user_id, p.project_name, l.chat_id, l.chat_title
    HAVING l.chat_id IS NOT NULL
    ORDER BY p.project_name ASC, l.chat_id ASC
  `).all();
}

export function listProjectOpportunityRows(limit = 500) {
  return db.prepare(`
    SELECT
      k.related_project_tg_id AS canonical_project_id,
      p.project_name,
      k.scope,
      k.title,
      k.content,
      k.created_at
    FROM knowledge_items k
    LEFT JOIN project_profiles p
      ON p.telegram_user_id = k.related_project_tg_id
    WHERE k.active = 1
      AND k.scope IN ('partner_opportunity', 'project_need', 'buying_signal', 'service_angle', 'followup_strategy')
    ORDER BY k.created_at DESC, k.id DESC
    LIMIT ?
  `).all(limit);
}

function hydrateProjectProfile(row) {
  return {
    ...row,
    categories: jsonParse(row.categories_json, []),
    targets: jsonParse(row.targets_json, []),
    rawFields: jsonParse(row.raw_fields_json, {}),
    embedding: jsonParse(row.embedding_json, []),
  };
}

function hydrateSourceSnapshot(row) {
  return {
    ...row,
    contentJson: jsonParse(row.content_json, null),
    metadata: jsonParse(row.metadata_json, null),
  };
}

function seedTeamMembers(teamIds) {
  for (const teamId of teamIds || []) {
    addTeamMember(teamId, null);
  }
}

function buildSearchTokens(query) {
  return [...new Set(
    String(query || '')
      .toLowerCase()
      .split(/[^a-z0-9_@]+/)
      .map((token) => token.replace(/^@/, '').trim())
      .filter((token) => token.length >= 3)
  )];
}

function getProjectConversationChatIds({
  projectTelegramUserId = null,
  projectTelegramUsername = '',
} = {}) {
  const linkedChatIds = [];
  const profile = findProjectProfileForTelegramUser({
    telegramUserId: projectTelegramUserId,
    telegramUsername: projectTelegramUsername,
  });

  if (profile) {
    linkedChatIds.push(...db.prepare(`
      SELECT chat_id
      FROM project_chat_links
      WHERE canonical_project_id = ?
        AND active = 1
      ORDER BY updated_at DESC, id DESC
    `).all(profile.telegram_user_id).map((row) => row.chat_id));
  }

  const { predicates, params } = buildProjectSenderMatch({
    projectTelegramUserId,
    projectTelegramUsername,
  });
  if (!predicates.length) {
    return [...new Set(linkedChatIds)];
  }

  const conversationChatIds = db.prepare(`
    SELECT DISTINCT chat_id
    FROM conversations
    WHERE sender_role = 'project'
      AND (${predicates.join(' OR ')})
    ORDER BY chat_id ASC
  `).all(...params).map((row) => row.chat_id);

  return [...new Set([...linkedChatIds, ...conversationChatIds])];
}

function collectRelatedProjectIds({
  telegramUserId = null,
  telegramUsername = '',
} = {}) {
  const ids = [];
  const normalizedUsername = normalizeTelegramUsername(telegramUsername);
  const profile = findProjectProfileForTelegramUser({
    telegramUserId,
    telegramUsername: normalizedUsername,
  });

  if (telegramUserId) {
    ids.push(Number(telegramUserId));
  }

  if (profile?.telegram_user_id) {
    ids.push(Number(profile.telegram_user_id));
    ids.push(...db.prepare(`
      SELECT alias_value
      FROM project_aliases
      WHERE canonical_project_id = ?
        AND alias_type = 'telegram_id'
        AND active = 1
    `).all(profile.telegram_user_id).map((row) => Number(row.alias_value)).filter(Boolean));
  }

  return [...new Set(ids)].filter(Boolean);
}

function getRecentConversationForChatIds(chatIds, limit) {
  const placeholders = chatIds.map(() => '?').join(', ');
  const rows = db.prepare(`
    SELECT *
    FROM conversations
    WHERE chat_id IN (${placeholders})
    ORDER BY message_date_iso DESC, id DESC
    LIMIT ?
  `).all(...chatIds, limit);

  return rows.reverse();
}

function buildProjectSenderMatch({
  projectTelegramUserId = null,
  projectTelegramUsername = '',
} = {}) {
  const normalizedUsername = normalizeTelegramUsername(projectTelegramUsername);
  const predicates = [];
  const params = [];

  if (projectTelegramUserId && projectTelegramUserId > 0) {
    predicates.push('sender_id = ?');
    params.push(projectTelegramUserId);
  }

  if (normalizedUsername) {
    predicates.push(`LOWER(COALESCE(sender_username, '')) = ?`);
    params.push(normalizedUsername);
  }

  return { predicates, params };
}

function normalizeAliasValue(aliasType, aliasValue) {
  if (aliasValue === null || aliasValue === undefined) {
    return '';
  }

  if (aliasType === 'telegram_username') {
    return normalizeTelegramUsername(aliasValue);
  }

  if (aliasType === 'telegram_id' || aliasType === 'chat_id' || aliasType === 'project_id') {
    const normalized = String(aliasValue).replace(/[^\d-]/g, '').trim();
    return normalized;
  }

  return String(aliasValue).trim().toLowerCase();
}

function ensureColumn(tableName, columnName, columnDefinition) {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all();
  if (columns.some((column) => column.name === columnName)) {
    return;
  }

  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDefinition}`);
}
