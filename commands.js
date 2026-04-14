import fs from 'node:fs';
import path from 'node:path';
import {
  ANNOUNCEMENT_REMINDER_LEAD_MINUTES,
  BOT_NAME,
} from './config.js';
import {
  addKnowledgeItem,
  addOperatorInstruction,
  addSuggestionReview,
  addTeamMember,
  deactivateKnowledgeItem,
  exportKnowledgeSnapshot,
  findProjectProfilesByQuery,
  getDbPath,
  getProjectProfileByAlias,
  getLatestProjectChatContext,
  getKnowledgeItemById,
  getProjectProfileByTelegramId,
  getProjectProfileByTelegramUsername,
  getRecentConversationForProject,
  getSuggestionById,
  getTeamMemberIds,
  isTeamMember,
  listOperatorInstructions,
  listSuggestionReviews,
  listUpcomingAnnouncementReminders,
  removeOperatorInstruction,
  searchKnowledgeByText,
  updateProjectLeadStage,
  upsertProjectAlias,
  upsertAnnouncementReminder,
  linkProjectChat,
} from './db.js';
import { embedText } from './embed.js';
import { generatePartnerRecommendations, generateProjectStatusAdvice } from './ai.js';
import { writeHistoryExportFiles } from './history.js';
import { findPartnerCandidates } from './rag.js';
import { buildDailyKnowledge } from './scheduler.js';
import {
  exportPendingDataToSheets,
  importServiceDatasetFromSheet,
  syncProjectProfilesFromSheet,
} from './sheets.js';
import {
  buildChatLinkFromParts,
  extractTags,
  formatLocalDateTime,
  extractNumericTelegramId,
  isNumeric,
  normalizeTelegramUsername,
  normalizeCommandPayload,
  nowIso,
  parseLocalDateTimeInput,
  sha256,
} from './utils.js';

const BOT_COMMANDS = [
  { command: 'menu', description: 'Open the Collably team menu' },
  { command: 'help', description: 'Show commands and usage' },
  { command: 'addknowledge', description: 'Save manual knowledge' },
  { command: 'addinstruction', description: 'Add a do/do-not instruction' },
  { command: 'instructions', description: 'List active instructions' },
  { command: 'removeinstruction', description: 'Disable an instruction by ID' },
  { command: 'addannouncement', description: 'Set a 30-minute announcement reminder' },
  { command: 'announcements', description: 'List upcoming announcement reminders' },
  { command: 'importhistoryhelp', description: 'Show historical chat import options' },
  { command: 'exporthistory', description: 'Export stored historical chat data' },
  { command: 'setleadstage', description: 'Set lead stage for a project' },
  { command: 'leadstage', description: 'Show lead stage for a project' },
  { command: 'linkproject', description: 'Link a project to user/chat aliases' },
  { command: 'reviewdraft', description: 'Review an AI draft as good or bad' },
  { command: 'draftreviews', description: 'Show draft reviews for a suggestion' },
  { command: 'findknowledge', description: 'Find stored knowledge by text' },
  { command: 'disableknowledge', description: 'Disable a knowledge item by ID' },
  { command: 'exportknowledge', description: 'Download knowledge export and SQLite DB' },
  { command: 'findpartners', description: 'Find ideal partners for a project' },
  { command: 'projectstatus', description: 'Show BD guidance for a project' },
  { command: 'buildknowledge', description: 'Build daily knowledge from conversations' },
  { command: 'trainknowledge', description: 'Alias for buildknowledge' },
  { command: 'addcollablyteam', description: 'Add a Collably team Telegram ID' },
  { command: 'syncsheets', description: 'Export pending SQLite data to Sheets' },
  { command: 'refreshprofiles', description: 'Reload project profiles from Sheets' },
  { command: 'refreshservices', description: 'Reload the Collably service dataset from Sheets' },
  { command: 'hidemenu', description: 'Hide the private menu keyboard' },
];

const MENU_LABELS = {
  help: 'Help',
  buildKnowledge: 'Build Knowledge',
  exportKnowledge: 'Export Knowledge',
  syncSheets: 'Sync Sheets',
  refreshProfiles: 'Refresh Profiles',
  refreshServices: 'Refresh Services',
  addKnowledge: 'Add Knowledge',
  addInstruction: 'Add Instruction',
  listInstructions: 'Instructions',
  addAnnouncement: 'Add Announcement',
  announcements: 'Announcements',
  findPartners: 'Find Partners',
  projectStatus: 'Project Status',
  addTeam: 'Add Team Member',
  hideMenu: 'Hide Menu',
};

export const RESERVED_PRIVATE_TEXTS = new Set(Object.values(MENU_LABELS));

export function registerCommands(bot) {
  bot.command('menu', handleMenu);
  bot.command('start', handleHelp);
  bot.command('help', handleHelp);
  bot.command('addknowledge', handleAddKnowledge);
  bot.command('addinstruction', handleAddInstruction);
  bot.command('instructions', handleInstructions);
  bot.command('removeinstruction', handleRemoveInstruction);
  bot.command('addannouncement', handleAddAnnouncement);
  bot.command('announcements', handleAnnouncements);
  bot.command('importhistoryhelp', handleImportHistoryHelp);
  bot.command('exporthistory', handleExportHistory);
  bot.command('setleadstage', handleSetLeadStage);
  bot.command('leadstage', handleLeadStage);
  bot.command('linkproject', handleLinkProject);
  bot.command('reviewdraft', handleReviewDraft);
  bot.command('draftreviews', handleDraftReviews);
  bot.command('findknowledge', handleFindKnowledge);
  bot.command('disableknowledge', handleDisableKnowledge);
  bot.command('exportknowledge', handleExportKnowledge);
  bot.command('downloadknowledge', handleExportKnowledge);
  bot.command('findpartners', handleFindPartners);
  bot.command('projectstatus', handleProjectStatus);
  bot.command('syncsheets', handleSyncSheets);
  bot.command('refreshprofiles', handleRefreshProfiles);
  bot.command('refreshservices', handleRefreshServices);
  bot.command('buildknowledge', handleBuildKnowledge);
  bot.command('trainknowledge', handleBuildKnowledge);
  bot.command('addcollablyteam', handleAddCollablyTeam);
  bot.command('hidemenu', handleHideMenu);

  bot.hears(MENU_LABELS.help, handleHelp);
  bot.hears(MENU_LABELS.buildKnowledge, handleBuildKnowledge);
  bot.hears(MENU_LABELS.exportKnowledge, handleExportKnowledge);
  bot.hears(MENU_LABELS.syncSheets, handleSyncSheets);
  bot.hears(MENU_LABELS.refreshProfiles, handleRefreshProfiles);
  bot.hears(MENU_LABELS.refreshServices, handleRefreshServices);
  bot.hears(MENU_LABELS.addKnowledge, handleAddKnowledgeUsage);
  bot.hears(MENU_LABELS.addInstruction, handleAddInstructionUsage);
  bot.hears(MENU_LABELS.listInstructions, handleInstructions);
  bot.hears(MENU_LABELS.addAnnouncement, handleAddAnnouncementUsage);
  bot.hears(MENU_LABELS.announcements, handleAnnouncements);
  bot.hears(MENU_LABELS.findPartners, handleFindPartnersUsage);
  bot.hears(MENU_LABELS.projectStatus, handleProjectStatusUsage);
  bot.hears(MENU_LABELS.addTeam, handleAddTeamUsage);
  bot.hears(MENU_LABELS.hideMenu, handleHideMenu);
}

export async function setupTelegramCommands(bot) {
  try {
    await bot.telegram.setMyCommands(BOT_COMMANDS, {
      scope: { type: 'all_private_chats' },
    });
  } catch (error) {
    console.error('Failed to register Telegram bot commands:', error.message);
  }
}

async function handleHelp(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply(buildHelpText(), buildMenuMarkup());
}

async function handleMenu(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply('Collably team menu is active.', buildMenuMarkup());
}

async function handleHideMenu(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply('Menu hidden.', {
    reply_markup: {
      remove_keyboard: true,
    },
  });
}

async function handleAddKnowledgeUsage(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply('Usage: /addknowledge title | content\nYou can also reply to a message and run /addknowledge.', buildMenuMarkup());
}

async function handleAddInstructionUsage(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply([
    'Usage:',
    '/addinstruction do | <instruction>',
    '/addinstruction notdo | <instruction>',
    '/addinstruction <project name or telegram id or @username> | do | <instruction>',
  ].join('\n'), buildMenuMarkup());
}

async function handleAddAnnouncementUsage(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply(
    'Usage: /addannouncement <project name or telegram id or @username> | YYYY-MM-DD HH:mm | <announcement text>',
    buildMenuMarkup()
  );
}

async function handleImportHistoryHelp(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply([
    'Historical chat import options:',
    '1. Telegram Desktop JSON export',
    '2. Generic JSON array of messages',
    '3. NDJSON / JSONL',
    '4. CSV logs with columns like chat_title, sender_name, sender_username, sender_role, message_text, date',
    '',
    'CLI examples:',
    'npm run import:history -- --file "/path/export.json" --project "Circle Layer"',
    'npm run import:history -- --file "/path/log.csv" --project "@t_uttopassa" --chat-title "Circle Layer Group"',
  ].join('\n'), buildMenuMarkup());
}

async function handleFindPartnersUsage(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply('Usage: /findpartners <project name or telegram user id or @username>', buildMenuMarkup());
}

async function handleProjectStatusUsage(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply('Usage: /projectstatus <project name or telegram user id or @username>', buildMenuMarkup());
}

async function handleAddTeamUsage(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  await ctx.reply('Usage: /addcollablyteam <telegram user id>', buildMenuMarkup());
}

function buildHelpText() {
  return [
    `${BOT_NAME} BD agent commands:`,
    '/menu',
    '/addknowledge title | content',
    '/addinstruction do | instruction',
    '/addinstruction <project> | do | instruction',
    '/instructions',
    '/removeinstruction <instruction id>',
    `/addannouncement <project> | YYYY-MM-DD HH:mm | text (${ANNOUNCEMENT_REMINDER_LEAD_MINUTES} min reminder)`,
    '/announcements',
    '/importhistoryhelp',
    '/exporthistory <all|project|@username|chat:<id>>',
    '/setleadstage <project> | <stage>',
    '/leadstage <project>',
    '/linkproject <project> | <telegram id or @username or chat:<id>>',
    '/reviewdraft <suggestion id> | good|bad | note',
    '/draftreviews <suggestion id>',
    '/findknowledge <query>',
    '/disableknowledge <knowledge id>',
    '/exportknowledge',
    '/findpartners <project name or telegram id>',
    '/projectstatus <project name or telegram id>',
    '/buildknowledge',
    '/trainknowledge',
    '/addcollablyteam <telegram user id>',
    '/syncsheets',
    '/refreshprofiles',
    '/refreshservices',
    '/hidemenu',
  ].join('\n');
}

function buildMenuMarkup() {
  return {
    reply_markup: {
      keyboard: [
        [MENU_LABELS.help, MENU_LABELS.buildKnowledge],
        [MENU_LABELS.exportKnowledge, MENU_LABELS.syncSheets],
        [MENU_LABELS.refreshProfiles, MENU_LABELS.refreshServices],
        [MENU_LABELS.addKnowledge, MENU_LABELS.addInstruction],
        [MENU_LABELS.listInstructions, MENU_LABELS.addAnnouncement],
        [MENU_LABELS.announcements, MENU_LABELS.findPartners],
        [MENU_LABELS.projectStatus, MENU_LABELS.addTeam],
        [MENU_LABELS.hideMenu],
      ],
      resize_keyboard: true,
      is_persistent: true,
      one_time_keyboard: false,
    },
  };
}

async function handleAddKnowledge(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const payload = normalizeCommandPayload(ctx.message.text);
  const repliedText = ctx.message.reply_to_message?.text?.trim() || '';
  const [rawTitle, rawContent] = payload.includes('|')
    ? payload.split('|', 2).map((item) => item.trim())
    : ['', payload];

  const title = rawTitle || 'Manual knowledge';
  const content = (rawContent || repliedText || '').trim();
  if (!content) {
    await ctx.reply('Usage: /addknowledge title | content\nYou can also reply to a message and run /addknowledge.');
    return;
  }

  const embedding = await embedText(content);
  addKnowledgeItem({
    externalKey: `manual:${ctx.from.id}:${sha256(`${title}:${content}`)}`,
    sourceType: 'manual',
    scope: 'manual',
    title,
    content,
    tags: extractTags(content),
    embedding,
    createdBy: ctx.from.id,
  });

  await ctx.reply('Knowledge saved.');
}

async function handleAddInstruction(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const payload = normalizeCommandPayload(ctx.message.text);
  const parsed = parseInstructionPayload(payload);
  if (!parsed) {
    await ctx.reply([
      'Usage:',
      '/addinstruction do | <instruction>',
      '/addinstruction notdo | <instruction>',
      '/addinstruction <project name or telegram id or @username> | do | <instruction>',
    ].join('\n'));
    return;
  }

  if (!parsed.content) {
    await ctx.reply('Instruction content cannot be empty.');
    return;
  }

  const target = resolveInstructionTarget(parsed.targetQuery);
  if (!target) {
    await ctx.reply('Instruction target not found. Use a synced project name, a Telegram user id, or an @username.');
    return;
  }

  const instruction = addOperatorInstruction({
    scope: target.scope,
    targetValue: target.targetValue,
    targetLabel: target.targetLabel,
    instructionType: parsed.instructionType,
    content: parsed.content,
    createdBy: Number(ctx.from.id),
  });

  await ctx.reply([
    `Instruction saved: #${instruction.id}`,
    `Scope: ${target.targetLabel}`,
    `${parsed.instructionType === 'do' ? 'Do' : 'Do not'}: ${parsed.content}`,
  ].join('\n'));
}

async function handleInstructions(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const instructions = listOperatorInstructions(50);
  if (!instructions.length) {
    await ctx.reply('No active instructions.');
    return;
  }

  await ctx.reply(instructions.map((instruction) => {
    const scopeLabel = instruction.scope === 'global'
      ? 'Global'
      : (instruction.target_label || instruction.target_value || 'Project');
    return `#${instruction.id} [${scopeLabel}] ${instruction.instruction_type === 'do' ? 'Do' : 'Do not'}: ${instruction.content}`;
  }).join('\n'));
}

async function handleRemoveInstruction(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const instructionId = Number(normalizeCommandPayload(ctx.message.text));
  if (!instructionId) {
    await ctx.reply('Usage: /removeinstruction <instruction id>');
    return;
  }

  const removed = removeOperatorInstruction(instructionId);
  await ctx.reply(removed ? `Instruction removed: #${instructionId}` : 'Instruction not found.');
}

async function handleAddAnnouncement(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const payload = normalizeCommandPayload(ctx.message.text);
  const parts = payload.split('|').map((item) => item.trim()).filter(Boolean);
  if (parts.length < 3) {
    await ctx.reply('Usage: /addannouncement <project name or telegram id or @username> | YYYY-MM-DD HH:mm | <announcement text>');
    return;
  }

  const [targetQuery, whenRaw, ...textParts] = parts;
  const announcementText = textParts.join(' | ').trim();
  const announcementAt = parseLocalDateTimeInput(whenRaw);
  if (!announcementAt || !announcementText) {
    await ctx.reply('Use a valid local date/time like 2026-04-20 18:30 and include the announcement text.');
    return;
  }

  if (new Date(announcementAt).getTime() <= Date.now()) {
    await ctx.reply('Announcement time must be in the future.');
    return;
  }

  const target = resolveProjectReference(targetQuery);
  if (!target) {
    await ctx.reply('Project not found. Use a synced project name, a Telegram user id, or an @username.');
    return;
  }

  const remindAt = new Date(new Date(announcementAt).getTime() - (ANNOUNCEMENT_REMINDER_LEAD_MINUTES * 60 * 1000)).toISOString();
  if (new Date(remindAt).getTime() <= Date.now()) {
    await ctx.reply(`Announcement time must be at least ${ANNOUNCEMENT_REMINDER_LEAD_MINUTES} minutes from now.`);
    return;
  }

  const latestChat = getLatestProjectChatContext({
    projectTelegramUserId: target.projectTelegramUserId,
    projectTelegramUsername: target.projectTelegramUsername,
  });
  const reminder = upsertAnnouncementReminder({
    externalKey: `manual-announcement:${target.projectReference}:${announcementAt}:${sha256(announcementText)}`,
    projectReference: target.projectReference,
    projectLabel: target.projectLabel,
    chatId: latestChat?.chat_id || null,
    chatTitle: latestChat?.chat_title || target.projectLabel,
    chatLink: latestChat
      ? buildChatLinkFromParts(latestChat.chat_id, '', latestChat.telegram_message_id)
      : '',
    announcementText,
    announcementAt,
    remindAt,
    createdBy: Number(ctx.from.id),
  });

  await ctx.reply([
    `Announcement reminder saved: #${reminder.id}`,
    `Project: ${target.projectLabel}`,
    `Announcement time: ${formatLocalDateTime(announcementAt)}`,
    `Reminder time: ${formatLocalDateTime(remindAt)}`,
  ].join('\n'));
}

async function handleAnnouncements(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const reminders = listUpcomingAnnouncementReminders(20);
  if (!reminders.length) {
    await ctx.reply('No upcoming announcement reminders.');
    return;
  }

  await ctx.reply(reminders.map((reminder) => [
    `#${reminder.id} ${reminder.project_label || 'Project'}`,
    `${formatLocalDateTime(reminder.announcement_at)}: ${reminder.announcement_text}`,
  ].join('\n')).join('\n\n'));
}

async function handleExportHistory(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const query = normalizeCommandPayload(ctx.message.text) || 'all';
  const target = resolveHistoryExportTarget(query);
  if (!target) {
    await ctx.reply('Usage: /exporthistory <all|project name|telegram id|@username|chat:<id>>');
    return;
  }

  const result = writeHistoryExportFiles({
    outputDir: path.join(process.cwd(), 'exports'),
    canonicalProjectId: target.canonicalProjectId,
    chatId: target.chatId,
    label: target.label,
  });

  await ctx.replyWithDocument({ source: result.jsonPath, filename: path.basename(result.jsonPath) });
  await ctx.replyWithDocument({ source: result.csvPath, filename: path.basename(result.csvPath) });
  await ctx.reply(`History export complete.\nConversations: ${result.conversationCount}\nSuggestions: ${result.suggestionCount}`);
}

async function handleSetLeadStage(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const payload = normalizeCommandPayload(ctx.message.text);
  const parts = payload.split('|').map((item) => item.trim()).filter(Boolean);
  if (parts.length < 2) {
    await ctx.reply('Usage: /setleadstage <project> | <stage>');
    return;
  }

  const profile = resolveProfile(parts[0]);
  if (!profile) {
    await ctx.reply('Project not found. Use a project name, Telegram user id, or @username.');
    return;
  }

  const updated = updateProjectLeadStage(profile.telegram_user_id, parts.slice(1).join(' | '));
  await ctx.reply([
    `Lead stage updated for ${updated?.project_name || profile.project_name || profile.telegram_user_id}`,
    `Lead stage: ${updated?.lead_stage || parts.slice(1).join(' | ')}`,
  ].join('\n'));
}

async function handleLeadStage(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const query = normalizeCommandPayload(ctx.message.text);
  if (!query) {
    await ctx.reply('Usage: /leadstage <project>');
    return;
  }

  const profile = resolveProfile(query);
  if (!profile) {
    await ctx.reply('Project not found. Use a project name, Telegram user id, or @username.');
    return;
  }

  await ctx.reply([
    `Project: ${profile.project_name || profile.telegram_user_id}`,
    `Lead stage: ${profile.lead_stage || 'Not set'}`,
  ].join('\n'));
}

async function handleLinkProject(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const payload = normalizeCommandPayload(ctx.message.text);
  const parts = payload.split('|').map((item) => item.trim()).filter(Boolean);
  if (parts.length < 2) {
    await ctx.reply('Usage: /linkproject <project> | <telegram id or @username or chat:<id>>');
    return;
  }

  const profile = resolveProfile(parts[0]);
  if (!profile) {
    await ctx.reply('Project not found. Use a project name, Telegram user id, or @username.');
    return;
  }

  const alias = resolveAliasDescriptor(parts[1]);
  if (!alias) {
    await ctx.reply('Alias not understood. Use a Telegram user id, @username, or chat:<id>.');
    return;
  }

  if (alias.type === 'chat_id') {
    linkProjectChat({
      canonicalProjectId: profile.telegram_user_id,
      chatId: Number(alias.value),
      chatTitle: profile.project_name || '',
    });
  } else {
    upsertProjectAlias({
      canonicalProjectId: profile.telegram_user_id,
      aliasType: alias.type,
      aliasValue: alias.value,
      sourceType: 'manual_link',
    });
  }

  await ctx.reply(`Project link saved for ${profile.project_name || profile.telegram_user_id}: ${parts[1]}`);
}

async function handleReviewDraft(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const payload = normalizeCommandPayload(ctx.message.text);
  const parts = payload.split('|').map((item) => item.trim()).filter(Boolean);
  if (parts.length < 2) {
    await ctx.reply('Usage: /reviewdraft <suggestion id> | good|bad | optional note');
    return;
  }

  const suggestionId = Number(parts[0]);
  const verdict = normalizeReviewVerdict(parts[1]);
  if (!suggestionId || !verdict) {
    await ctx.reply('Usage: /reviewdraft <suggestion id> | good|bad | optional note');
    return;
  }

  const suggestion = getSuggestionById(suggestionId);
  if (!suggestion) {
    await ctx.reply('Suggestion not found.');
    return;
  }

  const review = addSuggestionReview({
    suggestionId,
    reviewerId: Number(ctx.from.id),
    verdict,
    note: parts.slice(2).join(' | '),
  });

  await ctx.reply([
    `Draft review saved: #${review.id}`,
    `Suggestion: #${suggestionId}`,
    `Verdict: ${verdict}`,
  ].join('\n'));
}

async function handleDraftReviews(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const suggestionId = Number(normalizeCommandPayload(ctx.message.text));
  if (!suggestionId) {
    await ctx.reply('Usage: /draftreviews <suggestion id>');
    return;
  }

  const reviews = listSuggestionReviews(suggestionId);
  if (!reviews.length) {
    await ctx.reply('No draft reviews found for that suggestion.');
    return;
  }

  await ctx.reply(reviews.map((review) => [
    `#${review.id} ${review.verdict}`,
    review.note || 'No note',
  ].join('\n')).join('\n\n'));
}

async function handleFindKnowledge(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const query = normalizeCommandPayload(ctx.message.text);
  if (!query) {
    await ctx.reply('Usage: /findknowledge <query>');
    return;
  }

  const matches = searchKnowledgeByText(query, 10);
  if (!matches.length) {
    await ctx.reply('No active knowledge matches found.');
    return;
  }

  await ctx.reply(matches.map((item) => [
    `#${item.id} [${item.scope}] ${item.title || 'Untitled'}`,
    compactPreview(item.content, 180),
  ].join('\n')).join('\n\n'));
}

async function handleDisableKnowledge(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const knowledgeId = Number(normalizeCommandPayload(ctx.message.text));
  if (!knowledgeId) {
    await ctx.reply('Usage: /disableknowledge <knowledge id>');
    return;
  }

  const existing = getKnowledgeItemById(knowledgeId);
  if (!existing) {
    await ctx.reply('Knowledge item not found.');
    return;
  }

  const removed = deactivateKnowledgeItem(knowledgeId);
  await ctx.reply(removed
    ? `Knowledge disabled: #${knowledgeId} (${existing.title || existing.scope})`
    : 'Knowledge item was already disabled.');
}

async function handleExportKnowledge(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const snapshot = exportKnowledgeSnapshot();
  const exportDir = path.join(process.cwd(), 'exports');
  fs.mkdirSync(exportDir, { recursive: true });

  const timestamp = nowIso().replace(/[:.]/g, '-');
  const jsonPath = path.join(exportDir, `knowledge-export-${timestamp}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(snapshot, null, 2));

  await ctx.replyWithDocument({ source: jsonPath, filename: path.basename(jsonPath) });
  await ctx.replyWithDocument({ source: getDbPath(), filename: path.basename(getDbPath()) });
}

async function handleFindPartners(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const query = normalizeCommandPayload(ctx.message.text);
  const targetProfile = resolveProfile(query);
  if (!targetProfile) {
    await ctx.reply('Project not found. Use a project name, Telegram user id, or @username.');
    return;
  }

  const candidates = await findPartnerCandidates(targetProfile, 5);
  if (!candidates.length) {
    await ctx.reply('No strong partner candidates found in the current project database.');
    return;
  }

  const recommendation = await generatePartnerRecommendations({
    targetProfile,
    candidates,
  });

  await ctx.reply(recommendation);
}

async function handleProjectStatus(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const query = normalizeCommandPayload(ctx.message.text);
  const targetProfile = resolveProfile(query);
  if (!targetProfile) {
    await ctx.reply('Project not found. Use a project name, Telegram user id, or @username.');
    return;
  }

  const recentConversation = getRecentConversationForProject({
    projectTelegramUserId: targetProfile.telegram_user_id,
    projectTelegramUsername: targetProfile.telegram_username,
    limit: 12,
  });
  const advice = await generateProjectStatusAdvice({
    projectProfile: targetProfile,
    recentConversation,
  });

  await ctx.reply(advice);
}

async function handleSyncSheets(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const result = await exportPendingDataToSheets();
  await ctx.reply(`Sheet export complete.\nConversations: ${result.conversations}\nSuggestions: ${result.suggestions}\nKnowledge: ${result.knowledgeItems}`);
}

async function handleRefreshProfiles(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const synced = await syncProjectProfilesFromSheet();
  await ctx.reply(`Project profiles refreshed: ${synced}`);
}

async function handleRefreshServices(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const result = await importServiceDatasetFromSheet();
  if (!result.enabled) {
    await ctx.reply('Service dataset import is disabled. Set SERVICE_DATASET_SHEET_ID in .env first.');
    return;
  }

  await ctx.reply(`Service dataset refreshed.\nSheets processed: ${result.sheetsProcessed}\nEntries imported or updated: ${result.imported}`);
}

async function handleBuildKnowledge(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const result = await buildDailyKnowledge({ force: true });
  await ctx.reply([
    'Daily knowledge build complete.',
    `Chats processed: ${result.chatsProcessed}`,
    `Knowledge entries updated: ${result.knowledgeItemsCreated}`,
  ].join('\n'));
}

async function handleAddCollablyTeam(ctx) {
  if (!ensureTeamPrivate(ctx)) {
    return;
  }

  const payload = normalizeCommandPayload(ctx.message.text);
  const telegramUserId = Number(String(payload || '').replace(/[^\d]/g, ''));
  if (!telegramUserId) {
    await ctx.reply('Usage: /addcollablyteam <telegram user id>');
    return;
  }

  addTeamMember(telegramUserId, Number(ctx.from.id));
  const members = getTeamMemberIds();
  await ctx.reply([
    `Collably team member added: ${telegramUserId}`,
    `Active team IDs: ${members.join(', ')}`,
  ].join('\n'));
}

function ensureTeamPrivate(ctx) {
  const isTeam = isTeamMember(Number(ctx.from?.id));
  const isPrivate = ctx.chat?.type === 'private';

  if (!isTeam || !isPrivate) {
    if (ctx.chat?.type !== 'private') {
      ctx.reply('Use this command in private chat with the bot.');
    }
    return false;
  }

  return true;
}

function resolveProfile(query) {
  const value = String(query || '').trim();
  if (!value) {
    return null;
  }

  if (isNumeric(value)) {
    return getProjectProfileByTelegramId(Number(value)) || getProjectProfileByAlias('telegram_id', value);
  }

  const normalizedUsername = normalizeTelegramUsername(value);
  if (normalizedUsername) {
    return getProjectProfileByTelegramUsername(normalizedUsername) || getProjectProfileByAlias('telegram_username', normalizedUsername);
  }

  return findProjectProfilesByQuery(value, 1)[0] || null;
}

function parseInstructionPayload(payload) {
  const parts = String(payload || '').split('|').map((item) => item.trim()).filter(Boolean);
  if (parts.length < 2) {
    return null;
  }

  if (parts.length === 2) {
    const instructionType = normalizeInstructionType(parts[0]);
    if (!instructionType) {
      return null;
    }

    return {
      targetQuery: '',
      instructionType,
      content: parts[1],
    };
  }

  const instructionType = normalizeInstructionType(parts[1]);
  if (!instructionType) {
    return null;
  }

  return {
    targetQuery: parts[0],
    instructionType,
    content: parts.slice(2).join(' | '),
  };
}

function normalizeInstructionType(value = '') {
  const normalized = String(value || '').trim().toLowerCase();
  if (['do', 'required', 'must'].includes(normalized)) {
    return 'do';
  }

  if (['notdo', 'dont', "don't", 'do not', 'avoid'].includes(normalized)) {
    return 'notdo';
  }

  return '';
}

function resolveInstructionTarget(targetQuery) {
  const value = String(targetQuery || '').trim();
  if (!value) {
    return {
      scope: 'global',
      targetValue: null,
      targetLabel: 'Global',
    };
  }

  const target = resolveProjectReference(value);
  if (!target) {
    return null;
  }

  return {
    scope: 'project',
    targetValue: target.projectReference,
    targetLabel: target.projectLabel,
  };
}

function resolveProjectReference(query) {
  const value = String(query || '').trim();
  const profile = resolveProfile(value);
  if (profile) {
    return buildProjectReferenceFromProfile(profile);
  }

  if (isNumeric(value)) {
    const telegramUserId = Number(value);
    return {
      projectReference: `telegram_id:${telegramUserId}`,
      projectLabel: `Telegram ID ${telegramUserId}`,
      projectTelegramUserId: telegramUserId,
      projectTelegramUsername: '',
    };
  }

  const normalizedUsername = normalizeTelegramUsername(value);
  if (normalizedUsername) {
    return {
      projectReference: `telegram_username:${normalizedUsername}`,
      projectLabel: `@${normalizedUsername}`,
      projectTelegramUserId: null,
      projectTelegramUsername: normalizedUsername,
    };
  }

  return null;
}

function buildProjectReferenceFromProfile(profile) {
  const numericId = Number(profile.telegram_user_id);
  if (numericId > 0) {
    return {
      projectReference: `telegram_id:${numericId}`,
      projectLabel: profile.project_name || `Telegram ID ${numericId}`,
      projectTelegramUserId: numericId,
      projectTelegramUsername: profile.telegram_username || '',
    };
  }

  if (profile.telegram_username) {
    return {
      projectReference: `telegram_username:${profile.telegram_username}`,
      projectLabel: profile.project_name || `@${profile.telegram_username}`,
      projectTelegramUserId: null,
      projectTelegramUsername: profile.telegram_username,
    };
  }

  return null;
}

function resolveHistoryExportTarget(query) {
  const value = String(query || '').trim();
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
      return null;
    }

    return {
      canonicalProjectId: null,
      chatId,
      label: `chat-${chatId}`,
    };
  }

  const profile = resolveProfile(value);
  if (!profile) {
    return null;
  }

  return {
    canonicalProjectId: profile.telegram_user_id,
    chatId: null,
    label: profile.project_name || profile.telegram_username || profile.telegram_user_id,
  };
}

function resolveAliasDescriptor(value) {
  const text = String(value || '').trim();
  if (!text) {
    return null;
  }

  if (text.toLowerCase().startsWith('chat:')) {
    const chatId = extractNumericTelegramId(text.slice(5));
    return chatId ? { type: 'chat_id', value: String(chatId) } : null;
  }

  const username = normalizeTelegramUsername(text);
  if (username) {
    return { type: 'telegram_username', value: username };
  }

  const telegramId = extractNumericTelegramId(text);
  if (telegramId) {
    return { type: 'telegram_id', value: String(telegramId) };
  }

  return null;
}

function normalizeReviewVerdict(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (['good', 'useful', 'correct'].includes(normalized)) {
    return 'good';
  }

  if (['bad', 'wrong', 'poor'].includes(normalized)) {
    return 'bad';
  }

  return '';
}

function compactPreview(value, maxLength) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength - 3).trim()}...`;
}
