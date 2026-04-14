import {
  DAILY_KNOWLEDGE_WINDOW_HOURS,
  DOC_SYNC_INTERVAL_MS,
  KNOWLEDGE_BUILD_INTERVAL_MS,
  MAX_FOLLOW_UPS,
  PROFILE_SYNC_INTERVAL_MS,
  REMINDER_CHECK_INTERVAL_MS,
  SERVICE_DATASET_SYNC_INTERVAL_MS,
  SHEETS_SYNC_INTERVAL_MS,
} from './config.js';
import {
  addKnowledgeItem,
  findProjectProfileForTelegramUser,
  getDueAnnouncementReminders,
  getConversationsForKnowledgeWindow,
  getDueSuggestionsForReminder,
  getState,
  markAnnouncementReminderSent,
  markReminderSent,
  setState,
} from './db.js';
import { embedText } from './embed.js';
import { notifyTeamAboutAnnouncement, notifyTeamAboutSuggestion } from './notifications.js';
import { importKnowledgeDocs } from './rag.js';
import {
  exportPendingDataToSheets,
  importServiceDatasetFromSheet,
  syncProjectProfilesFromSheet,
} from './sheets.js';
import { nowIso, sha256, subtractHours } from './utils.js';
import { extractConversationKnowledge, formatConversationKnowledgeSummary } from './ai.js';

export async function runStartupTasks(bot) {
  await safeRun('profile sync', () => syncProjectProfilesFromSheet());
  await safeRun('service dataset import', () => importServiceDatasetFromSheet());
  await safeRun('doc import', () => importKnowledgeDocs());
  await safeRun('sheet export', () => exportPendingDataToSheets());
  await safeRun('reminder check', () => sendDueReminders(bot));
  await safeRun('daily knowledge', () => buildDailyKnowledge());
}

export function startSchedulers(bot) {
  setInterval(() => safeRun('profile sync', () => syncProjectProfilesFromSheet()), PROFILE_SYNC_INTERVAL_MS);
  setInterval(() => safeRun('service dataset import', () => importServiceDatasetFromSheet()), SERVICE_DATASET_SYNC_INTERVAL_MS);
  setInterval(() => safeRun('sheet export', () => exportPendingDataToSheets()), SHEETS_SYNC_INTERVAL_MS);
  setInterval(() => safeRun('reminder check', () => sendDueReminders(bot)), REMINDER_CHECK_INTERVAL_MS);
  setInterval(() => safeRun('daily knowledge', () => buildDailyKnowledge()), KNOWLEDGE_BUILD_INTERVAL_MS);
  setInterval(() => safeRun('doc import', () => importKnowledgeDocs()), DOC_SYNC_INTERVAL_MS);
}

async function sendDueReminders(bot) {
  const dueSuggestions = getDueSuggestionsForReminder(nowIso());
  for (const suggestion of dueSuggestions) {
    const reminderNumber = suggestion.reminder_stage + 1;
    await notifyTeamAboutSuggestion(bot.telegram, suggestion, 'reminder', reminderNumber);
    markReminderSent(suggestion.id, reminderNumber >= MAX_FOLLOW_UPS ? MAX_FOLLOW_UPS : reminderNumber);
  }

  const dueAnnouncements = getDueAnnouncementReminders(nowIso());
  for (const reminder of dueAnnouncements) {
    await notifyTeamAboutAnnouncement(bot.telegram, reminder);
    markAnnouncementReminderSent(reminder.id);
  }
}

export async function buildDailyKnowledge({ force = false } = {}) {
  const lastRunIso = getState('daily_knowledge_last_run');
  const now = new Date();

  if (!force && lastRunIso) {
    const hoursSinceRun = (now.getTime() - new Date(lastRunIso).getTime()) / (60 * 60 * 1000);
    if (hoursSinceRun < DAILY_KNOWLEDGE_WINDOW_HOURS) {
      return {
        skipped: true,
        reason: 'window_not_elapsed',
        chatsProcessed: 0,
        knowledgeItemsCreated: 0,
      };
    }
  }

  const endIso = now.toISOString();
  const startIso = (!force && lastRunIso) || subtractHours(now, DAILY_KNOWLEDGE_WINDOW_HOURS);
  const conversations = getConversationsForKnowledgeWindow(startIso, endIso);
  let chatsProcessed = 0;
  let knowledgeItemsCreated = 0;

  const byChat = new Map();
  for (const row of conversations) {
    if (!byChat.has(row.chat_id)) {
      byChat.set(row.chat_id, []);
    }
    byChat.get(row.chat_id).push(row);
  }

  for (const [chatId, rows] of byChat.entries()) {
    const transcript = rows
      .map((row) => `${row.sender_role === 'team' ? 'Collably' : 'Project'}: ${row.message_text}`)
      .join('\n');

    if (!transcript.trim()) {
      continue;
    }

    const projectParticipant = rows.find((row) => row.sender_role === 'project');
    const projectProfile = projectParticipant
      ? findProjectProfileForTelegramUser({
          telegramUserId: projectParticipant.sender_id,
          telegramUsername: projectParticipant.sender_username || '',
        })
      : null;
    const knowledge = await extractConversationKnowledge({
      chatTitle: rows[0].chat_title,
      transcript,
      projectProfile,
    });
    const dayKey = endIso.slice(0, 10);
    const knowledgeEntries = [
      {
        scope: 'conversation_summary',
        sourceType: 'daily_summary',
        title: `${rows[0].chat_title} ${dayKey}`,
        content: formatConversationKnowledgeSummary(knowledge),
      },
      ...knowledge.projectNeeds.map((content) => ({
        scope: 'project_need',
        sourceType: 'daily_extraction',
        title: `${rows[0].chat_title} project need`,
        content,
      })),
      ...knowledge.buyingSignals.map((content) => ({
        scope: 'buying_signal',
        sourceType: 'daily_extraction',
        title: `${rows[0].chat_title} buying signal`,
        content,
      })),
      ...knowledge.objections.map((content) => ({
        scope: 'objection',
        sourceType: 'daily_extraction',
        title: `${rows[0].chat_title} objection`,
        content,
      })),
      ...knowledge.serviceAngles.map((content) => ({
        scope: 'service_angle',
        sourceType: 'daily_extraction',
        title: `${rows[0].chat_title} service angle`,
        content,
      })),
      ...knowledge.partnerIdeas.map((content) => ({
        scope: 'partner_opportunity',
        sourceType: 'daily_extraction',
        title: `${rows[0].chat_title} partner idea`,
        content,
      })),
      ...knowledge.faqCandidates.map((content) => ({
        scope: 'faq_candidate',
        sourceType: 'daily_extraction',
        title: `${rows[0].chat_title} FAQ candidate`,
        content,
      })),
      ...knowledge.nextActions.map((content) => ({
        scope: 'followup_strategy',
        sourceType: 'daily_extraction',
        title: `${rows[0].chat_title} next action`,
        content,
      })),
    ];

    for (const entry of knowledgeEntries) {
      const embedding = await embedText(entry.content);
      addKnowledgeItem({
        externalKey: `daily-kb:${chatId}:${dayKey}:${entry.scope}:${sha256(entry.content)}`,
        sourceType: entry.sourceType,
        scope: entry.scope,
        title: entry.title,
        content: entry.content,
        tags: ['daily-knowledge', entry.scope, dayKey],
        embedding,
        chatId,
        relatedProjectTgId: projectParticipant?.sender_id || null,
      });
      knowledgeItemsCreated += 1;
    }

    chatsProcessed += 1;
  }

  setState('daily_knowledge_last_run', endIso);
  return {
    skipped: false,
    reason: '',
    chatsProcessed,
    knowledgeItemsCreated,
  };
}

async function safeRun(label, fn) {
  try {
    await fn();
  } catch (error) {
    console.error(`Scheduled task failed (${label}):`, error.message);
  }
}
