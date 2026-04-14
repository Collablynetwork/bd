import { RESERVED_PRIVATE_TEXTS } from './commands.js';
import { ANNOUNCEMENT_REMINDER_LEAD_MINUTES, BOT_NAME } from './config.js';
import {
  addKnowledgeItem,
  createSuggestion,
  findProjectProfilesByQuery,
  getProjectProfileByAlias,
  getProjectProfileByTelegramId,
  getProjectProfileByTelegramUsername,
  getApplicableInstructions,
  getPendingSuggestionByClientMessage,
  getRecentConversationForProject,
  getRecentKnowledgeForProject,
  getRecentApprovedReplies,
  getRecentConversation,
  searchProjectConversationSnippets,
  searchConversationSnippets,
  isTeamMember,
  linkProjectChat,
  markSuggestionHandled,
  upsertAnnouncementReminder,
  upsertProjectAlias,
  upsertConversation,
} from './db.js';
import { embedText } from './embed.js';
import { notifyTeamAboutSuggestion, markSuggestionCardsHandled } from './notifications.js';
import { getProjectProfileForUser, getRelevantKnowledge } from './rag.js';
import { buildChatLink, displayName, normalizeTelegramUsername, parseLocalDateTimeInput, sha256 } from './utils.js';
import { generateBDSuggestion, generateTeamResearchAnswer } from './ai.js';

export async function handleIncomingMessage(ctx) {
  const message = ctx.message;
  const text = extractMessageContent(message);

  if (!text || message.from?.is_bot) {
    return;
  }

  if (message.text?.startsWith('/')) {
    return;
  }

  const senderId = Number(message.from.id);
  const senderIsTeamMember = isTeamMember(senderId);

  if (ctx.chat.type === 'private') {
    if (senderIsTeamMember && !RESERVED_PRIVATE_TEXTS.has(text)) {
      queueBackgroundTask('team private question', () => handleTeamPrivateQuestion({
        telegram: ctx.telegram,
        chatId: ctx.chat.id,
        operatorQuestion: text,
      }));
    }
    return;
  }

  const chatTitle = ctx.chat.title || ctx.chat.username || `Chat_${ctx.chat.id}`;

  upsertConversation({
    chatId: ctx.chat.id,
    chatTitle,
    chatType: ctx.chat.type,
    telegramMessageId: message.message_id,
    replyToMessageId: message.reply_to_message?.message_id || null,
    senderId,
    senderName: displayName(message.from),
    senderUsername: message.from.username || '',
    senderRole: senderIsTeamMember ? 'team' : 'project',
    messageText: text,
    messageDateIso: new Date(message.date * 1000).toISOString(),
  });

  if (senderIsTeamMember) {
    queueBackgroundTask('announcement candidate', () => maybeCaptureAnnouncementCandidate({
      senderId,
      senderUsername: ctx.message.from.username || '',
      chatId: ctx.chat.id,
      chatTitle,
      chatLink: buildChatLink(ctx.chat, ctx.message.message_id),
      messageText: text,
    }));
    queueBackgroundTask('capture actual reply', () => maybeCaptureActualReply({
      chatId: ctx.chat.id,
      messageFromId: Number(ctx.message.from.id),
      actorName: displayName(ctx.message.from),
      replyToMessage: ctx.message.reply_to_message,
      actualReplyText: text,
      telegram: ctx.telegram,
    }));
    return;
  }

  queueBackgroundTask('project message', () => handleProjectMessage({
    telegram: ctx.telegram,
    clientId: Number(ctx.message.from.id),
    clientUsername: ctx.message.from.username || '',
    clientName: displayName(ctx.message.from),
    chatId: ctx.chat.id,
    chatTitle,
    chatLink: buildChatLink(ctx.chat, ctx.message.message_id),
    clientMessageId: ctx.message.message_id,
    clientText: text,
  }));
}

async function handleProjectMessage({
  telegram,
  clientId,
  clientUsername,
  clientName,
  chatId,
  chatTitle,
  chatLink,
  clientMessageId,
  clientText,
}) {
  const history = getRecentConversation(chatId, 30);
  const priorHistory = history.filter((entry) => entry.telegram_message_id !== clientMessageId);
  const priorTeamMessages = priorHistory.filter((entry) => entry.sender_role === 'team').length;
  const priorProjectMessages = priorHistory.filter((entry) => entry.sender_role === 'project').length;
  const isNewConversation = priorTeamMessages === 0 && priorProjectMessages <= 1;
  const projectProfile = getProjectProfileForUser({
    telegramUserId: clientId,
    telegramUsername: clientUsername,
    chatId,
  });
  if (projectProfile) {
    linkProjectIdentity({
      projectProfile,
      chatId,
      chatTitle,
      senderId: clientId,
      senderUsername: clientUsername,
    });
  }
  const projectConversationMemory = dedupeConversationSnippets([
    ...searchProjectConversationSnippets({
      projectTelegramUserId: projectProfile?.telegram_user_id || clientId,
      projectTelegramUsername: projectProfile?.telegram_username || clientUsername,
      query: clientText,
      limit: 12,
    }),
    ...getRecentConversationForProject({
      projectTelegramUserId: projectProfile?.telegram_user_id || clientId,
      projectTelegramUsername: projectProfile?.telegram_username || clientUsername,
      limit: 18,
    }),
  ], 18);
  const projectMemoryProjectId = resolveProjectKnowledgeId(projectProfile, clientId);
  const projectMemory = getRecentKnowledgeForProject(
    projectMemoryProjectId,
    10,
    projectProfile?.telegram_username || clientUsername
  );
  const { knowledge } = await getRelevantKnowledge(clientText, 6);
  const approvedExamples = getRecentApprovedReplies(5);
  const operatorInstructions = getApplicableInstructions({
    projectTelegramUserId: projectProfile?.telegram_user_id || clientId,
    projectTelegramUsername: projectProfile?.telegram_username || clientUsername,
  });
  const suggestionPayload = await generateBDSuggestion({
    clientMessage: clientText,
    history,
    isNewConversation,
    projectProfile,
    projectMemory,
    projectConversationMemory,
    knowledge,
    approvedExamples,
    operatorInstructions,
  });

  const suggestion = createSuggestion({
    chatId,
    chatTitle,
    chatLink,
    clientMessageId,
    clientSenderId: clientId,
    clientSenderName: clientName,
    clientText,
    aiResponse: suggestionPayload.reply,
    serviceAngle: suggestionPayload.serviceAngle,
    reason: suggestionPayload.reason,
    confidence: suggestionPayload.confidence,
  });

  await maybeCaptureAnnouncementCandidate({
    senderId: clientId,
    senderUsername: clientUsername,
    chatId,
    chatTitle,
    chatLink,
    messageText: clientText,
    projectProfile,
  });
  await notifyTeamAboutSuggestion(telegram, suggestion);
}

async function handleTeamPrivateQuestion({
  telegram,
  chatId,
  operatorQuestion,
}) {
  const matchedProfiles = resolveMatchedProfiles(operatorQuestion);
  const primaryProfile = matchedProfiles[0] || null;
  const projectConversationSnippets = primaryProfile ? dedupeConversationSnippets([
    ...searchProjectConversationSnippets({
      projectTelegramUserId: primaryProfile.telegram_user_id,
      projectTelegramUsername: primaryProfile.telegram_username,
      query: operatorQuestion,
      limit: 10,
    }),
    ...getRecentConversationForProject({
      projectTelegramUserId: primaryProfile.telegram_user_id,
      projectTelegramUsername: primaryProfile.telegram_username,
      limit: 12,
    }),
  ], 12) : [];
  const derivedProjectSenderId = projectConversationSnippets.find((entry) => entry.sender_role === 'project')?.sender_id
    || (primaryProfile?.telegram_user_id > 0 ? primaryProfile.telegram_user_id : null);
  const relevantProjectMemory = derivedProjectSenderId
    ? getRecentKnowledgeForProject(
        derivedProjectSenderId,
        8,
        primaryProfile?.telegram_username || ''
      )
    : [];
  const conversationSnippets = dedupeConversationSnippets([
    ...searchConversationSnippets(operatorQuestion, 10),
    ...projectConversationSnippets,
  ], 12);
  const { knowledge } = await getRelevantKnowledge(operatorQuestion, 8);
  const approvedExamples = getRecentApprovedReplies(6);
  const projectInstructions = getApplicableInstructions({
    projectTelegramUserId: primaryProfile?.telegram_user_id || null,
    projectTelegramUsername: primaryProfile?.telegram_username || '',
  });

  const result = await generateTeamResearchAnswer({
    operatorQuestion,
    matchedProfiles,
    projectInstructions,
    conversationSnippets,
    relevantKnowledge: dedupeKnowledgeEntries([
      ...relevantProjectMemory,
      ...knowledge,
    ], 12),
    approvedExamples,
  });

  await telegram.sendMessage(chatId, formatTeamResearchResult(result), {
    disable_web_page_preview: true,
  });
}

async function maybeCaptureActualReply({
  chatId,
  messageFromId,
  actorName,
  replyToMessage: repliedMessage,
  actualReplyText,
  telegram,
}) {
  if (!repliedMessage?.from || isTeamMember(Number(repliedMessage.from.id))) {
    return;
  }

  const suggestion = getPendingSuggestionByClientMessage(chatId, repliedMessage.message_id);
  if (!suggestion) {
    return;
  }

  const updated = markSuggestionHandled({
    suggestionId: suggestion.id,
    actionTakenById: messageFromId,
    actionTakenByName: actorName,
    actionSource: 'group_reply',
    actualReplyText,
  });

  await persistApprovedReplyKnowledge(updated);
  await markSuggestionCardsHandled(telegram, suggestion.id);
}

async function persistApprovedReplyKnowledge(suggestion) {
  if (!suggestion?.actual_reply_text) {
    return;
  }

  const content = [
    `Client: ${suggestion.client_text}`,
    `${BOT_NAME}: ${suggestion.actual_reply_text}`,
  ].join('\n');

  const embedding = await embedText(content);
  addKnowledgeItem({
    externalKey: `approved-reply:${suggestion.id}:${sha256(content)}`,
    sourceType: 'approved_reply',
    scope: 'reply_pattern',
    title: suggestion.chat_title,
    content,
    tags: ['approved-reply'],
    embedding,
    chatId: suggestion.chat_id,
    relatedProjectTgId: suggestion.client_sender_id,
    createdBy: suggestion.action_taken_by_id || null,
  });
}

async function maybeCaptureAnnouncementCandidate({
  senderId,
  senderUsername,
  chatId,
  chatTitle,
  chatLink,
  messageText,
  projectProfile = null,
}) {
  const candidate = extractAnnouncementCandidate(messageText);
  if (!candidate) {
    return;
  }

  const resolvedProject = projectProfile || getProjectProfileForUser({
    telegramUserId: senderId,
    telegramUsername: senderUsername,
    chatId,
  });
  if (!resolvedProject) {
    return;
  }

  const remindAt = new Date(new Date(candidate.announcementAt).getTime() - (ANNOUNCEMENT_REMINDER_LEAD_MINUTES * 60 * 1000)).toISOString();
  if (new Date(remindAt).getTime() <= Date.now()) {
    return;
  }

  const projectReference = buildProjectReference(resolvedProject);
  if (!projectReference) {
    return;
  }

  upsertAnnouncementReminder({
    externalKey: `auto-announcement:${chatId}:${candidate.announcementAt}:${sha256(candidate.announcementText)}`,
    projectReference: projectReference.reference,
    projectLabel: projectReference.label,
    chatId,
    chatTitle,
    chatLink,
    announcementText: candidate.announcementText,
    announcementAt: candidate.announcementAt,
    remindAt,
    createdBy: null,
  });
}

function queueBackgroundTask(label, task) {
  setTimeout(() => {
    Promise.resolve()
      .then(task)
      .catch((error) => {
        console.error(`Background task failed (${label}):`, error);
      });
  }, 0);
}

function dedupeConversationSnippets(items, limit) {
  const seen = new Set();
  const output = [];

  for (const item of items) {
    const key = `${item.chat_id}:${item.telegram_message_id}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(item);
    if (output.length >= limit) {
      break;
    }
  }

  return output;
}

function dedupeKnowledgeEntries(items, limit) {
  const seen = new Set();
  const output = [];

  for (const item of items) {
    const key = item.external_key || `${item.scope}:${item.content}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(item);
    if (output.length >= limit) {
      break;
    }
  }

  return output;
}

function formatTeamResearchResult(result) {
  const lines = [
    result.answer,
  ];

  if (result.matchedProjects?.length) {
    lines.push('');
    lines.push(`Matched projects: ${result.matchedProjects.join(', ')}`);
  }

  if (result.replyOptions?.length) {
    lines.push('');
    lines.push('Reply options:');
    result.replyOptions.forEach((option, index) => {
      lines.push(`${index + 1}. ${option}`);
    });
  }

  if (result.confidence) {
    lines.push('');
    lines.push(`Confidence: ${result.confidence}`);
  }

  return lines.join('\n');
}

function resolveMatchedProfiles(operatorQuestion) {
  const matchedProfiles = [];
  const seen = new Set();

  const numericIdMatches = String(operatorQuestion || '').match(/\b\d{5,}\b/g) || [];
  for (const value of numericIdMatches) {
    const profile = getProjectProfileByTelegramId(Number(value)) || getProjectProfileByAlias('telegram_id', value);
    if (profile) {
      pushProfile(profile);
    }
  }

  const usernameMatches = String(operatorQuestion || '').match(/@[A-Za-z][A-Za-z0-9_]{3,31}/g) || [];
  for (const value of usernameMatches) {
    const profile = getProjectProfileByTelegramUsername(normalizeTelegramUsername(value))
      || getProjectProfileByAlias('telegram_username', value);
    if (profile) {
      pushProfile(profile);
    }
  }

  for (const profile of findProjectProfilesByQuery(operatorQuestion, 3)) {
    pushProfile(profile);
  }

  return matchedProfiles.slice(0, 3);

  function pushProfile(profile) {
    const key = `${profile.telegram_user_id}:${profile.project_name || ''}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    matchedProfiles.push(profile);
  }
}

function extractMessageContent(message) {
  const text = String(message?.text || message?.caption || '').trim();
  const attachmentSummary = buildAttachmentSummary(message);
  return [text, attachmentSummary].filter(Boolean).join('\n').trim();
}

function buildAttachmentSummary(message) {
  const parts = [];

  if (message?.document) {
    parts.push(`[Attachment: document${message.document.file_name ? ` ${message.document.file_name}` : ''}]`);
  }
  if (message?.photo?.length) {
    parts.push('[Attachment: photo]');
  }
  if (message?.video) {
    parts.push(`[Attachment: video${message.video.file_name ? ` ${message.video.file_name}` : ''}]`);
  }
  if (message?.audio) {
    parts.push(`[Attachment: audio${message.audio.file_name ? ` ${message.audio.file_name}` : ''}]`);
  }
  if (message?.voice) {
    parts.push('[Attachment: voice]');
  }
  if (message?.animation) {
    parts.push(`[Attachment: animation${message.animation.file_name ? ` ${message.animation.file_name}` : ''}]`);
  }
  if (message?.sticker) {
    parts.push(`[Attachment: sticker ${message.sticker.emoji || ''}]`.trim());
  }

  return parts.join(' ');
}

function extractAnnouncementCandidate(messageText) {
  const text = String(messageText || '').trim();
  if (!text || !/(announcement|announce|ama|launch|listing|event)/i.test(text)) {
    return null;
  }

  const match = text.match(/(\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{1,2}:\d{2}|\d{2}[-/]\d{2}[-/]\d{4}[ T]\d{1,2}:\d{2})/);
  if (!match) {
    return null;
  }

  const announcementAt = parseLocalDateTimeInput(match[1]);
  if (!announcementAt || new Date(announcementAt).getTime() <= Date.now()) {
    return null;
  }

  return {
    announcementAt,
    announcementText: compactAnnouncementText(text),
  };
}

function compactAnnouncementText(text) {
  return String(text || '').replace(/\s+/g, ' ').trim().slice(0, 300);
}

function linkProjectIdentity({
  projectProfile,
  chatId,
  chatTitle,
  senderId,
  senderUsername,
}) {
  if (!projectProfile) {
    return;
  }

  linkProjectChat({
    canonicalProjectId: projectProfile.telegram_user_id,
    chatId,
    chatTitle,
  });

  if (senderId > 0) {
    upsertProjectAlias({
      canonicalProjectId: projectProfile.telegram_user_id,
      aliasType: 'telegram_id',
      aliasValue: senderId,
      sourceType: 'runtime',
    });
  }

  if (senderUsername) {
    upsertProjectAlias({
      canonicalProjectId: projectProfile.telegram_user_id,
      aliasType: 'telegram_username',
      aliasValue: senderUsername,
      sourceType: 'runtime',
    });
  }
}

function resolveProjectKnowledgeId(projectProfile, fallbackSenderId) {
  if (projectProfile?.telegram_user_id) {
    return projectProfile.telegram_user_id;
  }

  return fallbackSenderId;
}

function buildProjectReference(projectProfile) {
  if (!projectProfile) {
    return null;
  }

  if (projectProfile.telegram_user_id > 0) {
    return {
      reference: `telegram_id:${projectProfile.telegram_user_id}`,
      label: projectProfile.project_name || `Telegram ID ${projectProfile.telegram_user_id}`,
    };
  }

  if (projectProfile.telegram_username) {
    return {
      reference: `telegram_username:${projectProfile.telegram_username}`,
      label: projectProfile.project_name || `@${projectProfile.telegram_username}`,
    };
  }

  return null;
}
