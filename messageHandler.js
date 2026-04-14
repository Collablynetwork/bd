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
  getLatestProjectChatContext,
  getLatestSuggestionContextForAdmin,
  getLatestTeamChatContext,
  getPendingSuggestionByClientMessage,
  getRecentConversationForProject,
  getRecentKnowledgeForProject,
  getRecentApprovedReplies,
  getRecentConversation,
  searchApprovedReplyExamples,
  searchProjectConversationSnippets,
  searchConversationSnippetsByChat,
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
import { findPartnerCandidates, getProjectProfileForUser, getRelevantKnowledge } from './rag.js';
import * as sheetsApi from './sheets.js';
import { buildChatLink, displayName, normalizeTelegramUsername, parseLocalDateTimeInput, sha256 } from './utils.js';
import {
  extractAnnouncementReminderCandidate,
  generateBDSuggestion,
  generatePartnerRecommendations,
  generateTeamResearchAnswer,
} from './ai.js';

export async function handleIncomingMessage(ctx) {
  const message = ctx.message || ctx.channelPost || ctx.update?.channel_post || ctx.update?.edited_message || ctx.update?.edited_channel_post;
  const text = extractMessageContent(message);

  if (!message || !text || message.from?.is_bot) {
    return;
  }

  if (message.text?.startsWith('/')) {
    return;
  }

  const sender = resolveSenderContext(message);
  if (!sender.senderId) {
    return;
  }
  const senderId = sender.senderId;
  const senderIsTeamMember = sender.isTeamMember;
  const messageId = message.message_id;

  if (ctx.chat.type === 'private') {
    if (senderIsTeamMember && !RESERVED_PRIVATE_TEXTS.has(text)) {
      queueBackgroundTask('team private question', () => handleTeamPrivateQuestion({
        telegram: ctx.telegram,
        chatId: ctx.chat.id,
        operatorId: senderId,
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
    senderName: sender.senderName,
    senderUsername: sender.senderUsername,
    senderRole: senderIsTeamMember ? 'team' : 'project',
    messageText: text,
    messageDateIso: new Date(message.date * 1000).toISOString(),
  });

  if (senderIsTeamMember) {
    queueBackgroundTask('announcement candidate', () => maybeCaptureAnnouncementCandidate({
      senderId,
      senderUsername: sender.senderUsername,
      chatId: ctx.chat.id,
      chatTitle,
      chatLink: buildChatLink(ctx.chat, messageId),
      messageText: text,
    }));
    queueBackgroundTask('capture actual reply', () => maybeCaptureActualReply({
      chatId: ctx.chat.id,
      messageFromId: senderId,
      actorName: sender.senderName,
      replyToMessage: message.reply_to_message,
      actualReplyText: text,
      telegram: ctx.telegram,
    }));
    return;
  }

  queueBackgroundTask('project message', () => handleProjectMessage({
    telegram: ctx.telegram,
    clientId: senderId,
    clientUsername: sender.senderUsername,
    clientName: sender.senderName,
    chatId: ctx.chat.id,
    chatTitle,
    chatLink: buildChatLink(ctx.chat, messageId),
    clientMessageId: messageId,
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
  let projectProfile = getProjectProfileForUser({
    telegramUserId: clientId,
    telegramUsername: clientUsername,
    chatId,
  });
  projectProfile = await maybeRefreshProjectProfile({
    projectProfile,
    telegramUserId: clientId,
    telegramUsername: clientUsername,
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
    ...searchConversationSnippetsByChat(chatId, clientText, 12),
    ...getRecentConversation(chatId, 80),
  ], 18);
  const projectMemoryProjectId = resolveProjectKnowledgeId(projectProfile, clientId);
  const projectMemory = filterKnowledgeEntriesForChat(
    getRecentKnowledgeForProject(
      projectMemoryProjectId,
      20,
      projectProfile?.telegram_username || clientUsername
    ),
    chatId,
    10
  );
  const { knowledge: matchedKnowledge } = await getRelevantKnowledge(clientText, 20);
  const knowledge = filterKnowledgeEntriesForChat(matchedKnowledge, chatId, 6);
  const approvedExamples = getRecentApprovedReplies(20)
    .filter((entry) => Number(entry.chat_id) === Number(chatId))
    .slice(0, 5);
  const generalReplyPatterns = searchApprovedReplyExamples(clientText, {
    limit: 6,
    excludeChatId: chatId,
  });
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
    generalReplyPatterns,
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
  operatorId,
  operatorQuestion,
}) {
  const matchedProfiles = resolveMatchedProfiles(operatorQuestion);
  const inferredContext = inferOperatorProjectContext(operatorId);
  const explicitPrimaryProfile = matchedProfiles[0] || null;
  const primaryProfile = explicitPrimaryProfile || inferredContext?.projectProfile || null;
  const resolvedMatchedProfiles = primaryProfile
    ? dedupeProfiles([primaryProfile, ...matchedProfiles], 3)
    : matchedProfiles;
  const focusChatContext = resolveFocusChatContext({
    primaryProfile,
    inferredContext,
  });

  const projectConversationSnippets = primaryProfile
    ? dedupeConversationSnippets(
        focusChatContext?.chatId
          ? [
              ...searchConversationSnippetsByChat(focusChatContext.chatId, operatorQuestion, 10),
              ...getRecentConversation(focusChatContext.chatId, 24),
            ]
          : [
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
            ],
        12
      )
    : [];
  const derivedProjectSenderId = projectConversationSnippets.find((entry) => entry.sender_role === 'project')?.sender_id
    || (primaryProfile?.telegram_user_id > 0 ? primaryProfile.telegram_user_id : null);
  const relevantProjectMemory = derivedProjectSenderId
    ? filterKnowledgeEntriesForChat(
        getRecentKnowledgeForProject(
          derivedProjectSenderId,
          20,
          primaryProfile?.telegram_username || ''
        ),
        focusChatContext?.chatId || null,
        8
      )
    : [];
  const conversationSnippets = dedupeConversationSnippets([
    ...searchConversationSnippets(operatorQuestion, 10),
    ...projectConversationSnippets,
  ], 12);
  const { knowledge } = await getRelevantKnowledge(operatorQuestion, 8);
  const filteredKnowledge = focusChatContext?.chatId
    ? filterKnowledgeEntriesForChat(knowledge, focusChatContext.chatId, 8)
    : knowledge;
  const explicitApprovedExamples = focusChatContext?.chatId
    ? getRecentApprovedReplies(20)
        .filter((entry) => Number(entry.chat_id) === Number(focusChatContext.chatId))
        .slice(0, 6)
    : [];
  const generalReplyPatterns = searchApprovedReplyExamples(operatorQuestion, {
    limit: 6,
    excludeChatId: focusChatContext?.chatId || null,
  });
  const approvedExamples = getRecentApprovedReplies(6);
  const projectInstructions = getApplicableInstructions({
    projectTelegramUserId: primaryProfile?.telegram_user_id || null,
    projectTelegramUsername: primaryProfile?.telegram_username || '',
  });

  if (primaryProfile && /\bpartner|partners|collaborate|collaboration|synergy|synergies\b/i.test(operatorQuestion)) {
    const partnerCandidates = await findPartnerCandidates(primaryProfile, 5);
    if (partnerCandidates.length) {
      const recommendation = await generatePartnerRecommendations({
        targetProfile: primaryProfile,
        candidates: partnerCandidates,
      });

      await telegram.sendMessage(chatId, recommendation, {
        disable_web_page_preview: true,
      });
      return;
    }
  }

  const result = await generateTeamResearchAnswer({
    operatorQuestion,
    matchedProfiles: resolvedMatchedProfiles,
    projectInstructions,
    conversationSnippets,
    relevantKnowledge: dedupeKnowledgeEntries([
      ...relevantProjectMemory,
      ...filteredKnowledge,
    ], 12),
    approvedExamples: dedupeApprovedExamples([
      ...explicitApprovedExamples,
      ...approvedExamples,
    ], 6),
    generalReplyPatterns,
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
  let candidate = extractAnnouncementCandidate(messageText);
  if (!candidate) {
    if (!looksLikeTimelineMessage(messageText)) {
      return;
    }

    const resolvedProject = projectProfile || getProjectProfileForUser({
      telegramUserId: senderId,
      telegramUsername: senderUsername,
      chatId,
    });
    const aiCandidate = await extractAnnouncementReminderCandidate({
      messageText,
      projectProfile: resolvedProject,
    });
    if (aiCandidate.shouldCreateReminder) {
      const parsedDateTime = parseLocalDateTimeInput(aiCandidate.localDateTime);
      if (parsedDateTime) {
        candidate = {
          announcementAt: parsedDateTime,
          announcementText: aiCandidate.reminderText || compactAnnouncementText(messageText),
        };
      }
    }
    if (!candidate) {
      return;
    }
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

function filterKnowledgeEntriesForChat(items, chatId, limit) {
  const seen = new Set();
  const output = [];
  const hasChatFilter = chatId != null && chatId !== '';
  const normalizedChatId = hasChatFilter ? Number(chatId) : null;

  for (const item of items || []) {
    const itemChatId = item?.chat_id == null ? null : Number(item.chat_id);
    if (hasChatFilter && itemChatId !== null && itemChatId !== normalizedChatId) {
      continue;
    }

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

function dedupeApprovedExamples(items, limit) {
  const seen = new Set();
  const output = [];

  for (const item of items || []) {
    const key = item.id || `${item.chat_id}:${item.client_text}:${item.actual_reply_text}`;
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

function dedupeProfiles(items, limit) {
  const seen = new Set();
  const output = [];

  for (const item of items || []) {
    if (!item) {
      continue;
    }

    const key = `${item.telegram_user_id}:${item.project_name || ''}`;
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

function resolveSenderContext(message) {
  const fromUser = message?.from || null;
  const senderChat = message?.sender_chat || null;
  const senderId = Number(fromUser?.id || senderChat?.id || 0);
  const senderName = fromUser
    ? displayName(fromUser)
    : (senderChat?.title || senderChat?.username || `Chat ${senderId}`);
  const senderUsername = fromUser?.username || senderChat?.username || '';

  return {
    senderId,
    senderName,
    senderUsername,
    isTeamMember: fromUser ? isTeamMember(senderId) : false,
  };
}

function inferOperatorProjectContext(operatorId) {
  if (!operatorId) {
    return null;
  }

  const latestTeamChat = getLatestTeamChatContext(operatorId);
  if (latestTeamChat) {
    const projectProfile = getProjectProfileForUser({
      telegramUserId: null,
      telegramUsername: '',
      chatId: latestTeamChat.chat_id,
    });

    if (projectProfile) {
      return {
        projectProfile,
        chatId: latestTeamChat.chat_id,
        chatTitle: latestTeamChat.chat_title,
        source: 'latest_team_group_message',
      };
    }
  }

  const latestSuggestion = getLatestSuggestionContextForAdmin(operatorId);
  if (!latestSuggestion) {
    return null;
  }

  const projectProfile = getProjectProfileForUser({
    telegramUserId: latestSuggestion.client_sender_id,
    telegramUsername: '',
    chatId: latestSuggestion.chat_id,
  });

  if (!projectProfile) {
    return null;
  }

  return {
    projectProfile,
    chatId: latestSuggestion.chat_id,
    chatTitle: latestSuggestion.chat_title,
    source: 'latest_delivered_suggestion',
  };
}

function resolveFocusChatContext({ primaryProfile, inferredContext }) {
  if (!primaryProfile) {
    return null;
  }

  if (inferredContext?.projectProfile?.telegram_user_id === primaryProfile.telegram_user_id) {
    return inferredContext;
  }

  const latestChat = getLatestProjectChatContext({
    projectTelegramUserId: primaryProfile.telegram_user_id,
    projectTelegramUsername: primaryProfile.telegram_username,
  });

  if (!latestChat) {
    return null;
  }

  return {
    projectProfile: primaryProfile,
    chatId: latestChat.chat_id,
    chatTitle: latestChat.chat_title,
    source: 'latest_project_chat',
  };
}

function extractMessageContent(message) {
  const text = String(message?.text || message?.caption || '').trim();
  const attachmentSummary = buildAttachmentSummary(message);
  return [text, attachmentSummary].filter(Boolean).join('\n').trim();
}

async function maybeRefreshProjectProfile({
  projectProfile,
  telegramUserId,
  telegramUsername,
}) {
  try {
    const syncProfile = sheetsApi.syncProjectProfileForTelegramUser;
    if (typeof syncProfile !== 'function') {
      return projectProfile;
    }

    const refreshed = await syncProfile({
      telegramUserId,
      telegramUsername,
      force: !projectProfile,
    });

    return refreshed || projectProfile;
  } catch (error) {
    console.warn('Realtime project profile sync failed:', error.message);
    return projectProfile;
  }
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

function looksLikeTimelineMessage(text) {
  return /\b(announcement|announce|ama|listing|launch|event|timeline|going live|mainnet|airdrop|tomorrow|today|next week|utc|ist|pm|am|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(String(text || ''));
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
