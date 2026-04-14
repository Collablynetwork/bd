import { BOT_NAME, OPENAI_CHAT_MODEL, openaiClient } from './config.js';
import { compactText, nowIso } from './utils.js';

const BASE_SYSTEM_PROMPT = [
  `You are ${BOT_NAME}'s paid Business Development Manager.`,
  'You write replies that Collably team members can send directly in Telegram.',
  'Primary goals: qualify needs, advance the conversation, position paid services, and move good leads toward a commercial next step.',
  'Do not give away complete strategy, implementation steps, or free consulting that should belong inside a paid engagement.',
  'Never overpromise delivery, outcomes, introductions, pricing, or scope unless the provided context supports it.',
  'Keep the reply concise, commercial, relationship-aware, and human.',
  'Default to 1-3 short sentences and keep the reply under 80 words unless a shorter clarification is enough.',
  'Do not use bullet points, long explanations, or multiple asks in one reply.',
  'If the lead is early or vague, ask one focused question instead of giving a long plan.',
  'If Google Form data exists, use it directly and do not ask the project to repeat information already present in the form.',
  'Always protect paid service value.',
].join('\n');

export async function generateBDSuggestion({
  clientMessage,
  history,
  isNewConversation,
  projectProfile,
  projectMemory,
  projectConversationMemory,
  knowledge,
  approvedExamples,
  generalReplyPatterns,
  operatorInstructions,
}) {
  const userPrompt = [
    'Create one reply that Collably should send.',
    isNewConversation
      ? 'This is the start of the conversation. Begin with a simple hi/hello greeting before the business-development reply.'
      : 'This is not the start of the conversation. Do not force a generic hi/hello opening unless it is naturally needed.',
    'Keep reply length tight: maximum 80 words, ideally 35-65 words.',
    'Use the project Google Form data as the primary profile source when it exists.',
    'If the form already shows their project category, target category, event focus, or contact context, use that context directly.',
    'Ask at most one question.',
    '',
    'Project Google Form data:',
    formatProjectProfileContext(projectProfile),
    '',
    'Operator instructions:',
    formatOperatorInstructions(operatorInstructions),
    '',
    'Recent chat history:',
    history.length
      ? history.map((entry) => `${entry.sender_role === 'team' ? 'Collably' : 'Project'}: ${entry.message_text}`).join('\n')
      : 'No prior history.',
    '',
    'Earlier conversation history from this same Telegram group:',
    projectConversationMemory.length
      ? projectConversationMemory.map((entry) => `${entry.sender_role === 'team' ? 'Collably' : 'Project'} [${entry.chat_title}]: ${entry.message_text}`).join('\n')
      : 'No earlier same-group conversation found yet.',
    '',
    'Project-specific memory from earlier discussions:',
    projectMemory.length
      ? projectMemory.map((entry, index) => `${index + 1}. [${entry.scope}] ${entry.content}`).join('\n')
      : 'No project-specific memory yet.',
    '',
    'Relevant knowledge:',
    knowledge.length
      ? knowledge.map((entry, index) => `${index + 1}. [${entry.scope}] ${entry.content}`).join('\n')
      : 'No extra knowledge.',
    '',
    'Approved Collably reply examples:',
    approvedExamples.length
      ? approvedExamples.map((entry, index) => (
        `${index + 1}. Client: ${entry.client_text}\nCollably: ${entry.actual_reply_text}`
      )).join('\n\n')
      : 'No approved reply examples.',
    '',
    'General similar reply patterns from other Collably discussions:',
    generalReplyPatterns.length
      ? generalReplyPatterns.map((entry, index) => (
        `${index + 1}. Client: ${entry.client_text}\nCollably: ${entry.actual_reply_text}`
      )).join('\n\n')
      : 'No broader repeated-discussion pattern found.',
    '',
    'Latest project message:',
    clientMessage,
    '',
    'Use only this group history plus the current project profile to infer requirements, prior objections, and prior asks.',
    'Do not ask the project to repeat information they already provided in earlier conversations unless the context is clearly outdated or conflicting.',
    'You may reuse wording patterns from other approved Collably replies only when the question pattern is similar.',
    'Do not carry over project-specific facts, names, pricing, dates, or promises from another group unless the current project context independently supports them.',
    'Return JSON with keys: reply, reason, serviceAngle, confidence.',
    'reply must be ready to send as-is and must stay short.',
    'reason and serviceAngle should each be a short internal phrase.',
  ].join('\n');

  const result = await callJsonModel([
    { role: 'system', content: BASE_SYSTEM_PROMPT },
    { role: 'user', content: userPrompt },
  ], {
    reply: 'Thanks for sharing. To recommend the right paid support from our side, can you clarify your immediate priority and what outcome you want to achieve first?',
    reason: 'Fallback clarification reply.',
    serviceAngle: 'Discovery and qualification before proposing paid support.',
    confidence: 'low',
  });

  return {
    ...result,
    reply: trimReplyLength(result.reply, 60),
    reason: cleanSentence(result.reason, 'Fallback clarification reply.'),
    serviceAngle: cleanSentence(result.serviceAngle, 'Discovery and qualification before proposing paid support.'),
    confidence: cleanSentence(result.confidence, 'low'),
  };
}

export async function summarizeConversationWindow({ chatTitle, transcript, projectProfile }) {
  const knowledge = await extractConversationKnowledge({ chatTitle, transcript, projectProfile });
  return formatConversationKnowledgeSummary(knowledge);
}

export async function extractConversationKnowledge({ chatTitle, transcript, projectProfile }) {
  const prompt = [
    `You are converting the last 24 hours of Telegram conversation into reusable business-development knowledge for ${BOT_NAME}.`,
    'Focus on project requirements, business goals, urgency, objections, commercial fit, collaboration opportunities, and the next paid move.',
    'Return JSON with keys: summary, projectNeeds, buyingSignals, objections, serviceAngles, partnerIdeas, faqCandidates, nextActions.',
    'Each list should contain short standalone strings with no numbering.',
    '',
    `Chat: ${chatTitle}`,
    '',
    'Project profile:',
    projectProfile?.overview || 'No profile found.',
    '',
    'Transcript:',
    transcript,
  ].join('\n');

  const result = await callJsonModel([
    { role: 'system', content: BASE_SYSTEM_PROMPT },
    { role: 'user', content: prompt },
  ], {
    summary: 'No summary generated.',
    projectNeeds: [],
    buyingSignals: [],
    objections: [],
    serviceAngles: [],
    partnerIdeas: [],
    faqCandidates: [],
    nextActions: [],
  });

  return {
    summary: cleanSentence(result.summary, 'No summary generated.'),
    projectNeeds: normalizeStringArray(result.projectNeeds),
    buyingSignals: normalizeStringArray(result.buyingSignals),
    objections: normalizeStringArray(result.objections),
    serviceAngles: normalizeStringArray(result.serviceAngles),
    partnerIdeas: normalizeStringArray(result.partnerIdeas),
    faqCandidates: normalizeStringArray(result.faqCandidates),
    nextActions: normalizeStringArray(result.nextActions),
  };
}

export function formatConversationKnowledgeSummary(knowledge) {
  return compactText([
    `Summary: ${cleanSentence(knowledge.summary, 'No summary generated.')}`,
    '',
    `Project needs: ${arrayToSentence(knowledge.projectNeeds)}`,
    `Buying signals: ${arrayToSentence(knowledge.buyingSignals)}`,
    `Objections: ${arrayToSentence(knowledge.objections)}`,
    `Service angles: ${arrayToSentence(knowledge.serviceAngles)}`,
    `Partner ideas: ${arrayToSentence(knowledge.partnerIdeas)}`,
    `FAQ candidates: ${arrayToSentence(knowledge.faqCandidates)}`,
    `Next actions: ${arrayToSentence(knowledge.nextActions)}`,
  ].join('\n'));
}

export async function generatePartnerRecommendations({ targetProfile, candidates }) {
  const prompt = [
    `Find the best collaboration matches for the target project in ${BOT_NAME}'s database.`,
    'Shortlist up to three partners. Explain why each match is commercially relevant and what collaboration angle makes sense.',
    'Keep the answer concise and useful for an internal BD operator.',
    '',
    'Target project:',
    targetProfile?.overview || 'Unknown target profile.',
    '',
    'Candidate projects:',
    candidates.map((entry, index) => `${index + 1}. ${entry.project_name || 'Unnamed project'}\n${entry.overview}`).join('\n\n'),
  ].join('\n');

  const result = await callJsonModel([
    { role: 'system', content: BASE_SYSTEM_PROMPT },
    { role: 'user', content: `${prompt}\n\nReturn JSON with keys: summary, partners.` },
  ], {
    summary: 'No strong partner matches found.',
    partners: [],
  });

  const partnerLines = Array.isArray(result.partners)
    ? result.partners.slice(0, 3).map((entry, index) => {
      const name = entry.name || `Partner ${index + 1}`;
      const why = entry.why || 'Relevant based on available profile overlap.';
      const angle = entry.angle || 'Potential collaboration should be validated manually.';
      return `${index + 1}. ${name}\nWhy: ${why}\nAngle: ${angle}`;
    })
    : [];

  return compactText([
    result.summary || 'No strong partner matches found.',
    '',
    ...partnerLines,
  ].join('\n\n'));
}

export async function generateProjectStatusAdvice({ projectProfile, recentConversation }) {
  const prompt = [
    'Assess the project status and recommend business development next steps.',
    'Return JSON with keys: currentStatus, risks, bestMoves, outreachAngle.',
    '',
    'Project profile:',
    projectProfile?.overview || 'No profile found.',
    '',
    'Recent conversation:',
    recentConversation.length
      ? recentConversation.map((entry) => `${entry.sender_role === 'team' ? 'Collably' : 'Project'}: ${entry.message_text}`).join('\n')
      : 'No recent conversation found.',
  ].join('\n');

  const result = await callJsonModel([
    { role: 'system', content: BASE_SYSTEM_PROMPT },
    { role: 'user', content: prompt },
  ], {
    currentStatus: 'Status unclear.',
    risks: ['Need more project context.'],
    bestMoves: ['Qualify their current need before pitching support.'],
    outreachAngle: 'Lead with a short qualification message tied to their immediate priority.',
  });

  return compactText([
    `Status: ${result.currentStatus}`,
    `Risks: ${arrayToSentence(result.risks)}`,
    `Best moves: ${arrayToSentence(result.bestMoves)}`,
    `Outreach angle: ${result.outreachAngle}`,
  ].join('\n'));
}

export async function generateTeamResearchAnswer({
  operatorQuestion,
  matchedProfiles,
  projectInstructions,
  conversationSnippets,
  relevantKnowledge,
  approvedExamples,
  generalReplyPatterns = [],
}) {
  const prompt = [
    `You are ${BOT_NAME}'s internal BD support assistant for the Collably team.`,
    'Answer the operator question by researching stored project profiles, prior conversations, and approved Collably replies.',
    'Keep the answer concise and practical.',
    'Return JSON with keys: answer, replyOptions, matchedProjects, confidence.',
    'answer should be 2-4 short lines for the internal team.',
    'replyOptions should contain 0-3 short Telegram-ready reply drafts.',
    'If the question is not asking for a reply, replyOptions can be empty.',
    '',
    'Operator question:',
    operatorQuestion,
    '',
    'Matched project profiles:',
    matchedProfiles.length
      ? matchedProfiles.map((profile, index) => [
        `${index + 1}. ${profile.project_name || 'Unnamed project'}`,
        profile.telegram_username ? `Telegram username: @${profile.telegram_username}` : '',
        profile.lead_stage ? `Lead stage: ${profile.lead_stage}` : '',
        `Overview: ${profile.overview}`,
      ].filter(Boolean).join('\n')).join('\n\n')
      : 'No direct project profile match.',
    '',
    'Project/operator instructions:',
    projectInstructions.length
      ? projectInstructions.map((instruction) => `${instruction.instruction_type === 'do' ? 'Do' : 'Do not'}: ${instruction.content}`).join('\n')
      : 'No special instructions.',
    '',
    'Relevant conversation snippets:',
    conversationSnippets.length
      ? conversationSnippets.map((entry, index) => (
        `${index + 1}. [${entry.chat_title}] ${entry.sender_role === 'team' ? 'Collably' : 'Project'}: ${entry.message_text}`
      )).join('\n')
      : 'No strong conversation matches.',
    '',
    'Relevant knowledge:',
    relevantKnowledge.length
      ? relevantKnowledge.map((entry, index) => `${index + 1}. [${entry.scope}] ${entry.content}`).join('\n')
      : 'No extra knowledge.',
    '',
    'Approved Collably replies:',
    approvedExamples.length
      ? approvedExamples.map((entry, index) => `${index + 1}. Client: ${entry.client_text}\nCollably: ${entry.actual_reply_text}`).join('\n\n')
      : 'No approved reply examples.',
    '',
    'General similar reply patterns across other project discussions:',
    generalReplyPatterns.length
      ? generalReplyPatterns.map((entry, index) => `${index + 1}. Client: ${entry.client_text}\nCollably: ${entry.actual_reply_text}`).join('\n\n')
      : 'No broader repeated-discussion pattern found.',
    '',
    'Use broader patterns only as style/reference. Do not reuse project-specific facts from another project unless they are also present in the matched profile or current project context.',
  ].join('\n');

  const result = await callJsonModel([
    { role: 'system', content: BASE_SYSTEM_PROMPT },
    { role: 'user', content: prompt },
  ], {
    answer: 'No strong match found in the stored project memory yet. Use a short clarification reply and refresh profiles if needed.',
    replyOptions: [],
    matchedProjects: [],
    confidence: 'low',
  });

  return {
    answer: compactText(String(result.answer || 'No strong match found in the stored project memory yet.')),
    replyOptions: normalizeStringArray(result.replyOptions).slice(0, 3).map((item) => trimReplyLength(item, 60)),
    matchedProjects: normalizeStringArray(result.matchedProjects).slice(0, 3),
    confidence: cleanSentence(result.confidence, 'low'),
  };
}

export async function extractAnnouncementReminderCandidate({
  messageText,
  projectProfile,
}) {
  const prompt = [
    `You extract announcement or timeline reminders for ${BOT_NAME}.`,
    'Only return a reminder when the message clearly contains a future announcement, launch, AMA, listing, event, or important timeline with a date and time.',
    'Return JSON with keys: shouldCreateReminder, localDateTime, reminderText, confidence.',
    'localDateTime must be in YYYY-MM-DD HH:mm format in the operator local timezone.',
    'If the message does not clearly contain a usable future date and time, return shouldCreateReminder false and empty strings.',
    '',
    `Current timestamp: ${nowIso()}`,
    '',
    'Project profile:',
    projectProfile?.overview || 'No profile found.',
    '',
    'Message:',
    messageText,
  ].join('\n');

  const result = await callJsonModel([
    { role: 'system', content: BASE_SYSTEM_PROMPT },
    { role: 'user', content: prompt },
  ], {
    shouldCreateReminder: false,
    localDateTime: '',
    reminderText: '',
    confidence: 'low',
  });

  return {
    shouldCreateReminder: Boolean(result.shouldCreateReminder),
    localDateTime: compactText(String(result.localDateTime || '')),
    reminderText: compactText(String(result.reminderText || '')),
    confidence: cleanSentence(result.confidence, 'low'),
  };
}

async function callJsonModel(messages, fallback) {
  try {
    const response = await openaiClient.chat.completions.create({
      model: OPENAI_CHAT_MODEL,
      temperature: 0.25,
      max_completion_tokens: 260,
      response_format: { type: 'json_object' },
      messages,
    });

    const content = response.choices[0]?.message?.content || '{}';
    return normalizeJsonPayload(content, fallback);
  } catch (error) {
    console.error('AI call failed:', error.message);
    return fallback;
  }
}

function normalizeJsonPayload(content, fallback) {
  try {
    const parsed = JSON.parse(content);
    return { ...fallback, ...parsed };
  } catch {
    return fallback;
  }
}

function arrayToSentence(value) {
  if (!Array.isArray(value) || !value.length) {
    return 'None noted.';
  }

  return value.join('; ');
}

function normalizeStringArray(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return [...new Set(
    value
      .map((item) => cleanSentence(item, ''))
      .filter(Boolean)
  )];
}

function cleanSentence(value, fallback) {
  const cleaned = compactText(String(value || ''))
    .replace(/^[\-\d.\s]+/, '')
    .trim();

  return cleaned || fallback;
}

function trimReplyLength(value, maxWords) {
  const text = compactText(String(value || ''));
  if (!text) {
    return '';
  }

  const words = text.split(/\s+/);
  if (words.length <= maxWords) {
    return text;
  }

  return `${words.slice(0, maxWords).join(' ').replace(/[,:;.-]*$/, '')}...`;
}

function formatProjectProfileContext(projectProfile) {
  if (!projectProfile) {
    return 'No Google Form profile found.';
  }

  const lines = [];
  const rawFields = projectProfile.rawFields || {};
  const prioritizedFields = [
    'Project Name',
    'Project Twitter Link',
    'Telegram Contact ID',
    'Current Status',
    'Stage',
    'Requirement',
    'Requirements',
    'Need',
    'Needs',
  ];

  for (const label of prioritizedFields) {
    if (rawFields[label]) {
      lines.push(`${label}: ${rawFields[label]}`);
    }
  }

  if (projectProfile.categories?.length) {
    lines.push(`Project categories: ${projectProfile.categories.join(', ')}`);
  }

  if (projectProfile.targets?.length) {
    lines.push(`Target categories: ${projectProfile.targets.join(', ')}`);
  }

  const eventFields = Object.entries(rawFields)
    .filter(([key, value]) => value && key.toLowerCase().includes('event'))
    .slice(0, 6)
    .map(([key, value]) => `${key}: ${value}`);

  if (eventFields.length) {
    lines.push(...eventFields);
  }

  if (projectProfile.project_status) {
    lines.push(`Derived status: ${projectProfile.project_status}`);
  }

  if (projectProfile.lead_stage) {
    lines.push(`Lead stage: ${projectProfile.lead_stage}`);
  }

  if (projectProfile.telegram_username) {
    lines.push(`Telegram username from form: @${projectProfile.telegram_username}`);
  }

  if (projectProfile.overview) {
    lines.push(`Overview: ${projectProfile.overview}`);
  }

  return compactText(lines.join('\n')) || 'No usable Google Form data found.';
}

function formatOperatorInstructions(instructions) {
  if (!Array.isArray(instructions) || !instructions.length) {
    return 'No extra operator instructions.';
  }

  const doLines = instructions
    .filter((instruction) => instruction.instruction_type === 'do')
    .map((instruction) => `Do: ${instruction.content}`);
  const notDoLines = instructions
    .filter((instruction) => instruction.instruction_type === 'notdo')
    .map((instruction) => `Do not: ${instruction.content}`);

  return compactText([...doLines, ...notDoLines].join('\n')) || 'No extra operator instructions.';
}
