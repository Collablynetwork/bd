import crypto from 'node:crypto';

export function nowIso() {
  return new Date().toISOString();
}

export function addHours(dateInput, hours) {
  const date = dateInput instanceof Date ? new Date(dateInput) : new Date(dateInput);
  date.setTime(date.getTime() + hours * 60 * 60 * 1000);
  return date.toISOString();
}

export function subtractHours(dateInput, hours) {
  return addHours(dateInput, -hours);
}

export function sha256(value) {
  return crypto.createHash('sha256').update(String(value)).digest('hex');
}

export function jsonStringify(value) {
  return JSON.stringify(value ?? null);
}

export function jsonParse(value, fallback = null) {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

export function escapeHtml(value = '') {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}


export function formatMonospaceBlock(value = '') {
  const text = escapeHtml(String(value || '').trim());
  return `<pre>${text}</pre>`;
}

export function compactText(value = '') {
  return String(value)
    .replace(/\r/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

export function displayName(user = {}) {
  const fullName = [user.first_name, user.last_name].filter(Boolean).join(' ').trim();
  return fullName || user.username || `User ${user.id || 'unknown'}`;
}

export function normalizeCommandPayload(text = '') {
  return text.replace(/^\/[a-zA-Z0-9_]+(?:@\w+)?\s*/, '').trim();
}

export function chunkArray(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

export function extractTags(text = '') {
  const matches = String(text).match(/#[a-zA-Z0-9_-]+/g) || [];
  return [...new Set(matches.map((tag) => tag.slice(1).toLowerCase()))];
}

export function buildChatLink(chat, messageId) {
  if (!chat || !messageId) {
    return '';
  }

  return buildChatLinkFromParts(chat.id, chat.username, messageId);
}

export function isNumeric(value) {
  return /^\d+$/.test(String(value || '').trim());
}

export function extractNumericTelegramId(value = '') {
  const normalized = String(value || '').trim();
  if (!normalized || /[a-zA-Z@/]/.test(normalized)) {
    return 0;
  }

  const digits = normalized.replace(/[^\d-]/g, '');
  return /^-?\d+$/.test(digits) ? Number(digits) : 0;
}

export function normalizeTelegramUsername(value = '') {
  const normalized = String(value || '')
    .trim()
    .replace(/^https?:\/\/t\.me\//i, '')
    .replace(/^telegram\.me\//i, '')
    .replace(/^@/, '')
    .trim();

  if (!normalized || /\s/.test(normalized) || /^[\d-]+$/.test(normalized)) {
    return '';
  }

  const match = normalized.match(/^[A-Za-z][A-Za-z0-9_]{3,31}$/);
  return match ? match[0].toLowerCase() : '';
}

export function buildSyntheticTelegramId(seed = '') {
  const hex = sha256(seed).slice(0, 12);
  return -Number.parseInt(hex, 16);
}

export function buildChatLinkFromParts(chatId, chatUsername, messageId) {
  if (!messageId) {
    return '';
  }

  if (chatUsername) {
    return `https://t.me/${chatUsername}/${messageId}`;
  }

  const normalizedChatId = String(chatId || '');
  if (normalizedChatId.startsWith('-100')) {
    return `https://t.me/c/${normalizedChatId.slice(4)}/${messageId}`;
  }

  return '';
}

export function fieldLookup(rawFields, patterns) {
  const entries = Object.entries(rawFields || {});
  return entries.find(([key]) => patterns.every((pattern) => key.toLowerCase().includes(pattern)))?.[1] || '';
}

export function projectOverviewFromFields(rawFields) {
  const preferredOrder = [
    'Project Name',
    'Telegram Contact ID',
    'Project Category 1',
    'Target category 1',
    'Current Status',
    'Stage',
    'Website',
    'X / Twitter',
    'Problem Statement',
    'What are you building?',
    'Requirement',
    'Requirements',
    'Need',
    'Needs',
  ];

  const lines = [];
  for (const label of preferredOrder) {
    const value = rawFields[label];
    if (value) {
      lines.push(`${label}: ${value}`);
    }
  }

  for (const [key, value] of Object.entries(rawFields)) {
    if (!value || preferredOrder.includes(key)) {
      continue;
    }
    if (lines.length >= 24) {
      break;
    }
    lines.push(`${key}: ${value}`);
  }

  return compactText(lines.join('\n'));
}

export function deriveProjectStatus(rawFields) {
  const statusEntry = Object.entries(rawFields || {}).find(([key]) => {
    const normalized = key.toLowerCase();
    return normalized.includes('status') || normalized.includes('stage');
  });

  return statusEntry?.[1] || '';
}

export function dedupeStrings(items) {
  return [...new Set((items || []).filter(Boolean))];
}

export function parseLocalDateTimeInput(value = '') {
  const normalized = String(value || '').trim();
  if (!normalized) {
    return null;
  }

  const isoMatch = normalized.match(/^(\d{4})[-/](\d{2})[-/](\d{2})(?:[ T](\d{1,2})(?::(\d{2}))?)?$/);
  if (isoMatch) {
    return buildIsoFromLocalParts(
      Number(isoMatch[1]),
      Number(isoMatch[2]),
      Number(isoMatch[3]),
      Number(isoMatch[4] || 0),
      Number(isoMatch[5] || 0)
    );
  }

  const dmyMatch = normalized.match(/^(\d{2})[-/](\d{2})[-/](\d{4})(?:[ T](\d{1,2})(?::(\d{2}))?)?$/);
  if (dmyMatch) {
    return buildIsoFromLocalParts(
      Number(dmyMatch[3]),
      Number(dmyMatch[2]),
      Number(dmyMatch[1]),
      Number(dmyMatch[4] || 0),
      Number(dmyMatch[5] || 0)
    );
  }

  return null;
}

export function formatLocalDateTime(isoValue = '') {
  if (!isoValue) {
    return '';
  }

  const date = new Date(isoValue);
  if (Number.isNaN(date.getTime())) {
    return '';
  }

  return new Intl.DateTimeFormat('en-IN', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date);
}

function buildIsoFromLocalParts(year, month, day, hour, minute) {
  const date = new Date(year, month - 1, day, hour, minute, 0, 0);
  if (
    Number.isNaN(date.getTime())
    || date.getFullYear() !== year
    || date.getMonth() !== month - 1
    || date.getDate() !== day
  ) {
    return null;
  }

  return date.toISOString();
}
