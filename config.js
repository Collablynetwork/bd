import path from 'node:path';
import fs from 'node:fs';
import dotenv from 'dotenv';
import { google } from 'googleapis';
import { OpenAI } from 'openai';

dotenv.config();

const env = process.env;

const requiredVars = [
  'TELEGRAM_BOT_TOKEN',
  'COLLABLY_TEAM_IDS',
  'SPREADSHEET_ID',
  'CLIENT_DATA_SHEET_ID',
  'OPENAI_API_KEY',
];

const missing = requiredVars.filter((key) => !env[key]);
const hasGoogleKeyFile = Boolean(env.GOOGLE_SERVICE_ACCOUNT_FILE);
const hasInlineGoogleCreds = Boolean(env.GOOGLE_CLIENT_EMAIL && env.GOOGLE_PRIVATE_KEY);
if (!hasGoogleKeyFile && !hasInlineGoogleCreds) {
  missing.push('GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_CLIENT_EMAIL/GOOGLE_PRIVATE_KEY');
}
if (missing.length) {
  console.error(`Missing required .env variables: ${missing.join(', ')}`);
  process.exit(1);
}

export const BOT_NAME = 'Collably Network';
export const TELEGRAM_BOT_TOKEN = env.TELEGRAM_BOT_TOKEN;
export const TEAM_IDS = (env.COLLABLY_TEAM_IDS || '')
  .split(',')
  .map((item) => Number(item.trim()))
  .filter(Boolean);
export const ADMIN_IDS = (env.ADMIN_IDS || '')
  .split(',')
  .map((item) => Number(item.trim()))
  .filter(Boolean);
export const BROADCAST_THROTTLE_MS = Number(env.BROADCAST_THROTTLE_MS || 500);

export const SPREADSHEET_ID = env.SPREADSHEET_ID;
export const CLIENT_DATA_SHEET_ID = env.CLIENT_DATA_SHEET_ID;
export const CLIENT_DATA_SHEET_NAME = env.CLIENT_DATA_SHEET_NAME || '';
export const SERVICE_DATASET_SHEET_ID = env.SERVICE_DATASET_SHEET_ID || '';
export const SERVICE_DATASET_SHEET_NAME = env.SERVICE_DATASET_SHEET_NAME || '';
export const GOOGLE_SERVICE_ACCOUNT_FILE = env.GOOGLE_SERVICE_ACCOUNT_FILE || '';
export const KNOWLEDGE_DOC_ID = env.KNOWLEDGE_DOC_ID || '';
export const CLIENT_KNOWLEDGE_DOC_ID = env.CLIENT_KNOWLEDGE_DOC_ID || '';
export const OPENAI_CHAT_MODEL = env.OPENAI_CHAT_MODEL || env.FINE_TUNED_MODEL_NAME || 'gpt-4o-mini';
export const OPENAI_EMBEDDING_MODEL = env.OPENAI_EMBEDDING_MODEL || 'text-embedding-3-small';

export const DB_PATH = env.SQLITE_DB_PATH || path.join(process.cwd(), 'data', 'collably-agent.sqlite');
export const SHEETS_EXPORT_DELAY_HOURS = Number(env.SHEETS_EXPORT_DELAY_HOURS || 24);
export const FOLLOW_UP_STEP_HOURS = [
  Number(env.FIRST_FOLLOWUP_HOURS || 24),
  Number(env.SECOND_FOLLOWUP_HOURS || 72),
  Number(env.THIRD_FOLLOWUP_HOURS || 120),
];
export const MAX_FOLLOW_UPS = FOLLOW_UP_STEP_HOURS.length;

export const PROFILE_SYNC_INTERVAL_MS = Number(env.PROFILE_SYNC_INTERVAL_MS || 60 * 60 * 1000);
export const SHEETS_SYNC_INTERVAL_MS = Number(env.SHEETS_SYNC_INTERVAL_MS || 60 * 60 * 1000);
export const REMINDER_CHECK_INTERVAL_MS = Number(env.REMINDER_CHECK_INTERVAL_MS || 15 * 60 * 1000);
export const KNOWLEDGE_BUILD_INTERVAL_MS = Number(env.KNOWLEDGE_BUILD_INTERVAL_MS || 60 * 60 * 1000);
export const DOC_SYNC_INTERVAL_MS = Number(env.DOC_SYNC_INTERVAL_MS || 6 * 60 * 60 * 1000);
export const SERVICE_DATASET_SYNC_INTERVAL_MS = Number(env.SERVICE_DATASET_SYNC_INTERVAL_MS || 24 * 60 * 60 * 1000);
export const DAILY_KNOWLEDGE_WINDOW_HOURS = Number(env.DAILY_KNOWLEDGE_WINDOW_HOURS || 24);
export const ANNOUNCEMENT_REMINDER_LEAD_MINUTES = Number(env.ANNOUNCEMENT_REMINDER_LEAD_MINUTES || 30);

const googleScopes = [
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/documents',
];

const authOptions = GOOGLE_SERVICE_ACCOUNT_FILE
  ? buildKeyFileAuthOptions(GOOGLE_SERVICE_ACCOUNT_FILE, googleScopes)
  : {
      credentials: {
        client_email: env.GOOGLE_CLIENT_EMAIL,
        private_key: env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
      },
      scopes: googleScopes,
    };

const auth = new google.auth.GoogleAuth(authOptions);

export const sheetsClient = google.sheets({ version: 'v4', auth });
export const docsClient = google.docs({ version: 'v1', auth });
export const openaiClient = new OpenAI({ apiKey: env.OPENAI_API_KEY });

function buildKeyFileAuthOptions(keyFile, scopes) {
  const resolvedPath = path.resolve(keyFile);
  if (!fs.existsSync(resolvedPath)) {
    console.error(`Google service account file not found: ${resolvedPath}`);
    process.exit(1);
  }

  return {
    keyFile: resolvedPath,
    scopes,
  };
}
