import { initDb } from './db.js';
import { importHistoricalConversations } from './history.js';

const args = parseArgs(process.argv.slice(2));

if (!args.file) {
  console.error('Usage: node import-history.js --file /path/to/history.json [--project "Project Name|@username|telegram_id"] [--chat-title "Group"] [--chat-id -100123]');
  process.exit(1);
}

initDb();

try {
  const result = importHistoricalConversations({
    filePath: args.file,
    projectQuery: args.project || '',
    chatTitle: args['chat-title'] || '',
    chatId: args['chat-id'] || null,
  });

  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  console.error('History import failed:', error.message);
  process.exit(1);
}

function parseArgs(argv) {
  const result = {};

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) {
      continue;
    }

    const key = token.slice(2);
    const value = argv[index + 1] && !argv[index + 1].startsWith('--')
      ? argv[index + 1]
      : 'true';

    result[key] = value;
    if (value !== 'true') {
      index += 1;
    }
  }

  return result;
}
