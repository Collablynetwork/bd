import bot from './bot.js';
import { setupTelegramCommands } from './commands.js';
import { initDb } from './db.js';
import { runStartupTasks, startSchedulers } from './scheduler.js';

let isBotRunning = false;

async function main() {
  initDb();
  await bot.telegram.getMe();

  const launchPromise = bot.launch({
    polling: { drop_pending_updates: true },
  });
  isBotRunning = true;
  await setupTelegramCommands(bot);
  startSchedulers(bot);
  console.log(`[${new Date().toISOString()}] Collably BD agent is running and polling Telegram updates.`);
  console.log(`[${new Date().toISOString()}] Background knowledge sync started.`);

  launchPromise.catch((error) => {
    isBotRunning = false;
    console.error('Bot launch failed:', error);
    process.exit(1);
  });

  runStartupTasks(bot)
    .then(() => {
      console.log(`[${new Date().toISOString()}] Background startup sync completed.`);
    })
    .catch((error) => {
      console.error('Background startup sync failed:', error);
    });
}

main().catch((error) => {
  console.error('Fatal startup error:', error);
  process.exit(1);
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => {
    if (!isBotRunning) {
      process.exit(0);
      return;
    }

    try {
      bot.stop(signal);
    } catch (error) {
      console.warn(`Failed to stop bot cleanly on ${signal}:`, error.message);
    } finally {
      isBotRunning = false;
      process.exit(0);
    }
  });
}
