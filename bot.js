import { Telegraf } from 'telegraf';
import { TELEGRAM_BOT_TOKEN } from './config.js';
import { getSuggestionById, isTeamMember, markSuggestionHandled } from './db.js';
import { registerCommands } from './commands.js';
import { handleIncomingMessage } from './messageHandler.js';
import { markSuggestionCardsHandled } from './notifications.js';
import { displayName } from './utils.js';
import { handlePartnerCallback, registerPartnerFlow } from './partnerFlow.js';

const bot = new Telegraf(TELEGRAM_BOT_TOKEN);

registerCommands(bot);
registerPartnerFlow(bot);

bot.on('message', handleIncomingMessage);

bot.on('callback_query', async (ctx) => {
  const partnerHandled = await handlePartnerCallback(ctx);
  if (partnerHandled) return;

  const data = ctx.callbackQuery?.data || '';
  if (data === 'noop') {
    await ctx.answerCbQuery('Already handled.');
    return;
  }

  if (!data.startsWith('action:')) {
    return;
  }

  if (!isTeamMember(Number(ctx.from.id))) {
    await ctx.answerCbQuery('Not allowed.');
    return;
  }

  const suggestionId = Number(data.split(':')[1]);
  const suggestion = getSuggestionById(suggestionId);
  if (!suggestion) {
    await ctx.answerCbQuery('Suggestion not found.');
    return;
  }

  if (suggestion.status !== 'action_taken') {
    markSuggestionHandled({
      suggestionId,
      actionTakenById: Number(ctx.from.id),
      actionTakenByName: displayName(ctx.from),
      actionSource: 'button',
    });
  }

  await markSuggestionCardsHandled(ctx.telegram, suggestionId);
  await ctx.answerCbQuery('Action taken');
});

bot.catch((error, ctx) => {
  console.error(`Telegraf error for update ${ctx.update?.update_id || 'unknown'}:`, error);
});

export default bot;
