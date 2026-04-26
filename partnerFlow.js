import { google } from 'googleapis';
import fs from 'node:fs/promises';
import path from 'node:path';

const PARTNER_SHEET_ID = process.env.GOOGLE_SHEET_ID || process.env.PARTNER_GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID;
const PARTNER_GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || process.env.PARTNER_GOOGLE_API_KEY;
const ADMIN_IDS = (process.env.ADMIN_IDS || '')
  .split(',')
  .map((item) => String(item).trim())
  .filter(Boolean);

const STORE_PATH = path.join(process.cwd(), 'shown_partners.json');
const WEEKLY_MS = 7 * 24 * 60 * 60 * 1000;
const DAILY_MS = 24 * 60 * 60 * 1000;

let store = { chats: {}, pairHistory: [] };
const intervalMap = {};
const weeklyIntervalMap = {};
const processedPartnerCallbacks = new Set();

const sheets = PARTNER_GOOGLE_API_KEY
  ? google.sheets({ version: 'v4', auth: PARTNER_GOOGLE_API_KEY })
  : null;

const weeklyMessage = `Hey Team 👋  

Got any recent updates (partnerships, listings, launches, achievements)?  

Please drop:  
• Image/banner (optional but preferred)  
• Short caption text  
• (Optional) links  

✅ We’ll share it on Collably’s socials for more visibility.  

Reply /stopupdate if you’d like to pause these weekly reminders.`;

export async function loadPartnerStore() {
  try {
    const raw = await fs.readFile(STORE_PATH, 'utf8');
    const parsed = JSON.parse(raw || '{}');

    if (parsed && !parsed.chats) {
      const migrated = { chats: {}, pairHistory: [] };
      Object.entries(parsed || {}).forEach(([key, value]) => {
        if (key === 'pairHistory') {
          migrated.pairHistory = Array.isArray(parsed.pairHistory) ? parsed.pairHistory : [];
        } else if (value && typeof value === 'object' && Array.isArray(value.shown)) {
          migrated.chats[String(key)] = {
            shown: value.shown.map(String),
            weeklyEnabled: true,
          };
        }
      });
      store = migrated;
      return;
    }

    store = {
      chats: parsed.chats || {},
      pairHistory: Array.isArray(parsed.pairHistory) ? parsed.pairHistory : [],
    };
  } catch {
    store = { chats: {}, pairHistory: [] };
  }
}

async function saveStore() {
  await fs.writeFile(STORE_PATH, JSON.stringify(store, null, 2), 'utf8');
}

function ensurePartnerEnv() {
  const missing = [];
  if (!PARTNER_SHEET_ID) missing.push('GOOGLE_SHEET_ID or SPREADSHEET_ID');
  if (!PARTNER_GOOGLE_API_KEY) missing.push('GOOGLE_API_KEY');
  if (!sheets) missing.push('Google Sheets API key client');
  return missing;
}

function ensureChatState(chatId) {
  const key = String(chatId);
  if (!store.chats[key]) store.chats[key] = { shown: [], weeklyEnabled: true, projectSendingEnabled: true };
  if (!Array.isArray(store.chats[key].shown)) store.chats[key].shown = [];
  if (typeof store.chats[key].weeklyEnabled !== 'boolean') store.chats[key].weeklyEnabled = true;
  if (typeof store.chats[key].projectSendingEnabled !== 'boolean') store.chats[key].projectSendingEnabled = true;
  return store.chats[key];
}

function pairKey(a, b) {
  const [x, y] = [String(a), String(b)].sort();
  return `${x}::${y}`;
}

async function markShown(chatId, partnerGid) {
  const state = ensureChatState(chatId);
  const partner = String(partnerGid);
  if (!state.shown.includes(partner)) state.shown.push(partner);
  await saveStore();
}

async function markPairHistory(chatId, partnerGid) {
  const key = pairKey(chatId, partnerGid);
  if (!store.pairHistory.includes(key)) {
    store.pairHistory.push(key);
    await saveStore();
  }
}

async function fetchPartnerSheetData(sheetName) {
  const missing = ensurePartnerEnv();
  if (missing.length) throw new Error(`Missing partner env variables: ${missing.join(', ')}`);

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: PARTNER_SHEET_ID,
    range: sheetName,
  });
  return response.data.values || [];
}

async function isPremiumProjectGroup(chatId) {
  try {
    const premiumProjectsData = await fetchPartnerSheetData('Premium Projects');
    const premiumGroupIds = premiumProjectsData
      .slice(1)
      .map((row) => row?.[0])
      .filter(Boolean)
      .map(String);
    return premiumGroupIds.includes(String(chatId));
  } catch (error) {
    console.error('Error checking Premium Project group ID:', error.message);
    return false;
  }
}

function isAdminChat(chatId) {
  return ADMIN_IDS.includes(String(chatId));
}

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

async function sendNoMoreMatches(bot, chatId) {
  await bot.telegram.sendMessage(
    chatId,
    'We have no more matches for your project right now. We’ll notify you as soon as new potential partners are available.',
  );
}

async function sendMessageToGroup(bot, chatId, projectData) {
  const message = `
Please review your Potential partner:

Project Name: ${projectData.projectName}
Twitter Link: ${projectData.twitterLink}
    
Would you like to proceed with a partnership with the project?
  `;

  return bot.telegram.sendMessage(chatId, message, {
    reply_markup: {
      inline_keyboard: [
        [{ text: 'Yes', callback_data: `YES_${projectData.groupId}_${projectData.projectName}_${chatId}` }],
        [{ text: 'No', callback_data: `NO_${projectData.groupId}_${projectData.projectName}_${chatId}` }],
      ],
    },
  });
}

async function sendRandomPartner(bot, chatId) {
  try {
    const state = ensureChatState(chatId);
    if (!state.projectSendingEnabled) return;

    const trackingData = await fetchPartnerSheetData('Partnership Tracking Projects');
    if (!trackingData.length) {
      await sendNoMoreMatches(bot, chatId);
      return;
    }

    const header = trackingData[0];
    const premiumIndex = header.findIndex((value) => String(value) === String(chatId));
    if (premiumIndex === -1) {
      await bot.telegram.sendMessage(chatId, "We couldn't find your Group ID in Partnership Tracking Projects header Row 1.");
      return;
    }

    const candidates = [];
    for (let i = 2; i < trackingData.length; i += 1) {
      const projectName = trackingData[i][premiumIndex - 2];
      const twitterLink = trackingData[i][premiumIndex - 1];
      const partnerGid = trackingData[i][premiumIndex];
      if (projectName && twitterLink && partnerGid) {
        candidates.push({
          projectName: String(projectName).trim(),
          twitterLink: String(twitterLink).trim(),
          groupId: String(partnerGid).trim(),
        });
      }
    }

    if (!candidates.length) {
      await sendNoMoreMatches(bot, chatId);
      return;
    }

    const shownSet = new Set((state.shown || []).map(String));
    const fresh = candidates.filter((candidate) => {
      const notSeenByThisChat = !shownSet.has(String(candidate.groupId));
      const notGloballyPaired = !store.pairHistory.includes(pairKey(chatId, candidate.groupId));
      return notSeenByThisChat && notGloballyPaired;
    });

    if (!fresh.length) {
      await sendNoMoreMatches(bot, chatId);
      return;
    }

    const selected = pickRandom(fresh);
    await sendMessageToGroup(bot, chatId, selected);
    await markShown(chatId, selected.groupId);
  } catch (error) {
    console.error('sendRandomPartner error:', error);
    await bot.telegram.sendMessage(chatId, `Partner matching failed: ${error.message}`);
  }
}

async function startDailyFlow(bot, chatId) {
  const state = ensureChatState(chatId);
  state.projectSendingEnabled = true;
  await saveStore();
  await sendRandomPartner(bot, chatId);
  const intervalId = setInterval(() => sendRandomPartner(bot, chatId), DAILY_MS);
  if (!Array.isArray(intervalMap[chatId])) intervalMap[chatId] = [];
  intervalMap[chatId].push(intervalId);
}

async function stopProjectSendingForChat(chatId) {
  const state = ensureChatState(chatId);
  state.projectSendingEnabled = false;
  await saveStore();
  stopAllForChat(chatId);
}

function stopAllForChat(chatId) {
  const list = intervalMap[chatId];
  if (list && list.length) {
    list.forEach((id) => clearInterval(id));
    delete intervalMap[chatId];
    return true;
  }
  return false;
}

async function sendWeeklyUpdate(bot, chatId) {
  await bot.telegram.sendMessage(chatId, weeklyMessage);
}

function scheduleWeekly(bot, chatId) {
  if (weeklyIntervalMap[chatId]) {
    clearInterval(weeklyIntervalMap[chatId]);
    delete weeklyIntervalMap[chatId];
  }

  weeklyIntervalMap[chatId] = setInterval(async () => {
    const state = ensureChatState(chatId);
    if (state.weeklyEnabled) await sendWeeklyUpdate(bot, chatId);
  }, WEEKLY_MS);
}

async function startWeeklyForChat(bot, chatId, sendImmediately = false) {
  const state = ensureChatState(chatId);
  state.weeklyEnabled = true;
  await saveStore();
  if (sendImmediately) await sendWeeklyUpdate(bot, chatId);
  scheduleWeekly(bot, chatId);
}

async function stopWeeklyForChat(chatId) {
  const state = ensureChatState(chatId);
  state.weeklyEnabled = false;
  await saveStore();
  if (weeklyIntervalMap[chatId]) {
    clearInterval(weeklyIntervalMap[chatId]);
    delete weeklyIntervalMap[chatId];
  }
}

async function isAllowedPremiumOrAdmin(chatId) {
  return isAdminChat(chatId) || (await isPremiumProjectGroup(chatId));
}

export async function hydratePartnerSchedules(bot) {
  await loadPartnerStore();
  Object.keys(store.chats || {}).forEach((chatId) => {
    const state = store.chats[chatId];
    if (state?.weeklyEnabled) scheduleWeekly(bot, chatId);
  });
}

export function registerPartnerFlow(bot) {
  bot.command('project', async (ctx) => {
    const chatId = ctx.chat.id;
    if (!(await isAllowedPremiumOrAdmin(chatId))) {
      await ctx.reply('This functionality works only for Premium partners of Collably Network.');
      return;
    }

    await startDailyFlow(bot, chatId);
    await startWeeklyForChat(bot, chatId, false);
  });

  bot.command('stop', async (ctx) => {
    const stopped = stopAllForChat(ctx.chat.id);
    if (!stopped) await ctx.reply('No active project flow found to stop.');
  });

  bot.command('stopprojects', async (ctx) => {
    const chatId = ctx.chat.id;
    if (!(await isAllowedPremiumOrAdmin(chatId))) {
      await ctx.reply('This functionality works only for Premium partners of Collably Network.');
      return;
    }
    await stopProjectSendingForChat(chatId);
    await ctx.reply('✅ Project recommendations stopped for this group. Use /project to start again.');
  });

  bot.command('stopupdate', async (ctx) => {
    const chatId = ctx.chat.id;
    if (!(await isAllowedPremiumOrAdmin(chatId))) {
      await ctx.reply('This functionality works only for Premium partners of Collably Network.');
      return;
    }
    await stopWeeklyForChat(chatId);
    await ctx.reply('✅ Weekly update reminders paused. You can resume anytime with /startupdate');
  });

  bot.command('startupdate', async (ctx) => {
    const chatId = ctx.chat.id;
    if (!(await isAllowedPremiumOrAdmin(chatId))) {
      await ctx.reply('This functionality works only for Premium partners of Collably Network.');
      return;
    }
    await startWeeklyForChat(bot, chatId, true);
  });
}

export async function handlePartnerCallback(ctx) {
  const data = ctx.callbackQuery?.data || '';
  const isPartnerCallback = data.startsWith('YES_') || data.startsWith('NO_') || data.startsWith('CONFIRM_YES_') || data.startsWith('CONFIRM_NO_');
  if (!isPartnerCallback) return false;

  if (processedPartnerCallbacks.has(data)) {
    await ctx.answerCbQuery('Already handled.');
    return true;
  }
  processedPartnerCallbacks.add(data);

  let yesText = 'Yes';
  let noText = 'No';

  try {
    if (data.startsWith('YES_')) {
      const [, potentialGroupId, potentialPartnerName, premiumGroupId] = data.split('_');
      yesText = 'Accepted ✅';
      noText = '';

      const trackingData = await fetchPartnerSheetData('Partnership Tracking Projects');
      const premiumRow = trackingData[0].findIndex((id) => String(id) === String(premiumGroupId));
      if (premiumRow === -1) throw new Error('Premium group ID not found in tracking data');

      const premiumProjectName = trackingData[0][premiumRow - 2];
      const premiumProjectTwitterLink = trackingData[0][premiumRow - 1];

      await ctx.telegram.sendMessage(premiumGroupId, `Thank you for your interest. Please wait for confirmation from the ${potentialPartnerName} team.`);
      await ctx.telegram.sendMessage(potentialGroupId, `
The below potential partner has shown interest in partnership with your project:

Project Name: ${premiumProjectName}
Twitter Link: ${premiumProjectTwitterLink}

Would you like to proceed partnership with the project?
      `, {
        reply_markup: {
          inline_keyboard: [
            [{ text: 'Yes', callback_data: `CONFIRM_YES_${potentialGroupId}_${premiumGroupId}_${potentialPartnerName}` }],
            [{ text: 'No', callback_data: `CONFIRM_NO_${potentialGroupId}_${premiumGroupId}_${potentialPartnerName}` }],
          ],
        },
      });

      await markPairHistory(premiumGroupId, potentialGroupId);
    } else if (data.startsWith('NO_')) {
      yesText = '';
      noText = 'Rejected ❌';
      await ctx.reply("Ok! We'll find more suitable projects for you.");
      setTimeout(() => sendRandomPartner(ctx.telegram, ctx.chat.id), DAILY_MS);
    }

    if (data.startsWith('CONFIRM_YES_')) {
      const [, , potentialGroupId, premiumGroupId, potentialPartnerName] = data.split('_');
      yesText = 'Accepted ✅';
      noText = '';

      await ctx.telegram.sendMessage(potentialGroupId, `
Thank you for your interest. Please create a group with the project, share the link here, and tag @collablynetworkCEO, @collablynetworkCMO & @collablynetworkCOO. We'll invite the team for further discussion.
      `);

      await ctx.telegram.sendMessage(premiumGroupId, `
${potentialPartnerName} has accepted your partnership Proposal. Please create a group with the project, share the link here, and tag @collablynetworkCEO & @kundanCLB. We'll invite the team for further discussion.
      `);
    } else if (data.startsWith('CONFIRM_NO_')) {
      const [, , potentialGroupId, premiumGroupId, potentialPartnerName] = data.split('_');
      yesText = '';
      noText = 'Rejected ❌';

      await ctx.telegram.sendMessage(potentialGroupId, "Ok! We'll find more suitable projects for you.");
      await ctx.telegram.sendMessage(premiumGroupId, `Sorry to inform you that ${potentialPartnerName} has decided not to pursue a partnership at this time. We’ll continue to find more partnerships for you.`);
    }

    await ctx.telegram.editMessageReplyMarkup(
      ctx.chat.id,
      ctx.callbackQuery.message.message_id,
      undefined,
      { inline_keyboard: [[{ text: yesText || noText, callback_data: 'DISABLED' }]] },
    ).catch((error) => console.error('Failed to disable partner buttons:', error.message));

    await ctx.answerCbQuery('Done');
  } catch (error) {
    console.error('Partner callback error:', error);
    await ctx.answerCbQuery('Failed');
    await ctx.reply(`Partner action failed: ${error.message}`).catch(() => {});
  }

  return true;
}
