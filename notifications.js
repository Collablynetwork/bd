import { getSuggestionDeliveries, getTeamMemberIds, recordSuggestionDelivery } from './db.js';
import { escapeHtml, formatLocalDateTime } from './utils.js';

export async function notifyTeamAboutSuggestion(telegram, suggestion, deliveryType = 'initial', reminderNumber = 0) {
  const sentCount = [];
  const teamIds = getTeamMemberIds();

  for (const adminId of teamIds) {
    try {
      const sent = await telegram.sendMessage(
        adminId,
        buildSuggestionHtml(suggestion, deliveryType, reminderNumber),
        {
          parse_mode: 'HTML',
          reply_markup: {
            inline_keyboard: [[
              { text: suggestion.status === 'action_taken' ? 'Action taken' : 'Respond', callback_data: `action:${suggestion.id}` },
            ]],
          },
          disable_web_page_preview: true,
        }
      );

      recordSuggestionDelivery({
        suggestionId: suggestion.id,
        adminId,
        dmChatId: adminId,
        dmMessageId: sent.message_id,
        deliveryType,
      });
      sentCount.push(adminId);
    } catch (error) {
      if (error.response?.error_code === 403) {
        continue;
      }
      throw error;
    }
  }

  return sentCount.length;
}

export async function markSuggestionCardsHandled(telegram, suggestionId) {
  const deliveries = getSuggestionDeliveries(suggestionId);
  for (const delivery of deliveries) {
    try {
      await telegram.editMessageReplyMarkup(delivery.dm_chat_id, delivery.dm_message_id, undefined, {
        inline_keyboard: [[
          { text: 'Action taken', callback_data: 'noop' },
        ]],
      });
    } catch (error) {
      if (error.response?.error_code === 400 || error.response?.error_code === 403) {
        continue;
      }
      console.warn(`Failed to update delivery ${delivery.id}:`, error.message);
    }
  }
}

export async function notifyTeamAboutAnnouncement(telegram, reminder) {
  const teamIds = getTeamMemberIds();
  let sentCount = 0;

  for (const adminId of teamIds) {
    try {
      await telegram.sendMessage(adminId, buildAnnouncementHtml(reminder), {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      });
      sentCount += 1;
    } catch (error) {
      if (error.response?.error_code === 403) {
        continue;
      }
      throw error;
    }
  }

  return sentCount;
}

function buildSuggestionHtml(suggestion, deliveryType, reminderNumber) {
  const heading = deliveryType === 'initial'
    ? '<b>New BD draft</b>'
    : `<b>Reminder ${reminderNumber}/3</b>\nNo Collably response has been recorded yet.`;

  const chatLabel = suggestion.chat_link
    ? `<a href="${escapeHtml(suggestion.chat_link)}">${escapeHtml(suggestion.chat_title)}</a>`
    : escapeHtml(suggestion.chat_title);

  return [
    heading,
    `<b>Suggestion ID</b>: ${suggestion.id}`,
    `<b>Group</b>: ${chatLabel}`,
    `<b>Project member</b>: ${escapeHtml(suggestion.client_sender_name || String(suggestion.client_sender_id))}`,
    '<b>Client message</b>',
    `<pre>${escapeHtml(suggestion.client_text)}</pre>`,
    '<b>BD draft</b>',
    `<pre>${escapeHtml(suggestion.ai_response)}</pre>`,
    suggestion.service_angle ? `<b>Service angle</b>: ${escapeHtml(suggestion.service_angle)}` : '',
    suggestion.reason ? `<b>Internal note</b>: ${escapeHtml(suggestion.reason)}` : '',
  ].filter(Boolean).join('\n');
}

function buildAnnouncementHtml(reminder) {
  const chatLabel = reminder.chat_link
    ? `<a href="${escapeHtml(reminder.chat_link)}">${escapeHtml(reminder.chat_title || reminder.project_label || 'Open group')}</a>`
    : escapeHtml(reminder.chat_title || reminder.project_label || 'Project');

  return [
    '<b>Announcement reminder</b>',
    `<b>Project</b>: ${escapeHtml(reminder.project_label || 'Unknown project')}`,
    `<b>Group</b>: ${chatLabel}`,
    `<b>Announcement</b>: ${escapeHtml(reminder.announcement_text)}`,
    `<b>Scheduled time</b>: ${escapeHtml(formatLocalDateTime(reminder.announcement_at) || reminder.announcement_at)}`,
    '<b>Reminder</b>: 30 minutes before announcement time.',
  ].filter(Boolean).join('\n');
}
