const lastForwardedReplyByChat = new Map();

export function saveLastForwardedReply({ chatId, forwardedText, generatedReply, botMessageId }) {
  if (!chatId || !forwardedText) return;
  lastForwardedReplyByChat.set(Number(chatId), {
    forwardedText: String(forwardedText || ''),
    generatedReply: String(generatedReply || ''),
    botMessageId: botMessageId || null,
    updatedAt: new Date().toISOString(),
  });
}

export function getLastForwardedReply(chatId) {
  return lastForwardedReplyByChat.get(Number(chatId)) || null;
}
