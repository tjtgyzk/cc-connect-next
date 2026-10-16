package feishu

import (
	"context"
	"regexp"
	"strings"

	"github.com/timmyagentic/cc-connect-next/core"
)

var (
	_                   core.RichCardFinalDeliveryPolicy = (*interactivePlatform)(nil)
	leadingReplyMention                                  = regexp.MustCompile(`^<at\s+id=(?:"(ou_[A-Za-z0-9_-]+)"|'(ou_[A-Za-z0-9_-]+)'|(ou_[A-Za-z0-9_-]+))\s*></at>`)
	cardMentionStart                                     = regexp.MustCompile(`(?i)<at\s`)
)

// RequiresNewFinalCard preserves table/card rendering while putting a verified
// reply-to-sender mention in a newly sent message, rather than adding it to an
// existing progress card. Model-supplied tags for other users remain ineligible.
func (p *interactivePlatform) RequiresNewFinalCard(_ context.Context, value any, markdown string) bool {
	rc, ok := value.(replyContext)
	if !ok || rc.replyMentionID == "" || rc.messageID == "" || rc.chatID == "" {
		return false
	}
	// Do not turn an indented code example into an active mention.
	markdown = strings.TrimLeft(markdown, "\r\n")
	if markdown == "" {
		return false
	}
	if end, code := markdownIndentedCodeLineEnd(markdown, 0); code && end > 0 {
		return false
	}
	markdown = strings.TrimSpace(markdown)
	match := leadingReplyMention.FindStringSubmatch(markdown)
	if match == nil || len(cardMentionStart.FindAllStringIndex(markdown, -1)) != 1 {
		return false
	}
	return match[1]+match[2]+match[3] == rc.replyMentionID
}
