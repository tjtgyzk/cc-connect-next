package feishu

import (
	"html"
	"regexp"
	"strings"

	"github.com/timmyagentic/cc-connect-next/core"
)

var (
	previewMentionPattern = regexp.MustCompile(`(?is)<at\b[^>]*>(.*?)</at\s*>`)
	previewHTMLTagPattern = regexp.MustCompile(`(?is)</?[a-z][^>]*>`)
	previewOpenIDPattern  = regexp.MustCompile(`\bou_[a-zA-Z0-9_-]+\b`)
	previewListPattern    = regexp.MustCompile(`^(?:[-+*]|[0-9]+[.)])\s+(?:\[[ xX]\]\s+)?`)
)

// richCardPreviewText builds plain text for the chat list, independently of
// the card's rendered body. The caller applies the preview length limit.
func richCardPreviewText(markdown string) string {
	text := html.UnescapeString(markdown)
	text = previewMentionPattern.ReplaceAllStringFunc(text, func(tag string) string {
		label := previewMentionPattern.FindStringSubmatch(tag)[1]
		label = previewHTMLTagPattern.ReplaceAllString(label, "")
		label = previewOpenIDPattern.ReplaceAllString(label, "")
		label = strings.Join(strings.Fields(core.StripMarkdown(label)), " ")
		if label == "" {
			return " "
		}
		return " @" + strings.TrimPrefix(label, "@") + " "
	})
	text = previewHTMLTagPattern.ReplaceAllString(text, "")
	text = previewOpenIDPattern.ReplaceAllString(text, "")
	text = core.StripMarkdown(strings.TrimSpace(text))

	var paragraph, tablePreview []string
	lines := strings.Split(text, "\n")
	inTable := false
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			if len(paragraph) > 0 {
				break
			}
			inTable = false
			continue
		}

		// Reuse the card renderer's table checks. Wrapping also recognizes
		// Markdown tables whose optional outer pipes are omitted.
		wrapped := "|" + strings.Trim(line, "|") + "|"
		if isMarkdownTableSeparator(wrapped) {
			continue
		}
		if i+1 < len(lines) && strings.Contains(line, "|") {
			next := "|" + strings.Trim(strings.TrimSpace(lines[i+1]), "|") + "|"
			inTable = inTable || isMarkdownTableSeparator(next)
		}
		if isMarkdownTableRow(line) || (inTable && strings.Contains(line, "|")) {
			if len(paragraph) > 0 {
				break
			}
			// A table-only answer still gets useful context without flattening
			// the whole table into the notification preview.
			if len(tablePreview) < 2 {
				tablePreview = append(tablePreview, strings.ReplaceAll(line, "|", " "))
			}
			continue
		}
		inTable = false
		line = previewListPattern.ReplaceAllString(line, "")
		if line != "" {
			paragraph = append(paragraph, line)
		}
	}
	if len(paragraph) == 0 {
		paragraph = tablePreview
	}
	return strings.Join(strings.Fields(strings.Join(paragraph, " ")), " ")
}
