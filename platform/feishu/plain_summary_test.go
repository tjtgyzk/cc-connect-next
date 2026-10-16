package feishu

import (
	"strings"
	"testing"
)

func TestRichCardPreviewText(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{
			name: "native card mention before answer and table",
			text: "<at id=ou_audit_fake></at>\n\n**已完成核查**，请复核。\n\n| 项目 | 状态 |\n| --- | --- |\n| 测试 | 通过 |",
			want: "已完成核查，请复核。",
		},
		{
			name: "named native text mention",
			text: `<at user_id="ou_audit_fake">审计测试机器人</at> **请复核**。`,
			want: "@审计测试机器人 请复核。",
		},
		{
			name: "quoted card id and encoded name",
			text: `<at id="ou_audit_fake">小王 &amp; 小李</at> 已完成`,
			want: "@小王 & 小李 已完成",
		},
		{
			name: "empty mentions have no id in preview",
			text: `<at id=ou_audit_fake></at> <at user_id="ou_other_fake"></at>`,
			want: "",
		},
		{
			name: "id as display label is omitted",
			text: `<at id=ou_audit_fake>ou_audit_fake</at> 已完成`,
			want: "已完成",
		},
		{
			name: "encoded native mention",
			text: `&lt;at id=ou_audit_fake&gt;&lt;/at&gt; **已完成**`,
			want: "已完成",
		},
		{
			name: "first paragraph with markdown and whitespace",
			text: "  ## **结果**\n> 已完成\t`check_all()`，~~待处理~~通过。\n\n第二段不进入预览。",
			want: "结果 已完成 check_all()，待处理通过。",
		},
		{
			name: "list prefixes and task markers",
			text: "- [x] **检查完成**\n2. 已保存\n+ 请复核",
			want: "检查完成 已保存 请复核",
		},
		{
			name: "table without blank line after prose",
			text: "已完成。\n| 项目 | 状态 |\n| --- | --- |\n| 测试 | 通过 |",
			want: "已完成。",
		},
		{
			name: "table only keeps headers and first data row",
			text: "| **项目** | 状态 |\n| :--- | ---: |\n| 测试 | 通过 |\n| 构建 | 通过 |",
			want: "项目 状态 测试 通过",
		},
		{
			name: "table with optional outer pipes omitted",
			text: "项目 | 状态\n--- | ---\n测试 | 通过\n构建 | 通过",
			want: "项目 状态 测试 通过",
		},
		{
			name: "prefer prose following a table",
			text: "| 项目 | 状态 |\n| --- | --- |\n| 测试 | 通过 |\n\n检查完成，请复核。",
			want: "检查完成，请复核。",
		},
		{
			name: "preserve code identifiers and readable link text",
			text: "请看 [结果](https://example.com/result)，调用 `my_func_name()`。",
			want: "请看 结果 (https://example.com/result)，调用 my_func_name()。",
		},
		{
			name: "blank input",
			text: " \t\n\r\n ",
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := richCardPreviewText(tt.text); got != tt.want {
				t.Fatalf("richCardPreviewText() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRichCardPreviewTextLeavesLengthLimitToCaller(t *testing.T) {
	text := strings.Repeat("已完成🙂", 40)
	if got := richCardPreviewText(text); got != text {
		t.Fatalf("preview helper unexpectedly truncated text: %q", got)
	}
}
