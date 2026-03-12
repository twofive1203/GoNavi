package db

import "strings"

func normalizeKingbaseIdentCommon(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}

	// 兼容被多次 JSON 序列化后的转义引号：
	// \\\"schema\\\" -> \"schema\" -> "schema"
	for i := 0; i < 8; i++ {
		next := strings.TrimSpace(value)
		next = strings.ReplaceAll(next, `\\\"`, `\"`)
		next = strings.ReplaceAll(next, `\"`, `"`)
		if next == value {
			break
		}
		value = next
	}
	value = strings.TrimSpace(value)

	stripWrapperOnce := func(text string) string {
		t := strings.TrimSpace(text)
		if strings.HasPrefix(t, `\`) && len(t) > 1 {
			t = strings.TrimSpace(strings.TrimPrefix(t, `\`))
		}
		if strings.HasSuffix(t, `\`) && len(t) > 1 {
			t = strings.TrimSpace(strings.TrimSuffix(t, `\`))
		}
		if len(t) >= 4 && strings.HasPrefix(t, `\"`) && strings.HasSuffix(t, `\"`) {
			return strings.TrimSpace(t[2 : len(t)-2])
		}
		if len(t) >= 2 && strings.HasPrefix(t, `"`) && strings.HasSuffix(t, `"`) {
			return strings.TrimSpace(t[1 : len(t)-1])
		}
		if len(t) >= 2 && strings.HasPrefix(t, "`") && strings.HasSuffix(t, "`") {
			return strings.TrimSpace(t[1 : len(t)-1])
		}
		if len(t) >= 2 && strings.HasPrefix(t, "[") && strings.HasSuffix(t, "]") {
			return strings.TrimSpace(t[1 : len(t)-1])
		}
		return t
	}

	for i := 0; i < 8; i++ {
		next := stripWrapperOnce(value)
		if next == value {
			break
		}
		value = next
	}
	value = strings.TrimSpace(value)

	// 兼容错误的二次引用与残留反斜杠。
	value = strings.ReplaceAll(value, `\"`, `"`)
	value = strings.ReplaceAll(value, `""`, "")
	value = strings.TrimSpace(value)

	for i := 0; i < 8; i++ {
		next := strings.TrimSpace(value)
		changed := false
		if strings.HasPrefix(next, `\`) && len(next) > 1 {
			next = strings.TrimSpace(strings.TrimPrefix(next, `\`))
			changed = true
		}
		if strings.HasSuffix(next, `\`) && len(next) > 1 {
			next = strings.TrimSpace(strings.TrimSuffix(next, `\`))
			changed = true
		}
		if !changed || next == value {
			break
		}
		value = next
	}

	return strings.TrimSpace(value)
}

func splitKingbaseQualifiedNameCommon(raw string) (schema string, table string) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", ""
	}

	sep := findKingbaseQualifiedSeparator(text)
	if sep < 0 {
		return "", normalizeKingbaseIdentCommon(text)
	}

	schemaPart := normalizeKingbaseIdentCommon(text[:sep])
	tablePart := normalizeKingbaseIdentCommon(text[sep+1:])

	if tablePart == "" {
		if schemaPart == "" {
			return "", normalizeKingbaseIdentCommon(text)
		}
		return "", schemaPart
	}
	if schemaPart == "" {
		return "", tablePart
	}
	return schemaPart, tablePart
}

func findKingbaseQualifiedSeparator(raw string) int {
	inDouble := false
	inBacktick := false
	inBracket := false
	escaped := false

	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if escaped {
			escaped = false
			continue
		}

		if ch == '\\' {
			escaped = true
			continue
		}

		if inDouble {
			if ch == '"' {
				// SQL 双引号转义："" 代表字面量 "
				if i+1 < len(raw) && raw[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			continue
		}

		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		if inBracket {
			if ch == ']' {
				inBracket = false
			}
			continue
		}

		switch ch {
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '[':
			inBracket = true
		case '.':
			return i
		}
	}

	return -1
}
