package db

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	jsMaxSafeInteger int64  = 9007199254740991
	jsMinSafeInteger int64  = -9007199254740991
	jsMaxSafeUint    uint64 = 9007199254740991
)

var (
	jsMaxSafeBigInt = big.NewInt(jsMaxSafeInteger)
	jsMinSafeBigInt = big.NewInt(jsMinSafeInteger)
)

// normalizeQueryValue normalizes driver-returned values for UI/JSON transport.
// 当前主要处理 []byte：如果是可读文本则转为 string，否则转为十六进制字符串，避免前端出现“空白值”。
func normalizeQueryValue(v interface{}) interface{} {
	return normalizeQueryValueWithDBType(v, "")
}

func normalizeQueryValueWithDBType(v interface{}, databaseTypeName string) interface{} {
	if b, ok := v.([]byte); ok {
		return bytesToDisplayValue(b, databaseTypeName)
	}
	return normalizeCompositeQueryValue(v)
}

func normalizeCompositeQueryValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch typed := v.(type) {
	case []interface{}:
		items := make([]interface{}, len(typed))
		for i, item := range typed {
			items[i] = normalizeQueryValue(item)
		}
		return items
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, value := range typed {
			out[key] = normalizeQueryValue(value)
		}
		return out
	case json.Number:
		return normalizeJSONNumberForJS(typed)
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return normalizeQueryValue(rv.Elem().Interface())
	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		out := make(map[string]interface{}, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[mapKeyToString(iter.Key().Interface())] = normalizeQueryValue(iter.Value().Interface())
		}
		return out
	case reflect.Slice, reflect.Array:
		// []byte 在上层已单独处理，这里保留对其它切片/数组的递归规整。
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return nil
		}
		size := rv.Len()
		items := make([]interface{}, size)
		for i := 0; i < size; i++ {
			items[i] = normalizeQueryValue(rv.Index(i).Interface())
		}
		return items
	default:
		return normalizeUnsafeIntegerForJS(rv, v)
	}
}

func normalizeJSONNumberForJS(n json.Number) interface{} {
	text := strings.TrimSpace(n.String())
	if text == "" {
		return ""
	}

	if integer, ok := parseJSONInteger(text); ok {
		if integer.Cmp(jsMaxSafeBigInt) > 0 || integer.Cmp(jsMinSafeBigInt) < 0 {
			return text
		}
		return integer.Int64()
	}

	if f, err := n.Float64(); err == nil {
		return f
	}
	return text
}

func parseJSONInteger(text string) (*big.Int, bool) {
	if text == "" {
		return nil, false
	}
	start := 0
	if text[0] == '+' || text[0] == '-' {
		if len(text) == 1 {
			return nil, false
		}
		start = 1
	}
	for i := start; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return nil, false
		}
	}
	value, ok := new(big.Int).SetString(text, 10)
	if !ok {
		return nil, false
	}
	return value, true
}

func mapKeyToString(key interface{}) string {
	if key == nil {
		return "null"
	}
	if s, ok := key.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", key)
}

func bytesToDisplayValue(b []byte, databaseTypeName string) interface{} {
	if b == nil {
		return nil
	}
	if len(b) == 0 {
		return ""
	}

	dbType := strings.ToUpper(strings.TrimSpace(databaseTypeName))
	if isBitLikeDBType(dbType) {
		if u, ok := bytesToUint64(b); ok {
			// JS number precision is limited; keep large bitmasks as string.
			if u <= jsMaxSafeUint {
				return int64(u)
			}
			return fmt.Sprintf("%d", u)
		}
	}

	if utf8.Valid(b) {
		s := string(b)
		if isMostlyPrintable(s) {
			return s
		}
	}

	// Fallback: some drivers return BIT(1) as []byte{0} / []byte{1} without type info.
	if dbType == "" && len(b) == 1 && (b[0] == 0 || b[0] == 1) {
		return int64(b[0])
	}

	return bytesToReadableString(b)
}

func bytesToReadableString(b []byte) interface{} {
	if b == nil {
		return nil
	}
	if len(b) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(b)
}

func isBitLikeDBType(typeName string) bool {
	if typeName == "" {
		return false
	}
	switch typeName {
	case "BIT", "VARBIT":
		return true
	default:
	}
	return strings.HasPrefix(typeName, "BIT")
}

func bytesToUint64(b []byte) (uint64, bool) {
	if len(b) == 0 || len(b) > 8 {
		return 0, false
	}
	var u uint64
	for _, v := range b {
		u = (u << 8) | uint64(v)
	}
	return u, true
}

func normalizeUnsafeIntegerForJS(rv reflect.Value, original interface{}) interface{} {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := rv.Int()
		if n > jsMaxSafeInteger || n < jsMinSafeInteger {
			return strconv.FormatInt(n, 10)
		}
		return original
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := rv.Uint()
		if u > jsMaxSafeUint {
			return strconv.FormatUint(u, 10)
		}
		return original
	default:
		return original
	}
}

func isMostlyPrintable(s string) bool {
	if s == "" {
		return true
	}

	total := 0
	printable := 0
	for _, r := range s {
		total++
		switch r {
		case '\n', '\r', '\t':
			printable++
			continue
		default:
		}
		if unicode.IsPrint(r) {
			printable++
		}
	}

	// 允许少量不可见字符，避免把正常文本误判为二进制。
	return printable*100 >= total*90
}
