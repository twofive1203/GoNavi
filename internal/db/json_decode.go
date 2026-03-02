package db

import (
	"bytes"
	"encoding/json"
)

func decodeJSONWithUseNumber(data []byte, out interface{}) error {
	if out == nil {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	normalizeDecodedJSONNumbers(out)
	return nil
}

func normalizeDecodedJSONNumbers(out interface{}) {
	switch typed := out.(type) {
	case *[]map[string]interface{}:
		if typed == nil {
			return
		}
		for i := range *typed {
			row := (*typed)[i]
			for key, value := range row {
				row[key] = normalizeQueryValue(value)
			}
		}
	case *map[string]interface{}:
		if typed == nil || *typed == nil {
			return
		}
		for key, value := range *typed {
			(*typed)[key] = normalizeQueryValue(value)
		}
	case *[]interface{}:
		if typed == nil {
			return
		}
		for i, item := range *typed {
			(*typed)[i] = normalizeQueryValue(item)
		}
	case *interface{}:
		if typed == nil {
			return
		}
		*typed = normalizeQueryValue(*typed)
	}
}
