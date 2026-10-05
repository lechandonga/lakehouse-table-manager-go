package lakehouse

import "encoding/json"

// decodeNumber 按列类型把 JSON 标量转成对外稳定的 Go 类型。
func decodeNumber(raw json.RawMessage, typ string) any {
	if string(raw) == "null" {
		return nil
	}
	switch typ {
	case TypeInt:
		var i int64
		if json.Unmarshal(raw, &i) == nil {
			return i
		}
	case TypeFloat:
		var f float64
		if json.Unmarshal(raw, &f) == nil {
			return f
		}
	case TypeString:
		var str string
		if json.Unmarshal(raw, &str) == nil {
			return str
		}
	case TypeBool:
		var b bool
		if json.Unmarshal(raw, &b) == nil {
			return b
		}
	}
	return string(raw)
}

// decodeRawScalar 用于已删除/旧 schema 残留列（投影时按列 ID 忽略，类型不重要）。
func decodeRawScalar(raw json.RawMessage) any {
	var v any
	if json.Unmarshal(raw, &v) == nil {
		return v
	}
	return string(raw)
}
