package lakehouse

// normalizeValue 把 Go 常见数值类型归一化到 schema 类型；类型不兼容返回 ok=false。
func normalizeValue(v any, typ string) (any, bool) {
	if v == nil {
		return nil, true
	}
	switch typ {
	case TypeInt:
		switch x := v.(type) {
		case int:
			return int64(x), true
		case int8:
			return int64(x), true
		case int16:
			return int64(x), true
		case int32:
			return int64(x), true
		case int64:
			return x, true
		}
	case TypeString:
		if s, ok := v.(string); ok {
			return s, true
		}
	case TypeBool:
		if b, ok := v.(bool); ok {
			return b, true
		}
	case TypeFloat:
		switch x := v.(type) {
		case float32:
			return float64(x), true
		case float64:
			return x, true
		}
	}
	return nil, false
}
