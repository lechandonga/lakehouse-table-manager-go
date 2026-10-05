package lakehouse

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// primaryKeyString 主键编码为确定字符串键。主键支持 int/string/bool。
func primaryKeyString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return "s:" + x
	case int:
		return "i:" + strconv.FormatInt(int64(x), 10)
	case int64:
		return "i:" + strconv.FormatInt(x, 10)
	case float64:
		return "f:" + strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return "b:" + strconv.FormatBool(x)
	default:
		b, _ := json.Marshal(x)
		return "j:" + string(b)
	}
}

// coerceValue 校验/规整读取到的值与列类型一致。
func coerceValue(v any, col Column) (any, error) {
	if v == nil {
		if !col.Nullable {
			return nil, fmt.Errorf("%w: column %q", ErrNullViolation, col.Name)
		}
		return nil, nil
	}
	switch col.Type {
	case TypeInt:
		switch x := v.(type) {
		case int:
			return int64(x), nil
		case int64:
			return x, nil
		case float64:
			if x == float64(int64(x)) {
				return int64(x), nil
			}
		}
		return nil, typeErr(col, v)
	case TypeFloat:
		switch x := v.(type) {
		case float64:
			return x, nil
		case int:
			return float64(x), nil
		case int64:
			return float64(x), nil
		}
		return nil, typeErr(col, v)
	case TypeString:
		if s, ok := v.(string); ok {
			return s, nil
		}
		return nil, typeErr(col, v)
	case TypeBool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, typeErr(col, v)
	}
	return nil, typeErr(col, v)
}

func typeErr(col Column, v any) error {
	return fmt.Errorf("%w: column %q expects %s, got %T", ErrTypeMismatch, col.Name, col.Type, v)
}
