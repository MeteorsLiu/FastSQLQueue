package mysqlqueue

import (
	"errors"
	"fmt"
	"strings"
	"strconv"
)

func BindParam(SQL, Type string, args ...interface{}) (string, error) {
	TypeLength := len(Type) - 1
	ArgsLength := len(args) - 1
	if ArgsLength != TypeLength {
		return "", errors.New("Out of indexs")
	}
	var sb strings.Builder
	flag := 0
	for _, v := range SQL {
		if v == '?' {
			if flag <= TypeLength {
				sb.WriteString("'%")
				sb.WriteByte(Type[flag])
				sb.WriteString("'")

			} else {
				return "", errors.New("Out of indexs")
			}
			flag++
		} else {
			sb.WriteRune(v)
		}
	}
	for i, v := range args {
		switch val := v.(type) {
		case string:
			args[i] = Mysql_real_escape_string(val)
		}
	}

	return fmt.Sprintf(sb.String(), args...), nil

}


func AutoBindParam(SQL, args ...interface{}) (string, error) {
	ArgsLength := len(args) - 1

	for i, v := range SQL {
		if v == '?' {
			if flag <= ArgsLength {
				sb.WriteString("'")
				switch val := args[flag].(type) {
				case string:
					sb.WriteString(Mysql_real_escape_string(val))
				case int, int8, int16, int32, int64:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatInt(int64(val), 10))

				case uint, uint8, uint16, uint32, uint64:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatUint(uint64(val), 10))
				case float32:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatFloat(val, 'f', -1, 32))
				case float64:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatFloat(val, 'f', -1, 64))
				case []byte:
					sb.WriteString(Mysql_real_escape_bytes(val))
				case byte:
					sb.WriteString(Mysql_real_escape_byte(val))
				case rune:
					sb.WriteRune(val)

				case Boolean:				
					if val {
   						sb.WriteString("1")
					} else {
   						sb.WriteString("0")
					}

				default:
					return "", errors.New("Unknow Type")
				}
				sb.WriteString("'")

			} else {
				return "", errors.New("Out of indexs")
			}
			flag++
		} else {
			sb.WriteRune(v)
		}
	}

	return sb.String(), nil

}
