package mysqlqueue

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
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


func AutoBindParam(SQL string, args ...interface{}) (string, error) {
	ArgsLength := len(args) - 1
	flag := 0
	var sb strings.Builder
	for _, v := range SQL {
		if v == '?' {
			if flag <= ArgsLength {
				sb.WriteString("'")
				switch val := args[flag].(type) {
				case string:
					sb.WriteString(Mysql_real_escape_string(val))
				case int:
					sb.WriteString(Mysql_real_escape_string(strconv.Itoa(val, 10)))
				case int8:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatInt(int64(val), 10)))
				case int16:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatInt(int64(val), 10)))
				case int64:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatInt(val, 10)))

				case uint:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatUint(uint64(val), 10)))
				case uint16:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatUint(uint64(val), 10)))
				case uint32:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatUint(uint64(val), 10)))
				case uint64:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatUint(val, 10)))
				case float32:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatFloat(float64(val), 'f', -1, 32)))
				case float64:
					sb.WriteString(Mysql_real_escape_string(strconv.FormatFloat(val, 'f', -1, 64)))
				case []byte:
					sb.WriteString(Mysql_real_escape_bytes(val))
				case byte:
					if reflect.TypeOf(val).String() == "uint8" {
						sb.WriteString(Mysql_real_escape_string(strconv.FormatInt(int64(val), 10)))
					} else {
						sb.WriteByte(Mysql_real_escape_string(val))
					}
				case rune:
					if reflect.TypeOf(val).String() == "int32" {
						sb.WriteString(Mysql_real_escape_string(strconv.FormatInt(int64(val), 10)))
					} else {
						sb.WriteRune(val)
					}

				case bool:				
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