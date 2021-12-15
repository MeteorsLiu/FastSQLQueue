package mysqlqueue

import (
	"errors"
	"fmt"
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
