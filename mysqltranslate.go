package mysqlqueue

import (
	"fmt"
	"strings"
)





func BindParam(SQL, Type string, args ...interface{}) {
	TypeLength := len(Type)
	var sb strings.Builder
	flag := 0
	for _, v := range SQL {
		if flag > TypeLength {
			break
		}

		if v == '?' {
			sb.WriteString("'%")
			sb.WriteByte(Type[flag])
			sb.WriteString("'")
			flag++
		} else {
			sb.WriteRune(v)

		}
	}
	for i, v := range args {
		args[i] = Mysql_real_escape_string(v)
	}

	return fmt.SPrint(sb.String(), args...)

}
