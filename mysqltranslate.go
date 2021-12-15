package mysqlqueue

import (
	"fmt"
	"strings"
	"errors"
)





func BindParam(SQL, Type string, args ...interface{}) (string, error) {
	TypeLength := len(Type)-1 
	var sb strings.Builder
	flag := 0
	for _, v := range SQL {
		if flag > TypeLength {
			return "", errors.New("Out of indexs")
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

	return fmt.Sprintf(sb.String(), args...), nil

}
