package util

import "github.com/gzjjyz/srvlib/utils"

func LogErr(err error) {
	utils.SafeLogErr(err, true)
}
