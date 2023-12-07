package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient  logTopic = "KV_CLNT1"
	dServer  logTopic = "KV_SRVR"
	dInfo    logTopic = "KV_INFO"
	dError   logTopic = "KV_ERRO"
	dCommand logTopic = "KV_CMMD"
	dClear   logTopic = "KV_CLEA"
)

var (
	debugStart   time.Time
	logVerbosity int
)

const (
	TraceLevel = 0
	DebugLevel = 1
	WarnLevel  = 2
	InfoLevel  = 3
	ErrorLevel = 4
)

func init() {
	logVerbosity = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func getVerbosity() int {
	v := os.Getenv("SERVER_VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func logf(t logTopic, format string, v ...interface{}) {
	time := time.Since(debugStart).Microseconds()
	time /= 100
	prefix := fmt.Sprintf("%06d %s ", time, string(t))
	format = prefix + format
	log.Printf(format, v...)
}

func Tracef(t logTopic, format string, v ...interface{}) {
	if logVerbosity <= TraceLevel {
		logf(t, format, v...)
	}
}

func Debugf(t logTopic, format string, v ...interface{}) {
	if logVerbosity <= DebugLevel {
		logf(t, format, v...)
	}
}

func Warnf(t logTopic, format string, v ...interface{}) {
	if logVerbosity <= WarnLevel {
		logf(t, format, v...)
	}
}

func Infof(t logTopic, format string, v ...interface{}) {
	if logVerbosity <= InfoLevel {
		logf(t, format, v...)
	}
}
