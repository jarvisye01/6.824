package shardctrler

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dServer  logTopic = "SVER"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dPersist logTopic = "PERS"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var (
	debugStart   time.Time
	logVerbosity int
)

const (
	TraceLevel = 5
	DebugLevel = 4
	WarnLevel  = 3
	InfoLevel  = 2
	ErrorLevel = 1
	EmptyLevel = 0
)

func init() {
	logVerbosity = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func getVerbosity() int {
	v := os.Getenv("SHCTL_VERBOSE")
	level := EmptyLevel
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
	if logVerbosity >= TraceLevel {
		logf(t, format, v...)
	}
}

func Debugf(t logTopic, format string, v ...interface{}) {
	if logVerbosity >= DebugLevel {
		logf(t, format, v...)
	}
}

func Warnf(t logTopic, format string, v ...interface{}) {
	if logVerbosity >= WarnLevel {
		logf(t, format, v...)
	}
}

func Infof(t logTopic, format string, v ...interface{}) {
	if logVerbosity >= InfoLevel {
		logf(t, format, v...)
	}
}
