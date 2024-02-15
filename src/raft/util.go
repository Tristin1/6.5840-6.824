package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
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

type logTopic string

const (
	dINIT          logTopic = "INIT"
	dElection      logTopic = "ELEC"
	dHeartBeat     logTopic = "HEAR"
	dAppend        logTopic = "APPD"
	dClientAdd     logTopic = "LOGA"
	dCOMMITUPDATE  logTopic = "COMM"
	dNetworkFail   logTopic = "NETF"
	dInfo          logTopic = "INFO"
	dLeader        logTopic = "LEAD"
	DKVServer      logTopic = "KVSE"
	DClientRequest logTopic = "DCLI"
	dPersist       logTopic = "PERS"
	dSnap          logTopic = "SNAP"
	dTerm          logTopic = "TERM"
	DConfig        logTopic = "CONF"
	dTimer         logTopic = "TIMR"
	dTrace         logTopic = "TRCE"
	dVote          logTopic = "VOTE"
	dWarn          logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var leaderOnly int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	leaderOnly = getLeaderOnly()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func getLeaderOnly() int {
	v := os.Getenv("LEADERONLY")
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
