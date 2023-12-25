package crudiator

import (
	"fmt"
	"time"
)

type Level int

const (
	None Level = iota
	Debug
	Info
	Warn
	Error
)

func (l Level) shouldLog(lvl Level) bool {
	return lvl >= None && lvl <= l
}

type Logger interface {
	Error(e error)
	Info(m string, args ...any)
	Warn(m string, args ...any)
	Debug(m string, args ...any)
}

func NewStdOutLogger(lvl Level) Logger {
	return &StdOutLogger{lvl: lvl}
}

func NewNopLogger() Logger {
	return &nopLogger{}
}

func (l nopLogger) Debug(m string, args ...any) {

}

func (l nopLogger) Info(m string, args ...any) {

}

func (l nopLogger) Warn(m string, args ...any) {

}

func (l nopLogger) Error(e error) {

}

type nopLogger struct {
}

type StdOutLogger struct {
	lvl Level
}

func logf(strLevel string, m string, args ...any) {
	fmt.Printf("%s %s: %s\n", timeFormat(), strLevel, fmt.Sprintf(m, args...))
}

func (l StdOutLogger) Debug(m string, args ...any) {
	if l.lvl.shouldLog(Debug) {
		logf("DEBUG", m, args...)
	}
}

func (l StdOutLogger) Info(m string, args ...any) {
	if l.lvl.shouldLog(Info) {
		logf("INFO", m, args...)
	}
}

func (l StdOutLogger) Warn(m string, args ...any) {
	if l.lvl.shouldLog(Warn) {
		logf("WARN", m, args...)
	}
}

func (l StdOutLogger) Error(e error) {
	if l.lvl.shouldLog(Error) {
		logf("ERROR", e.Error())
	}
}

func timeFormat() string {
	t := time.Now()
	return t.Format(time.RFC3339)
}
