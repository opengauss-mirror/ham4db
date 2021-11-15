/*
	Copyright 2021 SANGFOR TECHNOLOGIES

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package log

import (
	"errors"
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"log/syslog"

	"os"
	"runtime/debug"
	"time"
)

// LogLevel indicate the severity of a log entry
type LogLevel int

const (
	FATAL LogLevel = iota
	CRITICAL
	ERROR
	WARNING
	NOTICE
	INFO
	DEBUG
)

// globalLogLevel indicates the global level filter for all logs (only entries with level equals or higher
// than this value will be logged)
var globalLogLevel = DEBUG
var printStackTrace = false
var disableOutPut = false

// syslogWriter is optional, and defaults to nil (disabled)
var syslogLevel = ERROR
var syslogWriter *syslog.Writer

var fatalFunc = func() { os.Exit(1) }

func (ll LogLevel) String() string {
	switch ll {
	case FATAL:
		return "FATAL"
	case CRITICAL:
		return "CRITICAL"
	case ERROR:
		return "ERROR"
	case WARNING:
		return "WARNING"
	case NOTICE:
		return "NOTICE"
	case INFO:
		return "INFO"
	case DEBUG:
		return "DEBUG"
	}
	return "unknown"
}

// SetPrintStackTrace enables/disables dumping the stack upon error logging
func SetPrintStackTrace(shouldPrintStackTrace bool) {
	printStackTrace = shouldPrintStackTrace
}

// SetLevel sets the global log level. Only entries with level equals or higher than
// this value will be logged
func SetLevel(logLevel LogLevel) {
	globalLogLevel = logLevel
}

// EnableSyslogWriter enables, if possible, writes to syslog. These will execute _in addition_ to normal logging
func EnableSyslogWriter(tag string) (err error) {
	syslogWriter, err = syslog.New(syslog.LOG_ERR, tag)
	if err != nil {
		syslogWriter = nil
	}
	return err
}

// SetSyslogLevel sets the minimal syslog level. Only entries with level equals or higher than
// this value will be logged. However, this is also capped by the global log level. That is,
// messages with lower level than global-log-level will be discarded at any case.
func SetSyslogLevel(logLevel LogLevel) {
	syslogLevel = logLevel
}

// SetFatalFunc set fatal function, when get fatal log, will exec this function
func SetFatalFunc(fatalF func()) {
	fatalFunc = fatalF
}

// DisableOutput disable log output or not
func DisableOutput(disable bool) {
	disableOutPut = disable
}

// logFormattedEntry nicely formats and emits a log entry
func logFormattedEntry(logLevel LogLevel, message string, args ...interface{}) string {

	// log level is higher than config
	if logLevel > globalLogLevel {
		return ""
	}

	// format log entry, if TZ env variable is set, update the timestamp timezone
	localizedTime := time.Now()
	tzLocation := os.Getenv("TZ")
	if tzLocation != "" {
		// if invalid tz location was provided, just leave it as the default
		if location, err := time.LoadLocation(tzLocation); err == nil {
			localizedTime = time.Now().In(location)
		}
	}
	msgArgs := fmt.Sprintf(message, args...)
	entryString := fmt.Sprintf("%s %s %s", localizedTime.Format(constant.DateFormatLog), logLevel, msgArgs)

	// if output is disabled, just return entry string
	if disableOutPut {
		return entryString
	}

	// output entry string to standard error output and system log if not nil
	fmt.Fprintln(os.Stderr, entryString)
	if syslogWriter != nil {
		go func() error {
			if logLevel > syslogLevel {
				return nil
			}
			switch logLevel {
			case FATAL:
				return syslogWriter.Emerg(msgArgs)
			case CRITICAL:
				return syslogWriter.Crit(msgArgs)
			case ERROR:
				return syslogWriter.Err(msgArgs)
			case WARNING:
				return syslogWriter.Warning(msgArgs)
			case NOTICE:
				return syslogWriter.Notice(msgArgs)
			case INFO:
				return syslogWriter.Info(msgArgs)
			case DEBUG:
				return syslogWriter.Debug(msgArgs)
			}
			return nil
		}()
	}
	return entryString
}

// logEntry emits a formatted log entry
func logEntry(logLevel LogLevel, message string, args ...interface{}) string {
	entryString := message
	for _, s := range args {
		entryString += fmt.Sprintf(" %s", s)
	}
	return logFormattedEntry(logLevel, entryString)
}

// logErrorEntry emits a log entry based on given error object
func logErrorEntry(logLevel LogLevel, err error) error {
	if err == nil {
		// No error
		return nil
	}
	entryString := fmt.Sprintf("%+v", err)
	logEntry(logLevel, entryString)
	if printStackTrace {
		debug.PrintStack()
	}
	return err
}

func Debug(message string, args ...interface{}) string {
	return logEntry(DEBUG, message, args...)
}

func Debugf(message string, args ...interface{}) string {
	return logFormattedEntry(DEBUG, message, args...)
}

func Info(message string, args ...interface{}) {
	logFormattedEntry(INFO, message, args...)
}

func Infof(message string, args ...interface{}) string {
	return logFormattedEntry(INFO, message, args...)
}

func Notice(message string, args ...interface{}) string {
	return logEntry(NOTICE, message, args...)
}

func Noticef(message string, args ...interface{}) string {
	return logFormattedEntry(NOTICE, message, args...)
}

func Warning(message string, args ...interface{}) {
	logFormattedEntry(WARNING, message, args...)
}

func Warningf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(WARNING, message, args...))
}

func Error(message string, args ...interface{}) {
	logFormattedEntry(ERROR, message, args...)
}

func Errorf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(ERROR, message, args...))
}

func Errore(err error) error {
	return logErrorEntry(ERROR, err)
}

func Erroref(err error) {
	_ = logErrorEntry(ERROR, err)
}

func Critical(message string, args ...interface{}) error {
	return errors.New(logEntry(CRITICAL, message, args...))
}

func Criticalf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(CRITICAL, message, args...))
}

func Criticale(err error) error {
	return logErrorEntry(CRITICAL, err)
}

// Fatal emits a FATAL level entry and exists the program
func Fatal(message string, args ...interface{}) {
	logEntry(FATAL, message, args...)
	fatalFunc()
}

// Fatalf emits a FATAL level entry and exists the program
func Fatalf(message string, args ...interface{}) error {
	logFormattedEntry(FATAL, message, args...)
	fatalFunc()
	return errors.New(logFormattedEntry(CRITICAL, message, args...))
}

// Fatale emits a FATAL level entry and exists the program
func Fatale(err error) error {
	logErrorEntry(FATAL, err)
	fatalFunc()
	return err
}
