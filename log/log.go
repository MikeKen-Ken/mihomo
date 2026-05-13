package log

import (
	"fmt"
	"os"

	"github.com/metacubex/mihomo/common/observable"

	log "github.com/sirupsen/logrus"
)

var (
	logCh  = make(chan Event)
	source = observable.NewObservable[Event](logCh)
	level  = INFO
)

// chineseTextFormatter 控制台输出：时间、级别为中文标签，消息体由调用方模板决定。
type chineseTextFormatter struct{}

func (f *chineseTextFormatter) Format(entry *log.Entry) ([]byte, error) {
	ts := entry.Time.Format("2006-01-02T15:04:05.000000000Z07:00")
	var lvl string
	switch entry.Level {
	case log.DebugLevel:
		lvl = "调试"
	case log.InfoLevel:
		lvl = "信息"
	case log.WarnLevel:
		lvl = "警告"
	case log.ErrorLevel:
		lvl = "错误"
	case log.FatalLevel:
		lvl = "致命"
	case log.PanicLevel:
		lvl = "异常"
	default:
		lvl = entry.Level.String()
	}
	return []byte(fmt.Sprintf("%s [%s] %s\n", ts, lvl, entry.Message)), nil
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&chineseTextFormatter{})
}

type Event struct {
	LogLevel LogLevel
	Payload  string
}

func (e *Event) Type() string {
	return e.LogLevel.String()
}

func Infoln(format string, v ...any) {
	event := newLog(INFO, format, v...)
	logCh <- event
	print(event)
}

func Warnln(format string, v ...any) {
	event := newLog(WARNING, format, v...)
	logCh <- event
	print(event)
}

func Errorln(format string, v ...any) {
	event := newLog(ERROR, format, v...)
	logCh <- event
	print(event)
}

func Debugln(format string, v ...any) {
	event := newLog(DEBUG, format, v...)
	logCh <- event
	print(event)
}

func Fatalln(format string, v ...any) {
	log.Fatalf(format, v...)
}

func Subscribe() observable.Subscription[Event] {
	sub, _ := source.Subscribe()
	return sub
}

func UnSubscribe(sub observable.Subscription[Event]) {
	source.UnSubscribe(sub)
}

func Level() LogLevel {
	return level
}

func SetLevel(newLevel LogLevel) {
	level = newLevel
}

func print(data Event) {
	if data.LogLevel < level {
		return
	}

	switch data.LogLevel {
	case INFO:
		log.Infoln(data.Payload)
	case WARNING:
		log.Warnln(data.Payload)
	case ERROR:
		log.Errorln(data.Payload)
	case DEBUG:
		log.Debugln(data.Payload)
	}
}

func newLog(logLevel LogLevel, format string, v ...any) Event {
	return Event{
		LogLevel: logLevel,
		Payload:  fmt.Sprintf(format, v...),
	}
}
