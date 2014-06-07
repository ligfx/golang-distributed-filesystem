package comm

import "log"

type LoggerWrapper struct {
	*log.Logger
}

func (l *LoggerWrapper) Write(p []byte) (n int, err error) {
	l.Print(string(p))
	return len(p), nil
}