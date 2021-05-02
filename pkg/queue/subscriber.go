package queue

import (
	"errors"
	"time"

	"github.com/lib/pq"
)

const (
	minReconnectInterval = time.Second
	maxReconnectInterval = time.Second
	timeToCheck          = 10 * time.Second
)

type Subscriber struct {
	listener *pq.Listener

	channel    string
	connString string
}

func NewSubscriber(connString string, channel string) *Subscriber {
	return &Subscriber{channel: channel, connString: connString}
}

func (s *Subscriber) Subscribe() (notify *Notify, err error) {
	if s.listener != nil {
		return nil, errors.New("already subscribe")
	}

	notify = &Notify{
		Messages: make(chan string),
		Err:      make(chan error),
	}

	eventCallback := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			notify.Err <- err
		}
	}

	s.listener = pq.NewListener(s.connString, minReconnectInterval, maxReconnectInterval, eventCallback)
	if err := s.listener.Listen(s.channel); err != nil {
		return nil, err
	}

	return waitForNotification(s.listener), nil
}

func (s *Subscriber) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
}

func waitForNotification(l *pq.Listener) *Notify {
	notify := NewNotify()
	go func() {
		for {
			select {
			case n := <-l.Notify:
				if n != nil {
					notify.Messages <- n.Extra
				}
			case <-time.After(timeToCheck):
				if err := l.Ping(); err != nil {
					notify.Err <- err
					continue
				}
			}
		}
	}()
	return notify
}
