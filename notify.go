package pgpubsub

type Notify struct {
	Messages chan string
	Err      chan error
}

func NewNotify() *Notify {
	return &Notify{
		Messages: make(chan string),
		Err:      make(chan error),
	}
}

func (n *Notify) Close() {
	close(n.Messages)
	close(n.Err)
}
