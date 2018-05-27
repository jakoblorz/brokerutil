package loopback

type Publisher struct {
	channel chan interface{}
}

func (p Publisher) NotifyOnMessagePublish(msg interface{}) error {

	p.channel <- msg

	return nil
}
