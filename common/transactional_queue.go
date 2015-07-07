package common

type TransactionalQueue struct {
}

type TransactionalQueueTransaction struct {
}

func (self *TransactionalQueue) Push(blocks []BlockID) {
	panic("Not implemented")
}

func (self *TransactionalQueue) StartRead() *TransactionalQueueTransaction {
	panic("Not implemented")
}

func (self *TransactionalQueueTransaction) AbortIfNotCommitted() error {
	panic("Not implemented")
}

func (self *TransactionalQueueTransaction) Get() []BlockID {
	panic("Not implemented")
}

func (self *TransactionalQueueTransaction) CommitRead() error {
	panic("Not implemented")
}
