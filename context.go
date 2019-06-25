package raft

// Context represents the current state of the server. It is passed into
// a command when the command is being applied since the server methods
// are locked.

// 服务的当前状态： 任期、索引、commit索引
// 暴露给外部的是个接口
type Context interface {
	Server() Server
	CurrentTerm() uint64
	CurrentIndex() uint64
	CommitIndex() uint64
}

// context is the concrete implementation of Context.
// Context的具体实现
type context struct {
	server       Server // raft服务
	currentIndex uint64 // 当前索引
	currentTerm  uint64 // 当前任期
	commitIndex  uint64 // 已提交索引
}

// Server returns a reference to the server.
func (c *context) Server() Server {
	return c.server
}

// CurrentTerm returns current term the server is in.
func (c *context) CurrentTerm() uint64 {
	return c.currentTerm
}

// CurrentIndex returns current index the server is at.
func (c *context) CurrentIndex() uint64 {
	return c.currentIndex
}

// CommitIndex returns last commit index the server is at.
func (c *context) CommitIndex() uint64 {
	return c.commitIndex
}
