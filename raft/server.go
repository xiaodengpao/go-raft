package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	Stopped      = "stopped"
	Initialized  = "initialized"
	Follower     = "follower"
	Candidate    = "candidate"
	Leader       = "leader"
	Snapshotting = "snapshotting"
)

const (
	MaxLogEntriesPerRequest         = 2000
	NumberOfLogEntriesAfterSnapshot = 200
)

const (
	// DefaultHeartbeatInterval is the interval that the leader will send
	// AppendEntriesRequests to followers to maintain leadership.
	DefaultHeartbeatInterval = 50 * time.Millisecond

	DefaultElectionTimeout = 150 * time.Millisecond
)

// ElectionTimeoutThresholdPercent specifies the threshold at which the server
// will dispatch warning events that the heartbeat RTT is too close to the
// election timeout.
const ElectionTimeoutThresholdPercent = 0.8

//------------------------------------------------------------------------------
//
// Errors
//
//------------------------------------------------------------------------------

var NotLeaderError = errors.New("raft.Server: Not current leader")
var DuplicatePeerError = errors.New("raft.Server: Duplicate peer")
var CommandTimeoutError = errors.New("raft: Command timeout")
var StopError = errors.New("raft: Has been stopped")

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A server is involved in the consensus protocol and can act as a follower,
// candidate or a leader.
type Server interface {
	Name() string
	Context() interface{}
	StateMachine() StateMachine
	Leader() string
	State() string
	Path() string
	LogPath() string
	SnapshotPath(lastIndex uint64, lastTerm uint64) string
	Term() uint64
	CommitIndex() uint64
	VotedFor() string
	MemberCount() int
	QuorumSize() int
	IsLogEmpty() bool
	LogEntries() []*LogEntry
	LastCommandName() string
	GetState() string
	ElectionTimeout() time.Duration
	SetElectionTimeout(duration time.Duration)
	HeartbeatInterval() time.Duration
	SetHeartbeatInterval(duration time.Duration)
	Transporter() Transporter
	SetTransporter(t Transporter)
	AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	RequestVote(req *RequestVoteRequest) *RequestVoteResponse
	RequestSnapshot(req *SnapshotRequest) *SnapshotResponse
	SnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse
	AddPeer(name string, connectiongString string) error
	RemovePeer(name string) error
	Peers() map[string]*Peer
	Init() error
	Start() error
	Stop()
	Running() bool
	Do(command Command) (interface{}, error)
	TakeSnapshot() error
	LoadSnapshot() error
	AddEventListener(string, EventListener)
	FlushCommitIndex()
}

type server struct {
	*eventDispatcher

	name string
	path string

	// state：每个节点总是处于以下状态的一种：follower、candidate、leader
	state string

	// 网络运输车
	transporter Transporter

	// 上下文
	context interface{}

	// currentTerm：Raft协议关键概念，每个term内都会产生一个新的leader，当前任期
	currentTerm uint64

	// 投票
	votedFor string

	// 日志
	log *Log

	// 领导者
	leader string

	// peers：raft中每个节点需要了解其他节点信息，尤其是leader节点
	peers map[string]*Peer

	// 读写锁
	mutex sync.RWMutex

	syncedPeer map[string]bool

	stopped chan bool

	// 命令通道，所有的命令通过c传递
	// 包括两类RPC请求
	c chan *ev

	// 选举超时
	electionTimeout time.Duration

	// 心跳间隔时间
	heartbeatInterval time.Duration

	// 快照，用于快速恢复数据
	snapshot *Snapshot

	// PendingSnapshot is an unfinished snapshot.
	// After the pendingSnapshot is saved to disk,
	// it will be set to snapshot and also will be
	// set to nil.
	pendingSnapshot *Snapshot

	stateMachine            StateMachine
	maxLogEntriesPerRequest uint64

	connectionString string

	routineGroup sync.WaitGroup
}

// An internal event to be processed by the server's event loop.
type ev struct {
	target      interface{}
	returnValue interface{}
	c           chan error
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new server with a log at the given path. transporter must
// not be nil. stateMachine can be nil if snapshotting and log
// compaction is to be disabled. context can be anything (including nil)
// and is not used by the raft package except returned by
// Server.Context(). connectionString can be anything.
func NewServer(name string, path string, transporter Transporter, stateMachine StateMachine, ctx interface{}, connectionString string) (Server, error) {
	if name == "" {
		return nil, errors.New("raft.Server: Name cannot be blank")
	}
	if transporter == nil {
		panic("raft: Transporter required")
	}

	s := &server{
		name:                    name,
		path:                    path,
		transporter:             transporter,
		stateMachine:            stateMachine,
		context:                 ctx,
		state:                   Stopped,
		peers:                   make(map[string]*Peer),
		log:                     newLog(),
		c:                       make(chan *ev, 256),
		electionTimeout:         DefaultElectionTimeout,
		heartbeatInterval:       DefaultHeartbeatInterval,
		maxLogEntriesPerRequest: MaxLogEntriesPerRequest,
		connectionString:        connectionString,
	}
	s.eventDispatcher = newEventDispatcher(s)

	// Setup apply function.
	s.log.ApplyFunc = func(e *LogEntry, c Command) (interface{}, error) {
		// Dispatch commit event.
		s.DispatchEvent(newEvent(CommitEventType, e, nil))

		// Apply command to the state machine.
		switch c := c.(type) {
		case CommandApply:
			return c.Apply(&context{
				server:       s,
				currentTerm:  s.currentTerm,
				currentIndex: s.log.internalCurrentIndex(),
				commitIndex:  s.log.commitIndex,
			})
		case deprecatedCommandApply:
			return c.Apply(s)
		default:
			return nil, fmt.Errorf("Command does not implement Apply()")
		}
	}

	return s, nil
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

//--------------------------------------
// General
//--------------------------------------

// Retrieves the name of the server.
func (s *server) Name() string {
	return s.name
}

// Retrieves the storage path for the server.
func (s *server) Path() string {
	return s.path
}

// The name of the current leader.
func (s *server) Leader() string {
	return s.leader
}

// Retrieves a copy of the peer data.
func (s *server) Peers() map[string]*Peer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	peers := make(map[string]*Peer)
	for name, peer := range s.peers {
		peers[name] = peer.clone()
	}
	return peers
}

// Retrieves the object that transports requests.
func (s *server) Transporter() Transporter {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.transporter
}

func (s *server) SetTransporter(t Transporter) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.transporter = t
}

// Retrieves the context passed into the constructor.
func (s *server) Context() interface{} {
	return s.context
}

// Retrieves the state machine passed into the constructor.
func (s *server) StateMachine() StateMachine {
	return s.stateMachine
}

// Retrieves the log path for the server.
func (s *server) LogPath() string {
	return path.Join(s.path, "log")
}

// Retrieves the current state of the server.
func (s *server) State() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

// Sets the state of the server.
func (s *server) setState(state string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Temporarily store previous values.
	prevState := s.state
	prevLeader := s.leader

	// Update state and leader.
	s.state = state
	if state == Leader {
		s.leader = s.Name()
		s.syncedPeer = make(map[string]bool)
	}

	// Dispatch state and leader change events.
	s.DispatchEvent(newEvent(StateChangeEventType, s.state, prevState))

	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}
}

// Retrieves the current term of the server.
func (s *server) Term() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.currentTerm
}

// Retrieves the current commit index of the server.
func (s *server) CommitIndex() uint64 {
	s.log.mutex.RLock()
	defer s.log.mutex.RUnlock()
	return s.log.commitIndex
}

// Retrieves the name of the candidate this server voted for in this term.
func (s *server) VotedFor() string {
	return s.votedFor
}

// Retrieves whether the server's log has no entries.
func (s *server) IsLogEmpty() bool {
	return s.log.isEmpty()
}

// A list of all the log entries. This should only be used for debugging purposes.
func (s *server) LogEntries() []*LogEntry {
	s.log.mutex.RLock()
	defer s.log.mutex.RUnlock()
	return s.log.entries
}

// A reference to the command name of the last entry.
func (s *server) LastCommandName() string {
	return s.log.lastCommandName()
}

// Get the state of the server for debugging
func (s *server) GetState() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return fmt.Sprintf("Name: %s, State: %s, Term: %v, CommitedIndex: %v ", s.name, s.state, s.currentTerm, s.log.commitIndex)
}

// Check if the server is promotable
func (s *server) promotable() bool {
	return s.log.currentIndex() > 0
}

//--------------------------------------
// Membership
//--------------------------------------

// Retrieves the number of member servers in the consensus.
func (s *server) MemberCount() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.peers) + 1
}

// Retrieves the number of servers required to make a quorum.
func (s *server) QuorumSize() int {
	return (s.MemberCount() / 2) + 1
}

//--------------------------------------
// Election timeout
//--------------------------------------

// Retrieves the election timeout.
func (s *server) ElectionTimeout() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.electionTimeout
}

// Sets the election timeout.
func (s *server) SetElectionTimeout(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.electionTimeout = duration
}

//--------------------------------------
// Heartbeat timeout
//--------------------------------------

// Retrieves the heartbeat timeout.
func (s *server) HeartbeatInterval() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.heartbeatInterval
}

// Sets the heartbeat timeout.
func (s *server) SetHeartbeatInterval(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.heartbeatInterval = duration
	for _, peer := range s.peers {
		peer.setHeartbeatInterval(duration)
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Initialization
//--------------------------------------

// Reg the NOPCommand
func init() {
	RegisterCommand(&NOPCommand{})
	RegisterCommand(&DefaultJoinCommand{})
	RegisterCommand(&DefaultLeaveCommand{})
}

// 启动 raft server
// If log entries exist then allow promotion to candidate if no AEs received.
// If no log entries exist then wait for AEs from another node.
// If no log entries exist and a self-join command is issued then
// immediately become leader and commit entry.
func (s *server) Start() error {
	// Exit if the server is already running.
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}

	if err := s.Init(); err != nil {
		return err
	}

	// stopped needs to be allocated each time server starts
	// because it is closed at `Stop`.
	s.stopped = make(chan bool)
	s.setState(Follower)

	// If no log entries exist then
	// 1. wait for AEs from another node
	// 2. wait for self-join command
	// to set itself promotable
	if !s.promotable() {
		s.debugln("start as a new raft server")

		// If log entries exist then allow promotion to candidate
		// if no AEs received.
	} else {
		s.debugln("start from previous saved state")
	}

	debugln(s.GetState())

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()

		// raft循环
		s.loop()
	}()

	return nil
}

// Init initializes the raft server.
// If there is no previous log file under the given path, Init() will create an empty log file.
// Otherwise, Init() will load in the log entries from the log file.
func (s *server) Init() error {
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}

	// Server has been initialized or server was stopped after initialized
	// If log has been initialized, we know that the server was stopped after
	// running.
	if s.state == Initialized || s.log.initialized {
		s.state = Initialized
		return nil
	}

	// Create snapshot directory if it does not exist
	err := os.Mkdir(path.Join(s.path, "snapshot"), 0700)
	if err != nil && !os.IsExist(err) {
		s.debugln("raft: Snapshot dir error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	if err := s.readConf(); err != nil {
		s.debugln("raft: Conf file error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	// Initialize the log and load it up.
	if err := s.log.open(s.LogPath()); err != nil {
		s.debugln("raft: Log error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	// Update the term to the last term in the log.
	_, s.currentTerm = s.log.lastInfo()

	s.state = Initialized
	return nil
}

// Shuts down the server.
func (s *server) Stop() {
	if s.State() == Stopped {
		return
	}

	close(s.stopped)

	// make sure all goroutines have stopped before we close the log
	s.routineGroup.Wait()

	s.log.close()
	s.setState(Stopped)
}

// Checks if the server is currently running.
func (s *server) Running() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return (s.state != Stopped && s.state != Initialized)
}

//--------------------------------------
// Term
//--------------------------------------

// 可能涉及的操作：
// 1：更新当前任期   2：存储prevLeader   3：voteFor重置   4：leader = leaderName
// 如果一个leader发现了更大的term，立即停止自己的领导身份，变身follwer（保证任期最大特性, 保证任期的逻辑时钟特性，同时leader被废弃后，要保证整个集群的信息完成性不受影响）
func (s *server) updateCurrentTerm(term uint64, leaderName string) {
	_assert(term > s.currentTerm,
		"upadteCurrentTerm: update is called when term is not larger than currentTerm")

	// Store previous values temporarily.
	prevTerm := s.currentTerm
	prevLeader := s.leader

	// set currentTerm = T, convert to follower (§5.1)
	// 如果当前是leader，停止心跳
	if s.state == Leader {
		for _, peer := range s.peers {
			peer.stopHeartbeat(false)
		}
	}

	// update the term and clear vote for
	if s.state != Follower {
		s.setState(Follower)
	}

	s.mutex.Lock()
	s.currentTerm = term
	s.leader = leaderName
	s.votedFor = ""
	s.mutex.Unlock()

	// Dispatch change events.
	s.DispatchEvent(newEvent(TermChangeEventType, s.currentTerm, prevTerm))

	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}
}

//--------------------------------------
// Event Loop
//--------------------------------------

//               ________
//            --|Snapshot|                 timeout
//            |  --------                  ______
// recover    |       ^                   |      |
// snapshot / |       |snapshot           |      |
// higher     |       |                   v      |     recv majority votes
// term       |    --------    timeout    -----------                        -----------
//            |-> |Follower| ----------> | Candidate |--------------------> |  Leader   |
//                 --------               -----------                        -----------
//                    ^          higher term/ |                         higher term |
//                    |            new leader |                                     |
//                    |_______________________|____________________________________ |
// The main event loop for the server
func (s *server) loop() {
	defer s.debugln("server.loop.end")

	state := s.State()

	// 按照server当前状态执行循环
	for state != Stopped {
		s.debugln("server.loop.run ", state)
		switch state {

		// 跟随者
		case Follower:
			s.followerLoop()

		// 候选人
		case Candidate:
			s.candidateLoop()

		// 领导
		case Leader:
			s.leaderLoop()

		// 快照
		case Snapshotting:
			s.snapshotLoop()
		}
		state = s.State()
	}
}

// Sends an event to the event loop to be processed. The function will wait
// until the event is actually processed before returning.
func (s *server) send(value interface{}) (interface{}, error) {
	if !s.Running() {
		return nil, StopError
	}

	event := &ev{target: value, c: make(chan error, 1)}
	select {
	case s.c <- event:
	case <-s.stopped:
		return nil, StopError
	}
	select {
	case <-s.stopped:
		return nil, StopError
	case err := <-event.c:
		return event.returnValue, err
	}
}

func (s *server) sendAsync(value interface{}) {
	if !s.Running() {
		return
	}

	event := &ev{target: value, c: make(chan error, 1)}
	// try a non-blocking send first
	// in most cases, this should not be blocking
	// avoid create unnecessary go routines
	select {
	case s.c <- event:
		return
	default:
	}

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		select {
		case s.c <- event:
		case <-s.stopped:
		}
	}()
}

// 跟随者事件循环.
// Responds to RPCs from candidates and leaders.
// Converts to candidate if election timeout elapses without either:
//   1.Receiving valid AppendEntries RPC, or
//   2.Granting vote to candidate
func (s *server) followerLoop() {
	since := time.Now()
	electionTimeout := s.ElectionTimeout()
	timeoutChan := afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)

	for s.State() == Follower {
		var err error
		update := false
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return

		case e := <-s.c:

			// follower从其他节点接收的几类消息
			switch req := e.target.(type) {

			// 服务初始化时，只有自己一个节点的情况下，become leader
			case JoinCommand:
				//If no log entries exist and a self-join command is issued
				//then immediately become leader and commit entry.
				if s.log.currentIndex() == 0 && req.NodeName() == s.Name() {
					s.debugln("selfjoin and promote to leader")
					s.setState(Leader)
					s.processCommand(req, e)
				} else {
					err = NotLeaderError
				}

			// 添加日志请求
			case *AppendEntriesRequest:
				// If heartbeats get too close to the election timeout then send an event.
				elapsedTime := time.Now().Sub(since)
				if elapsedTime > time.Duration(float64(electionTimeout)*ElectionTimeoutThresholdPercent) {
					s.DispatchEvent(newEvent(ElectionTimeoutThresholdEventType, elapsedTime, nil))
				}
				e.returnValue, update = s.processAppendEntriesRequest(req)

			// 投票请求（其他候选人申请成为leader）
			case *RequestVoteRequest:
				e.returnValue, update = s.processRequestVoteRequest(req)

			case *SnapshotRequest:
				e.returnValue = s.processSnapshotRequest(req)

			default:
				err = NotLeaderError
			}
			// Callback to event.
			e.c <- err

		case <-timeoutChan:
			// 当前日志 index > 0
			if s.promotable() {
				s.setState(Candidate)
			} else {
				// 作为跟随者继续等待，刷新超时时间
				update = true
			}
		}

		// 只有两种情况下会保持follower状态并继续等待:
		//   1.收到更大任期（不小于自己）leader的PRC
		//   2.成功投出自己的选票
		if update {
			since = time.Now()
			timeoutChan = afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)
		}
	}
}

// 候选人循环，当服务处于候选人状态时，会运行这个loop
func (s *server) candidateLoop() {

	// 上一个leader
	prevLeader := s.leader

	// 清空leader
	s.leader = ""

	// 这是啥
	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}

	lastLogIndex, lastLogTerm := s.log.lastInfo()
	doVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	var respChan chan *RequestVoteResponse

	for s.State() == Candidate {
		if doVote {
			// Increment current term, vote for self.
			s.currentTerm++
			s.votedFor = s.name

			// Send RequestVote RPCs to all other servers.
			respChan = make(chan *RequestVoteResponse, len(s.peers))
			for _, peer := range s.peers {
				s.routineGroup.Add(1)
				go func(peer *Peer) {
					defer s.routineGroup.Done()
					// 向其他节点拉票：
					// 当前任期、节点名、最大日志index、上一个任务号
					// 上一个任期号不随着currentTerm++的增加而增加
					peer.sendVoteRequest(newRequestVoteRequest(s.currentTerm, s.name, lastLogIndex, lastLogTerm), respChan)
				}(peer)
			}

			// 等待其他节点的结果:
			//   * 获取大多数节点的支持: 变成领导人
			//   * 从领导人处接收到AppendEntries请求: return，然后跟随.
			//   * 网络超时: increment term, start new election
			//   * 发现更高的任期号: step down (§5.1) ps: 和网络超时，term++ 配合使用

			// 节点默认先给自己一票， = 1
			votesGranted = 1

			// 投票的超时时间，每个节点都是随机的， 1倍 ~ 2倍 ElectionTimeout
			timeoutChan = afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)

			// 循环内不再拉票
			doVote = false
		}

		// 如果获取了半数以上投票，不再等待其他节点的结果.
		// 并结束候选人循环
		if votesGranted == s.QuorumSize() {
			s.debugln("server.candidate.recv.enough.votes")
			s.setState(Leader)
			return
		}

		// 从其他节点获取返回结果.
		// 1、服务停止
		// 2、其他节点投票结果
		// 3、投票请求 \ 添加日志请求
		// 4、服务超时
		select {

		// 1
		case <-s.stopped:
			s.setState(Stopped)
			return

		// 2
		case resp := <-respChan:
			if success := s.processVoteResponse(resp); success {
				s.debugln("server.candidate.vote.granted: ", votesGranted)
				votesGranted++
			}

		// 3 在候选人阶段，有可能会收到投票消息、添加日志请求
		case e := <-s.c:
			var err error
			switch req := e.target.(type) {
			case Command:
				err = NotLeaderError

			// 收到AppendEntries消息
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)

			// 收到其他节点应答RequestVote消息
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}

			// Callback to event.
			e.c <- err

		// 4
		case <-timeoutChan:
			doVote = true
		}
	}
}

// The event loop that is run when the server is in a Leader state.
func (s *server) leaderLoop() {
	logIndex, _ := s.log.lastInfo()

	// Update the peers prevLogIndex to leader's lastLogIndex and start heartbeat.
	s.debugln("leaderLoop.set.PrevIndex to ", logIndex)
	for _, peer := range s.peers {
		peer.setPrevLogIndex(logIndex)
		peer.startHeartbeat()
	}

	// Commit a NOP after the server becomes leader. From the Raft paper:
	// "Upon election: send initial empty AppendEntries RPCs (heartbeat) to
	// each server; repeat during idle periods to prevent election timeouts
	// (§5.2)". The heartbeats started above do the "idle" period work.
	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.Do(NOPCommand{})
	}()

	// Begin to collect response from followers
	for s.State() == Leader {
		var err error
		select {
		case <-s.stopped:
			// Stop all peers before stop
			for _, peer := range s.peers {
				peer.stopHeartbeat(false)
			}
			s.setState(Stopped)
			return

		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				s.processCommand(req, e)
				continue
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *AppendEntriesResponse:
				s.processAppendEntriesResponse(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}

			// Callback to event.
			e.c <- err
		}
	}

	s.syncedPeer = nil
}

func (s *server) snapshotLoop() {
	for s.State() == Snapshotting {
		var err error
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return

		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				err = NotLeaderError
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			case *SnapshotRecoveryRequest:
				e.returnValue = s.processSnapshotRecoveryRequest(req)
			}
			// Callback to event.
			e.c <- err
		}
	}
}

//--------------------------------------
// Commands
//--------------------------------------

// Attempts to execute a command and replicate it. The function will return
// when the command has been successfully committed or an error has occurred.

func (s *server) Do(command Command) (interface{}, error) {
	return s.send(command)
}

// Processes a command.
func (s *server) processCommand(command Command, e *ev) {
	s.debugln("server.command.process")

	// Create an entry for the command in the log.
	entry, err := s.log.createEntry(s.currentTerm, command, e)

	if err != nil {
		s.debugln("server.command.log.entry.error:", err)
		e.c <- err
		return
	}

	if err := s.log.appendEntry(entry); err != nil {
		s.debugln("server.command.log.error:", err)
		e.c <- err
		return
	}

	s.syncedPeer[s.Name()] = true
	if len(s.peers) == 0 {
		commitIndex := s.log.currentIndex()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Appends zero or more log entry from the leader to this server.
func (s *server) AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*AppendEntriesResponse)
	return resp
}

// 可能涉及到的操作
// 1：任期检查   2：日志完备性检查   3：append log   4：提交日志
// 任期检查中，低于当前任期只是return false
func (s *server) processAppendEntriesRequest(req *AppendEntriesRequest) (*AppendEntriesResponse, bool) {
	s.traceln("server.ae.process")

	// 任期小于本节点，直接return false
	// 任何服务、任何时刻，任期值低于当前任期的数据都不可信
	if req.Term < s.currentTerm {
		s.debugln("server.ae.error: stale term")
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), false
	}

	// 任期相同
	// 说明有可能是当前节点的领导者发来的消息
	if req.Term == s.currentTerm {
		_assert(s.State() != Leader, "leader.elected.at.same.term.%d\n", s.currentTerm)

		// 如果是候选人状态，直接切换成follower
		if s.state == Candidate {
			s.setState(Follower)
		}

		// set leader
		s.leader = req.LeaderName

		// 任期比自己的大
	} else {
		// 更新自己的任期，停掉leader心跳，变成Follower
		s.updateCurrentTerm(req.Term, req.LeaderName)
	}

	// 判断leader是否合规：日志完整性
	if err := s.log.truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		s.debugln("server.ae.truncate.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// 判断leader是否合规：日志完整性
	if err := s.log.appendEntries(req.Entries); err != nil {
		s.debugln("server.ae.append.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// 提交日志.
	// append日志和提交日志可以同时进行
	if err := s.log.setCommitIndex(req.CommitIndex); err != nil {
		s.debugln("server.ae.commit.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// 日志提交成功后，返回
	return newAppendEntriesResponse(s.currentTerm, true, s.log.currentIndex(), s.log.CommitIndex()), true
}

// 可能涉及到的操作
// 1：判断当前任期   2：leader提交日志，写进磁盘
func (s *server) processAppendEntriesResponse(resp *AppendEntriesResponse) {
	// 如果返回的任期大于当前任期，更新自己的任期
	// 并切换成follwer
	if resp.Term() > s.Term() {
		s.updateCurrentTerm(resp.Term(), "")
		return
	}

	// panic response if it's not successful.
	if !resp.Success() {
		return
	}

	// if one peer successfully append a log from the leader term,
	// we add it to the synced list
	if resp.append == true {
		s.syncedPeer[resp.peer] = true
	}

	// Increment the commit count to make sure we have a quorum before committing.
	if len(s.syncedPeer) < s.QuorumSize() {
		return
	}

	// 寻找可提交日志的最大索引值
	// leader+follower，大多数节点的日志索引值的最大值
	var indices []uint64
	indices = append(indices, s.log.currentIndex())
	for _, peer := range s.peers {
		indices = append(indices, peer.getPrevLogIndex())
	}
	sort.Sort(sort.Reverse(uint64Slice(indices)))

	// 提交之前的日志(仅leader自己提交)
	commitIndex := indices[s.QuorumSize()-1]
	committedIndex := s.log.commitIndex

	if commitIndex > committedIndex {
		// 日志提交前要写入磁盘
		s.log.sync()
		// 日志提交
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}
}

// processVoteReponse processes a vote request:
// 1. if the vote is granted for the current term of the candidate, return true
// 2. if the vote is denied due to smaller term, update the term of this server
//    which will also cause the candidate to step-down, and return false.
// 3. if the vote is for a smaller term, ignore it and return false.
func (s *server) processVoteResponse(resp *RequestVoteResponse) bool {
	if resp.VoteGranted && resp.Term == s.currentTerm {
		return true
	}

	if resp.Term > s.currentTerm {
		s.debugln("server.candidate.vote.failed")
		s.updateCurrentTerm(resp.Term, "")
	} else {
		s.debugln("server.candidate.vote: denied")
	}
	return false
}

//--------------------------------------
// Request Vote
//--------------------------------------

// Requests a vote from a server. A vote can be obtained if the vote's term is
// at the server's current term and the server has not made a vote yet. A vote
// can also be obtained if the term is greater than the server's current term.
func (s *server) RequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*RequestVoteResponse)
	return resp
}

// 处理其他节点发起的投票请求
// 投票不成功，update = false
func (s *server) processRequestVoteRequest(req *RequestVoteRequest) (*RequestVoteResponse, bool) {

	// 其他节点任期低于当前节点任期，return false.
	// 任期低的肯定是不对的，想想为什么？
	if req.Term < s.Term() {
		s.debugln("server.rv.deny.vote: cause stale term")
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	// 如果其他节点任期大于当前任期，更新term，变身跟随者
	if req.Term > s.Term() {
		s.updateCurrentTerm(req.Term, "")

		// 如果对方term不比自己的大，且已经投过票了，return false
	} else if s.votedFor != "" && s.votedFor != req.CandidateName {
		s.debugln("server.deny.vote: cause duplicate vote: ", req.CandidateName,
			" already vote for ", s.votedFor)
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	// 查看投票者的最新日志，如果不够新，return false
	// 这里比对的是存储的日志（未提交状态）
	lastIndex, lastTerm := s.log.lastInfo()
	if lastIndex > req.LastLogIndex || lastTerm > req.LastLogTerm {
		s.debugln("server.deny.vote: cause out of date log: ", req.CandidateName,
			"Index :[", lastIndex, "]", " [", req.LastLogIndex, "]",
			"Term :[", lastTerm, "]", " [", req.LastLogTerm, "]")
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	s.debugln("server.rv.vote: ", s.name, " votes for", req.CandidateName, "at term", req.Term)

	// 投票
	s.votedFor = req.CandidateName
	return newRequestVoteResponse(s.currentTerm, true), true
}

//--------------------------------------
// Membership
//--------------------------------------

// Adds a peer to the server.
func (s *server) AddPeer(name string, connectiongString string) error {
	s.debugln("server.peer.add: ", name, len(s.peers))

	// Do not allow peers to be added twice.
	if s.peers[name] != nil {
		return nil
	}

	// Skip the Peer if it has the same name as the Server
	if s.name != name {
		peer := newPeer(s, name, connectiongString, s.heartbeatInterval)

		if s.State() == Leader {
			peer.startHeartbeat()
		}

		s.peers[peer.Name] = peer

		s.DispatchEvent(newEvent(AddPeerEventType, name, nil))
	}

	// Write the configuration to file.
	s.writeConf()

	return nil
}

// Removes a peer from the server.
func (s *server) RemovePeer(name string) error {
	s.debugln("server.peer.remove: ", name, len(s.peers))

	// Skip the Peer if it has the same name as the Server
	if name != s.Name() {
		// Return error if peer doesn't exist.
		peer := s.peers[name]
		if peer == nil {
			return fmt.Errorf("raft: Peer not found: %s", name)
		}

		// Stop peer and remove it.
		if s.State() == Leader {
			// We create a go routine here to avoid potential deadlock.
			// We are holding log write lock when reach this line of code.
			// Peer.stopHeartbeat can be blocked without go routine, if the
			// target go routine (which we want to stop) is calling
			// log.getEntriesAfter and waiting for log read lock.
			// So we might be holding log lock and waiting for log lock,
			// which lead to a deadlock.
			// TODO(xiangli) refactor log lock
			s.routineGroup.Add(1)
			go func() {
				defer s.routineGroup.Done()
				peer.stopHeartbeat(true)
			}()
		}

		delete(s.peers, name)

		s.DispatchEvent(newEvent(RemovePeerEventType, name, nil))
	}

	// Write the configuration to file.
	s.writeConf()

	return nil
}

//--------------------------------------
// Log compaction
//--------------------------------------

func (s *server) TakeSnapshot() error {
	if s.stateMachine == nil {
		return errors.New("Snapshot: Cannot create snapshot. Missing state machine.")
	}

	// Shortcut without lock
	// Exit if the server is currently creating a snapshot.
	if s.pendingSnapshot != nil {
		return errors.New("Snapshot: Last snapshot is not finished.")
	}

	// TODO: acquire the lock and no more committed is allowed
	// This will be done after finishing refactoring heartbeat
	s.debugln("take.snapshot")

	lastIndex, lastTerm := s.log.commitInfo()

	// check if there is log has been committed since the
	// last snapshot.
	if lastIndex == s.log.startIndex {
		return nil
	}

	path := s.SnapshotPath(lastIndex, lastTerm)
	// Attach snapshot to pending snapshot and save it to disk.
	s.pendingSnapshot = &Snapshot{lastIndex, lastTerm, nil, nil, path}

	state, err := s.stateMachine.Save()
	if err != nil {
		return err
	}

	// Clone the list of peers.
	peers := make([]*Peer, 0, len(s.peers)+1)
	for _, peer := range s.peers {
		peers = append(peers, peer.clone())
	}
	peers = append(peers, &Peer{Name: s.Name(), ConnectionString: s.connectionString})

	// Attach snapshot to pending snapshot and save it to disk.
	s.pendingSnapshot.Peers = peers
	s.pendingSnapshot.State = state
	s.saveSnapshot()

	// We keep some log entries after the snapshot.
	// We do not want to send the whole snapshot to the slightly slow machines
	if lastIndex-s.log.startIndex > NumberOfLogEntriesAfterSnapshot {
		compactIndex := lastIndex - NumberOfLogEntriesAfterSnapshot
		compactTerm := s.log.getEntry(compactIndex).Term()
		s.log.compact(compactIndex, compactTerm)
	}

	return nil
}

// Retrieves the log path for the server.
func (s *server) saveSnapshot() error {
	if s.pendingSnapshot == nil {
		return errors.New("pendingSnapshot.is.nil")
	}

	// Write snapshot to disk.
	if err := s.pendingSnapshot.save(); err != nil {
		return err
	}

	// Swap the current and last snapshots.
	tmp := s.snapshot
	s.snapshot = s.pendingSnapshot

	// Delete the previous snapshot if there is any change
	if tmp != nil && !(tmp.LastIndex == s.snapshot.LastIndex && tmp.LastTerm == s.snapshot.LastTerm) {
		tmp.remove()
	}
	s.pendingSnapshot = nil

	return nil
}

// Retrieves the log path for the server.
func (s *server) SnapshotPath(lastIndex uint64, lastTerm uint64) string {
	return path.Join(s.path, "snapshot", fmt.Sprintf("%v_%v.ss", lastTerm, lastIndex))
}

func (s *server) RequestSnapshot(req *SnapshotRequest) *SnapshotResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*SnapshotResponse)
	return resp
}

func (s *server) processSnapshotRequest(req *SnapshotRequest) *SnapshotResponse {
	// If the follower’s log contains an entry at the snapshot’s last index with a term
	// that matches the snapshot’s last term, then the follower already has all the
	// information found in the snapshot and can reply false.
	entry := s.log.getEntry(req.LastIndex)

	if entry != nil && entry.Term() == req.LastTerm {
		return newSnapshotResponse(false)
	}

	// Update state.
	s.setState(Snapshotting)

	return newSnapshotResponse(true)
}

func (s *server) SnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*SnapshotRecoveryResponse)
	return resp
}

func (s *server) processSnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse {
	// Recover state sent from request.
	if err := s.stateMachine.Recovery(req.State); err != nil {
		panic("cannot recover from previous state")
	}

	// Recover the cluster configuration.
	s.peers = make(map[string]*Peer)
	for _, peer := range req.Peers {
		s.AddPeer(peer.Name, peer.ConnectionString)
	}

	// Update log state.
	s.currentTerm = req.LastTerm
	s.log.updateCommitIndex(req.LastIndex)

	// Create local snapshot.
	s.pendingSnapshot = &Snapshot{req.LastIndex, req.LastTerm, req.Peers, req.State, s.SnapshotPath(req.LastIndex, req.LastTerm)}
	s.saveSnapshot()

	// Clear the previous log entries.
	s.log.compact(req.LastIndex, req.LastTerm)

	return newSnapshotRecoveryResponse(req.LastTerm, true, req.LastIndex)
}

// Load a snapshot at restart
func (s *server) LoadSnapshot() error {
	// Open snapshot/ directory.
	dir, err := os.OpenFile(path.Join(s.path, "snapshot"), os.O_RDONLY, 0)
	if err != nil {
		s.debugln("cannot.open.snapshot: ", err)
		return err
	}

	// Retrieve a list of all snapshots.
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		dir.Close()
		panic(err)
	}
	dir.Close()

	if len(filenames) == 0 {
		s.debugln("no.snapshot.to.load")
		return nil
	}

	// Grab the latest snapshot.
	sort.Strings(filenames)
	snapshotPath := path.Join(s.path, "snapshot", filenames[len(filenames)-1])

	// Read snapshot data.
	file, err := os.OpenFile(snapshotPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	// Check checksum.
	var checksum uint32
	n, err := fmt.Fscanf(file, "%08x\n", &checksum)
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("checksum.err: bad.snapshot.file")
	}

	// Load remaining snapshot contents.
	b, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	// Generate checksum.
	byteChecksum := crc32.ChecksumIEEE(b)
	if uint32(checksum) != byteChecksum {
		s.debugln(checksum, " ", byteChecksum)
		return errors.New("bad snapshot file")
	}

	// Decode snapshot.
	if err = json.Unmarshal(b, &s.snapshot); err != nil {
		s.debugln("unmarshal.snapshot.error: ", err)
		return err
	}

	// Recover snapshot into state machine.
	if err = s.stateMachine.Recovery(s.snapshot.State); err != nil {
		s.debugln("recovery.snapshot.error: ", err)
		return err
	}

	// Recover cluster configuration.
	for _, peer := range s.snapshot.Peers {
		s.AddPeer(peer.Name, peer.ConnectionString)
	}

	// Update log state.
	s.log.startTerm = s.snapshot.LastTerm
	s.log.startIndex = s.snapshot.LastIndex
	s.log.updateCommitIndex(s.snapshot.LastIndex)

	return err
}

//--------------------------------------
// Config File
//--------------------------------------

// Flushes commit index to the disk.
// So when the raft server restarts, it will commit upto the flushed commitIndex.
func (s *server) FlushCommitIndex() {
	s.debugln("server.conf.update")
	// Write the configuration to file.
	s.writeConf()
}

func (s *server) writeConf() {

	peers := make([]*Peer, len(s.peers))

	i := 0
	for _, peer := range s.peers {
		peers[i] = peer.clone()
		i++
	}

	r := &Config{
		CommitIndex: s.log.commitIndex,
		Peers:       peers,
	}

	b, _ := json.Marshal(r)

	confPath := path.Join(s.path, "conf")
	tmpConfPath := path.Join(s.path, "conf.tmp")

	err := writeFileSynced(tmpConfPath, b, 0600)

	if err != nil {
		panic(err)
	}

	os.Rename(tmpConfPath, confPath)
}

// Read the configuration for the server.
func (s *server) readConf() error {
	confPath := path.Join(s.path, "conf")
	s.debugln("readConf.open ", confPath)

	// open conf file
	b, err := ioutil.ReadFile(confPath)

	if err != nil {
		return nil
	}

	conf := &Config{}

	if err = json.Unmarshal(b, conf); err != nil {
		return err
	}

	s.log.updateCommitIndex(conf.CommitIndex)

	return nil
}

//--------------------------------------
// Debugging
//--------------------------------------

func (s *server) debugln(v ...interface{}) {
	if logLevel > Debug {
		debugf("[%s Term:%d] %s", s.name, s.Term(), fmt.Sprintln(v...))
	}
}

func (s *server) traceln(v ...interface{}) {
	if logLevel > Trace {
		tracef("[%s] %s", s.name, fmt.Sprintln(v...))
	}
}
