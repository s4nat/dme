package Voting

import (
	"fmt"
	"sync"
	"time"
)

const (
	REQUEST = "REQUEST"
	VOTE    = "VOTE"
	RELEASE = "RELEASE"
	RESCIND = "RESCIND"
)

type Node struct {
	ID          int
	LocalClock  [NUM_NODES]int
	NodeChannel chan Message
	PQ          []Message
	NodeMap     map[int]*Node
	ReplyMap    map[string]map[int]bool
	Status      map[string]CSStatus
	Vote        Message
	wg          *sync.WaitGroup
	quit        chan int
}

type CSStatus string

const (
	REQ  CSStatus = "REQ"
	EXE  CSStatus = "EXEC"
	DONE CSStatus = "DONE"
)

type Message struct {
	Timestamp   [NUM_NODES]int
	FromID      int
	Type        string
	ReplyTarget ReplyTarget
	Status      string
}

type ReplyTarget struct {
	targetId  int
	timestamp [NUM_NODES]int
}

func NewNode(id int) *Node {
	nodeChan := make(chan Message)
	pq := []Message{}
	replyMap := make(map[string]map[int]bool)
	status := make(map[string]CSStatus)
	quit := make(chan int)
	node := Node{
		ID:          id,
		LocalClock:  [NUM_NODES]int{},
		NodeChannel: nodeChan,
		PQ:          pq,
		ReplyMap:    replyMap,
		Status:      status,
		quit:        quit,
	}

	return &node
}

func NewMessage(messageType string, fromId int, timestamp [NUM_NODES]int) *Message {
	replyTarget := ReplyTarget{-1, [NUM_NODES]int{}}
	msg := Message{Timestamp: timestamp,
		FromID:      fromId,
		Type:        messageType,
		ReplyTarget: replyTarget,
		Status:      REQUEST}

	return &msg
}

func (node *Node) enterCS(msg Message) {
	defer node.wg.Done()
	start := timestampsToString(msg.Timestamp)
	node.Status[start] = EXE
	node.pop(msg.FromID)

	fmt.Printf("[Node %d] <Entering CS> PQ: %s at local time %d\n", node.ID, printPQ(node.PQ), node.LocalClock)
	numSeconds := 500
	fmt.Printf("[Node %d] Entering critical section for %d milliseconds for msg with priority %d \n", node.ID, numSeconds, msg.Timestamp)
	fmt.Printf("==================[NODE %d] EXECUTING CS==================\n", node.ID)
	time.Sleep(time.Duration(numSeconds) * time.Millisecond)
	fmt.Printf("[Node %d] Finished critical section in %dms \n", node.ID, numSeconds)
	node.LocalClock = converge(node.ID, node.LocalClock, msg.Timestamp)

	if _, ok := node.ReplyMap[start]; !ok {
		node.ReplyMap[start] = node.getEmptyReplyMap()
	}

	for id, req := range node.ReplyMap[start] {
		if id != node.ID {
			if req {
				releaseMsg := NewMessage(RELEASE, node.ID, node.LocalClock)
				releaseMsg.Status = "DONE"
				releaseMsg.ReplyTarget.targetId = node.ID
				releaseMsg.ReplyTarget.timestamp = msg.Timestamp
				node.send(*releaseMsg, id)
			}
		}
	}

	delete(node.ReplyMap, start)
	node.Status[start] = DONE

	if len(node.PQ) > 0 {
		head := node.PQ[0]
		node.Vote = head
		go node.reply(head)
	}

}

func (node *Node) reply(msg Message) {
	fmt.Printf("[Node: %d] Replying to Node %d Request to enter CS\n", node.ID, msg.FromID)
	node.LocalClock = converge(node.ID, node.LocalClock, msg.Timestamp)
	replyMsg := NewMessage(VOTE, node.ID, node.LocalClock)

	replyMsg.ReplyTarget.targetId = msg.FromID
	replyMsg.ReplyTarget.timestamp = msg.Timestamp

	node.Vote = msg

	node.pop(msg.FromID)
	node.send(*replyMsg, msg.FromID)
}

func (node *Node) requestCS() {
	node.LocalClock[node.ID] += 1

	reqMsg := NewMessage(REQUEST, node.ID, node.LocalClock)

	if NUM_NODES == 1 {
		node.enterCS(Message{
			node.LocalClock,
			node.ID,
			REQUEST,
			ReplyTarget{},
			"",
		})
	}

	fmt.Printf("****************************\n Node %d has requested to enter Critical Section \n****************************\n", node.ID)
	time.Sleep(time.Duration(500) * time.Millisecond)

	pendingReplyMap := map[int]bool{}
	timeStr := timestampsToString(node.LocalClock)
	node.Status[timeStr] = REQ
	for id := range node.NodeMap {
		if id == node.ID {
			if node.Vote == (Message{}) {
				pendingReplyMap[id] = true
				node.Vote = *reqMsg
			} else {
				node.enqueue(*reqMsg)
			}
		} else {
			pendingReplyMap[id] = false
		}
	}

	node.ReplyMap[timeStr] = pendingReplyMap
	node.broadcast(*reqMsg)
}

// Check if Majority nodes have replied to the CS Request at the given timestamp
func (node *Node) replyCheck(timestamp [NUM_NODES]int) bool {
	timestampStr := timestampsToString(timestamp)
	total := 0

	if _, ok := node.ReplyMap[timestampStr]; !ok {
		return false
	}
	for _, replyStatus := range node.ReplyMap[timestampStr] {
		if replyStatus {
			total += 1
		}
	}
	return total >= NUM_NODES/2+1
}

func (node *Node) rescind(targetId int, timestamp [NUM_NODES]int) {
	fmt.Printf("[Node: %d] Rescinding Vote\n", node.ID)
	node.LocalClock[node.ID] += 1
	replyMsg := NewMessage(RESCIND, node.ID, node.LocalClock)

	replyMsg.ReplyTarget.targetId = targetId
	replyMsg.ReplyTarget.timestamp = timestamp

	node.send(*replyMsg, targetId)
}

func (node *Node) processRequest(msg Message) {
	fmt.Printf("[Node: %d] Recieved request from Node %d for Timestamp %d\n", node.ID, msg.FromID, msg.Timestamp)
	var earlierReq bool = false

	fmt.Printf("[Node: %d] Current Vote %d and TS: %d\n", node.ID, node.Vote.FromID, node.Vote.Timestamp)
	for _, prevReqMsg := range node.PQ {
		if isEarlier(msg.Timestamp, prevReqMsg.Timestamp) {
			earlierReq = true
			break
		} else if areConcurrent(prevReqMsg.Timestamp, msg.Timestamp) && prevReqMsg.FromID < msg.FromID {
			earlierReq = true
			break
		}
	}

	if isEarlier(msg.Timestamp, node.Vote.Timestamp) {
		earlierReq = true
	} else if areConcurrent(node.Vote.Timestamp, msg.Timestamp) && node.Vote.FromID < msg.FromID {
		earlierReq = true
	}

	fmt.Printf("[Node: %d] Request from Node %d at TS %d is the Earlier Request: %t\n", node.ID, msg.FromID, msg.Timestamp, earlierReq)
	// If the incoming request is earlier than the previous request
	if earlierReq {
		// and if it has already voted for one of the previous requests
		if node.Vote != (Message{}) {
			// not self vote
			if node.Vote.FromID != node.ID {
				// rescind that previous vote
				node.rescind(node.Vote.FromID, node.Vote.Timestamp)
			} else {
				// if it is a self vote, rescind the self vote
				node.ReplyMap[timestampsToString(node.Vote.Timestamp)][node.ID] = false
			}
			fmt.Printf("[Node: %d] Current Vote: %d %d\n", node.ID, node.Vote.FromID, node.Vote.Timestamp)
			node.enqueue(Message{node.Vote.Timestamp, node.Vote.FromID, REQUEST, ReplyTarget{}, REQUEST})

			// set the current vote to Nil
			node.Vote = Message{}
		}
		go node.reply(msg)
	} else {
		// if the incoming request is not earlier than the previous requests, put it in the queue.
		node.enqueue(msg)
	}

	fmt.Printf("[Node: %d] PQ: %s \n", node.ID, printPQ(node.PQ))
}

func (node *Node) processReply(msg Message) {
	msgTime := msg.ReplyTarget.timestamp
	msgTimeStr := timestampsToString(msgTime)

	fmt.Printf("[Node: %d] The priority queue: %s\n", node.ID, printPQ(node.PQ))

	if _, ok := node.ReplyMap[msgTimeStr]; !ok {
		node.ReplyMap[msgTimeStr] = node.getEmptyReplyMap()
	}
	if node.Status[msgTimeStr] == EXE || node.Status[msgTimeStr] == DONE {
		releaseMsg := NewMessage(RELEASE, node.ID, node.LocalClock)
		node.send(*releaseMsg, msg.ReplyTarget.targetId)
		return
	}
	node.ReplyMap[msgTimeStr][msg.FromID] = true
	if node.Vote == (Message{}) {
		node.Vote = *NewMessage(REQUEST, node.ID, msgTime)
	}

	if node.replyCheck(msg.ReplyTarget.timestamp) {
		fmt.Printf("[Node %d]  Request with TS: %d has recieved majority replies \n", node.ID, msg.ReplyTarget.timestamp)
		head := node.PQ[0]
		if head.Timestamp == msgTime && node.Status[msgTimeStr] == REQ {
			node.enterCS(head)
		} else if node.Status[msgTimeStr] == REQ {
			node.enterCS(Message{msg.ReplyTarget.timestamp, node.ID, REQUEST, ReplyTarget{}, REQUEST})
		}
	}
}

func (node *Node) processRelease(msg Message) {

	if CSStatus(msg.Status) == REQ {
		node.enqueue(node.Vote)
	}

	node.Vote = Message{}

	if len(node.PQ) > 0 {
		head := node.PQ[0]
		if head.FromID != node.ID {
			go node.reply(head)
		} else {
			timeStr := timestampsToString(head.Timestamp)
			node.ReplyMap[timeStr][node.ID] = true
			if node.replyCheck(head.Timestamp) {
				fmt.Printf("[Node %d]  Request with TS: %d has recieved majority replies \n", node.ID, msg.ReplyTarget.timestamp)
				//delete(node.replyMap, timestampsToString(head.timestamp))
				node.enterCS(head)
			}
		}
	}

}

func (node *Node) processRescind(msg Message) {
	timeStr := timestampsToString(msg.ReplyTarget.timestamp)
	node.LocalClock = converge(node.ID, node.LocalClock, msg.Timestamp)
	if node.Status[timeStr] == EXE || node.Status[timeStr] == DONE {
		return
	} else {
		node.ReplyMap[timeStr][msg.FromID] = false
		releaseMsg := NewMessage(RELEASE, node.ID, node.LocalClock)
		releaseMsg.Status = REQUEST
		releaseMsg.ReplyTarget.targetId = node.ID
		releaseMsg.ReplyTarget.timestamp = msg.ReplyTarget.timestamp

		go node.send(*releaseMsg, msg.FromID)
	}
}

func (node *Node) handleIncomingMessage(msg Message) {
	time.Sleep(time.Duration(50) * time.Millisecond)
	node.LocalClock = converge(node.ID, node.LocalClock, msg.Timestamp)

	fmt.Printf("[Node %d] Received a <%s> Message from Node %d \n", node.ID, msg.Type, msg.FromID)

	switch msg.Type {
	case REQUEST:
		node.processRequest(msg)

	case VOTE:
		node.processReply(msg)

	case RELEASE:
		node.processRelease(msg)

	case RESCIND:
		node.processRescind(msg)
	}
}

func (node *Node) chanListen() {
	for {
		select {
		case msg := <-node.NodeChannel:
			go node.handleIncomingMessage(msg)
		case <-node.quit:
			return
		}
	}
}
