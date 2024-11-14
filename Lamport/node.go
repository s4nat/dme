package Lamport

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	REQUEST = "REQUEST"
	REPLY   = "REPLY"
	RELEASE = "RELEASE"
)

type Node struct {
	id           int
	logicalClock int
	nodeChannel  chan Message
	pq           []Message
	ptrMap       map[int]*Node
	//A map to track the replies the node has received for its own request stamped with timestamp t
	//{requestTimeStamp: {nodeId: True} etc}
	replyTracker map[int]map[int]bool

	//Keep tracking of pending replies
	pendingReplies map[int][]Message
	globalWG       *sync.WaitGroup
	quit           chan int
}

type Message struct {
	Type      string
	FromID    int
	Timestamp int
	//[id, timestamp]
	replyTarget ReplyTarget
}

type ReplyTarget struct {
	targetID  int
	timestamp int
}

func (n *Node) pqPush(newMsg Message) {
	for _, msg := range n.pq {
		if newMsg.FromID == msg.FromID {
			return
		}
	}
	n.pq = append(n.pq, newMsg)
	sort.SliceStable(n.pq, func(i, j int) bool {
		if n.pq[i].Timestamp < n.pq[j].Timestamp {
			return true
		} else if n.pq[i].Timestamp == n.pq[j].Timestamp && n.pq[i].FromID < n.pq[j].FromID {
			return true
		} else {
			return false
		}
	})
}

func (n *Node) pqPop(fromID int) {
	for i, msg := range n.pq {
		if msg.FromID == fromID {
			if i >= len(n.pq)-1 { //Handle edge case where it's the only element or last element
				n.pq = n.pq[:i]

			} else {
				n.pq = append(n.pq[:i], n.pq[i+1:]...)
			}
			return
		}
	}
}

func (n *Node) requestCS() {
	n.logicalClock += 1

	if NUM_NODES == 1 {
		n.enterCS(Message{
			Type:        "Request",
			FromID:      n.id,
			Timestamp:   n.logicalClock,
			replyTarget: ReplyTarget{},
		})
	}

	fmt.Printf("=======================================\n Node %d is requesting to enter the CS \n=======================================\n", n.id)

	// time.Sleep(time.Duration(500) * time.Millisecond)

	requestMsg := Message{
		Type:      REQUEST,
		FromID:    n.id,
		Timestamp: n.logicalClock,
	}

	n.pqPush(requestMsg)
	otherNodes := map[int]bool{}
	for nodeId, _ := range n.ptrMap {
		if nodeId == n.id {
			continue
		}
		otherNodes[nodeId] = false
	}

	// Set replies for this request as False for all otherNodes
	n.replyTracker[n.logicalClock] = otherNodes
	n.broadcastMessage(requestMsg)
}

// Enter criticial section
func (n *Node) enterCS(msg Message) {
	n.pqPop(msg.FromID) //dequeue at the start to avoid race conditions
	fmt.Printf("[Node %d] <Entering CS> PQ: %s \n", n.id, stringPQ(n.pq))
	//msg should be the request that is being granted the CS now

	//Simulate a random duration for the CS
	numSeconds := 1
	fmt.Printf("[Node %d] Entering critical section for %d seconds for CSRequest with timestamp %d \n", n.id, numSeconds, msg.Timestamp)
	fmt.Printf("========== [Node %d] Executing CS ==========\n", n.id)
	time.Sleep(time.Duration(numSeconds) * time.Second)
	fmt.Printf("[Node %d] Finished critical section in %d seconds \n", n.id, numSeconds)
	n.logicalClock += 1
	releaseMessage := Message{
		Type:        RELEASE,
		FromID:      n.id,
		Timestamp:   n.logicalClock,
		replyTarget: ReplyTarget{},
	}
	n.globalWG.Done()
	n.broadcastMessage(releaseMessage)
}

func (n *Node) replyMessage(receivedMsg Message) {
	fmt.Printf("Node %d is replying Node %d \n", n.id, receivedMsg.FromID)
	n.logicalClock += 1
	replyMessage := Message{
		Type:      REPLY,
		FromID:    n.id,
		Timestamp: n.logicalClock,
		replyTarget: ReplyTarget{
			targetID:  receivedMsg.FromID,
			timestamp: receivedMsg.Timestamp,
		},
	}

	n.sendMessage(replyMessage, receivedMsg.FromID)
}

func (n *Node) handleRequest(msg Message) {
	// Check if the node has received a reply from the sender for an earlier request.
	var replied bool = false
	if len(n.replyTracker) == 0 {
		go n.replyMessage(msg)
		replied = true
	}
	for requestTS, replyMap := range n.replyTracker {
		if requestTS < msg.Timestamp {
			//Received the necessary reply
			if replyMap[msg.FromID] {
				go n.replyMessage(msg)
				replied = true
			}

		} else if requestTS == msg.Timestamp && n.id < msg.FromID {
			//Tiebreaker - there is a higher priority request AND ascertained that we received the necessary reply
			if replyMap[msg.FromID] {
				go n.replyMessage(msg)
				replied = true
			}
		} else {
			//	no earlier request
			go n.replyMessage(msg)
			replied = true
		}
	}
	if !replied {
		//Add to a map of pending replies
		n.pendingReplies[msg.FromID] = append(n.pendingReplies[msg.FromID], msg)
	} else {
		n.pqPush(msg)
	}

	fmt.Printf("Node %d's PQ: %s \n", n.id, stringPQ(n.pq))
}

func (n *Node) handleReply(msg Message) {
	//Keep track in the replyTracker
	ts := msg.replyTarget.timestamp
	//if the ts does not exist in the replyTracker, create a entry for it
	if _, ok := n.replyTracker[ts]; !ok {
		n.replyTracker[ts] = n.getEmptyReplyMap()
	}
	n.replyTracker[ts][msg.FromID] = true
	//Need to check if the node that replied has a pending request
	for _, reqMsg := range n.pendingReplies[msg.FromID] {
		fmt.Printf("[Node %d] Can now reply the request from [Node %d] \n", n.id, reqMsg.FromID)
		n.handleRequest(reqMsg)
	}
	// Check if everyone has replied this node
	if n.allReplied(msg.replyTarget.timestamp) {

		fmt.Printf("[Node %d] All replies have been received for Request with TS: %d \n", n.id, msg.replyTarget.timestamp)
		firstRequest := n.pq[0]
		if firstRequest.FromID == n.id && firstRequest.Timestamp == msg.replyTarget.timestamp {
			fmt.Println(firstRequest.FromID, n.id)
			fmt.Printf("[Node %d] Request with timestamp %d is also at the front of the queue. \n[Node %d] will "+
				"now enter the CS. \n", n.id, msg.replyTarget.timestamp, n.id)

			//reset
			delete(n.replyTracker, msg.replyTarget.timestamp)
			n.enterCS(firstRequest)
		}
	}
}

func (n *Node) handleRelease(msg Message) {
	n.pqPop(msg.FromID)

	if len(n.pq) > 0 {
		firstRequest := n.pq[0]
		if firstRequest.FromID == n.id {
			if n.allReplied(n.pq[0].Timestamp) {
				//reset
				delete(n.replyTracker, n.pq[0].Timestamp)
				n.enterCS(firstRequest)
			}
		}
	}
}

func (n *Node) handleIncomingMessage(msg Message) {
	// time.Sleep(time.Duration(50) * time.Millisecond)
	//Taking the max(self.logicalClock, msg.timestamp) + 1
	if msg.Timestamp >= n.logicalClock {
		n.logicalClock = msg.Timestamp + 1
	} else {
		n.logicalClock += 1
	}

	fmt.Printf("[Node %d] Received a <%s> Message from Node %d \n", n.id, msg.Type, msg.FromID)
	switch mType := msg.Type; {
	case mType == REQUEST:
		n.handleRequest(msg)

	case mType == REPLY:
		n.handleReply(msg)

	case mType == RELEASE:
		n.handleRelease(msg)
	}
}

func (n *Node) listen() {
	for {
		select {
		case msg := <-n.nodeChannel:
			n.handleIncomingMessage(msg)
		case <-n.quit:
			return
		}
	}
}
