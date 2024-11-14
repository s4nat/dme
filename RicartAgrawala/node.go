package RicartAgrawala

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	REQUEST      = "REQUEST"
	REPLY        = "REPLY"
	RELEASE      = "RELEASE"
	RELEASEREPLY = "RELEASEREPLY"
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
			Type:        REQUEST,
			FromID:      n.id,
			Timestamp:   n.logicalClock,
			replyTarget: ReplyTarget{},
		})
	}
	fmt.Printf("=======================================\n Node %d is requesting to enter the CS \n =======================================\n", n.id)

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
	n.replyTracker[n.logicalClock] = otherNodes
	n.broadcastMessage(requestMsg)
}

// Enter criticial section
func (n *Node) enterCS(msg Message) {
	//Should have at least 1 request
	fmt.Printf("[Node %d] <Entering CS> %s \n", n.id, stringPQ(n.pq))
	n.pqPop(msg.FromID) //dequeue at the start to avoid race conditions
	//msg should be the request that is being granted the CS now

	//Simulate a random duration for the CS
	numSeconds := 1
	fmt.Printf("[Node %d] Entering critical section for %d seconds for msg with priority %d \n", n.id, numSeconds, msg.Timestamp)
	fmt.Println("==========Executing CS==========")
	time.Sleep(time.Duration(numSeconds) * time.Second)
	fmt.Printf("[Node %d] Finished critical section in %d seconds \n", n.id, numSeconds)
	n.logicalClock += 1
	n.globalWG.Done()
	fmt.Printf("[Node %d] After finishing CS, PQ: %s \n", n.id, stringPQ(n.pq))
	for _, reqMsg := range n.pq {
		releaseMessage := Message{
			Type:        RELEASEREPLY,
			FromID:      n.id,
			Timestamp:   n.logicalClock,
			replyTarget: ReplyTarget{reqMsg.FromID, reqMsg.Timestamp},
		}

		n.sendMessage(releaseMessage, reqMsg.FromID)
	}
	//Empty the PQ
	n.pq = []Message{}
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
	//Check if i has received a reply from machine j for an earlier request
	// assert that the request's messageType = 0
	// time.Sleep(time.Duration(50) * time.Millisecond)
	var earlierRequestExists bool = false

	for _, requestMsg := range n.pq {
		//If it is the node's own message
		if requestMsg.FromID == n.id {
			if requestMsg.Timestamp < msg.Timestamp {
				earlierRequestExists = true
				break
			} else if requestMsg.Timestamp == msg.Timestamp && requestMsg.FromID < msg.FromID {
				earlierRequestExists = true
				break
			}
		}
	}

	if earlierRequestExists {
		n.pqPush(msg)
	} else {
		go n.replyMessage(msg)
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
	n.replyTracker[msg.replyTarget.timestamp][msg.FromID] = true
	// Check if everyone has replied this node
	if n.allReplied(msg.replyTarget.timestamp) {
		//reset
		delete(n.replyTracker, msg.replyTarget.timestamp)
		fmt.Printf("[Node %d] All replies have been received for Request with TS: %d \n", n.id, msg.replyTarget.timestamp)
		firstRequest := n.pq[0]
		n.enterCS(firstRequest)
	}
}

func (n *Node) handleReleaseReply(msg Message) {
	//Mark the reply map
	ts := msg.replyTarget.timestamp
	//if the ts does not exist in the replyTracker, create a entry for it
	if _, ok := n.replyTracker[ts]; !ok {
		n.replyTracker[ts] = n.getEmptyReplyMap()
	}
	n.replyTracker[msg.replyTarget.timestamp][msg.FromID] = true
	fmt.Printf("[Node %d] PQ: %s \n", n.id, stringPQ(n.pq))
	if n.allReplied(n.pq[0].Timestamp) {
		//Reset
		delete(n.replyTracker, msg.replyTarget.timestamp)
		n.enterCS(n.pq[0])
	}
}

func (n *Node) handleIncomingMessage(msg Message) {

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

	case mType == RELEASEREPLY:
		n.handleReleaseReply(msg)
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
