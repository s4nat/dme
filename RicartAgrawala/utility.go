package RicartAgrawala

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

func stringPQ(pq []Message) string {
	if len(pq) == 0 {
		return "PQ: | Empty |"
	}
	var ret = "|"
	for _, msg := range pq {
		ret += fmt.Sprintf("PQ: [TS: %d] by Node %d| ", msg.Timestamp, msg.FromID)
	}
	return ret
}

func (n *Node) allReplied(timestamp int) bool {
	//Ensure the edge case that if the timestamp does not even exist, we do not considered that it has received all its reply
	if _, ok := n.replyTracker[timestamp]; !ok {
		return false
	}
	for _, replyStatus := range n.replyTracker[timestamp] {
		if !replyStatus {
			return false
		}
	}
	return true
}

func (n *Node) getEmptyReplyMap() map[int]bool {
	ret := map[int]bool{}
	for i, _ := range n.ptrMap {
		if i == n.id {
			continue
		}
		ret[i] = false
	}
	return ret
}

func (n *Node) broadcastMessage(msg Message) {
	for nodeId, _ := range n.ptrMap {
		if nodeId == n.id {
			continue
		}
		go n.sendMessage(msg, nodeId)
	}
}

func (n *Node) sendMessage(msg Message, receiverID int) {
	fmt.Printf("[Node %d] Sending a <%s> message to Node %d at MemAddr %p \n", n.id,
		msg.Type, receiverID, n.ptrMap[receiverID])
	//Simulate uncertain latency and asynchronous nature of message passing
	numMilliSeconds := rand.Intn(1000)
	time.Sleep(time.Duration(numMilliSeconds) * time.Millisecond)
	receiver := n.ptrMap[receiverID]
	receiver.nodeChannel <- msg
}

// Constructors
func NewNode(id int) *Node {
	channel := make(chan Message)
	var pq []Message
	//Create a blank map to track
	var replyTracker = map[int]map[int]bool{}
	n := Node{id, 0, channel, pq, nil, replyTracker, map[int][]Message{}, &sync.WaitGroup{}, make(chan int)}

	return &n
}

func (n *Node) setPtrMap(ptrMap map[int]*Node) {
	n.ptrMap = ptrMap
}

// Function to get user input with a prompt
func getUserInput(prompt string) int {
	fmt.Print(prompt)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	inputStr := scanner.Text()

	// Convert the input to an integer
	num, err := strconv.Atoi(inputStr)
	if err != nil {
		fmt.Println("Error: Invalid input. Please enter a valid number.")
		return getUserInput(prompt)
	}

	return num
}
