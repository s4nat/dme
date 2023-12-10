package Voting

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"

	"time"
)

func MaxOf(x int, y int) int {
	if y > x {
		return y
	} else {
		return x
	}
}

func isEarlier(t1 [NUM_NODES]int, t2 [NUM_NODES]int) bool {
	//t1 earlier than t2
	for i, _ := range t1 {
		if t2[i] < t1[i] {
			return false
		}
	}
	return true
}

func areConcurrent(t1 [NUM_NODES]int, t2 [NUM_NODES]int) bool {
	t1Ahead := false
	t2Ahead := false

	for i, _ := range t1 {
		if t1[i] > t2[i] {
			t1Ahead = true
		}
		if t2[i] > t1[i] {
			t2Ahead = true
		}
		if t1Ahead && t2Ahead {
			return true
		}
	}
	return false
}

func timestampsToString(timestamps [NUM_NODES]int) string {
	result := ""
	for _, timestamp := range timestamps {
		result += strconv.Itoa(timestamp) + " "
	}
	return result
}

func converge(clientid int, t1 [NUM_NODES]int, t2 [NUM_NODES]int) [NUM_NODES]int {
	result := [NUM_NODES]int{}
	for i, v := range t1 {
		result[i] = MaxOf(v, t2[i])
	}
	result[clientid] += 1
	return result
}

func (node *Node) pop(senderId int) {
	for i, msg := range node.PQ {
		if msg.FromID == senderId {
			if i < len(node.PQ)-1 {
				node.PQ = append(node.PQ[:i], node.PQ[i+1:]...)
			} else {
				node.PQ = node.PQ[:i]
			}
			return
		}
	}
}

func (node *Node) enqueue(reqMsg Message) {
	fmt.Printf("[Node %d] Queue Image Before Req Addition: %s \n", node.ID, printPQ(node.PQ))

	for _, msg := range node.PQ {
		if reqMsg.FromID == msg.FromID {
			return
		}
	}

	node.PQ = append(node.PQ, reqMsg)

	sort.SliceStable(node.PQ, func(i, j int) bool {
		if isEarlier(node.PQ[i].Timestamp, node.PQ[j].Timestamp) {
			return true
		} else if node.PQ[j].FromID < node.PQ[i].FromID && areConcurrent(node.PQ[i].Timestamp, node.PQ[j].Timestamp) {
			return true
		} else {
			return false
		}
	})

	fmt.Printf("[Node %d] Queue Image Post Req Addition: %s \n", node.ID, printPQ(node.PQ))
}

func printPQ(pq []Message) string {
	if len(pq) == 0 {
		return "PQ: | Empty |"
	}
	var ret string = "|"
	for _, msg := range pq {
		ret += fmt.Sprintf("PQ: [TS: %d] by Node %d| ", msg.Timestamp, msg.FromID)
	}
	return ret
}

func (node *Node) send(msg Message, receiverId int) {
	fmt.Printf("[Node %d] Sending a <%s> message to Node %d\n", node.ID,
		msg.Type, receiverId)

	randTime := rand.Intn(500) + 500
	time.Sleep(time.Duration(randTime) * time.Millisecond)

	receiver := node.NodeMap[receiverId]
	receiver.NodeChannel <- msg
}

func (node *Node) broadcast(msg Message) {
	for id, _ := range node.NodeMap {
		if id == node.ID {
			continue
		}
		go node.send(msg, id)
	}
}

func (node *Node) getEmptyReplyMap() map[int]bool {
	ret := map[int]bool{}
	for i, _ := range node.NodeMap {
		if i == node.ID {
			continue
		}
		ret[i] = false
	}
	return ret
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
