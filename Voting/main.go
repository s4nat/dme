package Voting

import (
	"fmt"
	"sync"
	"time"
)

const NUM_NODES = 10

func VotingManual() {
	var wg sync.WaitGroup

	fmt.Printf("**************************************************\n  VOTING PROTOCOL  \n**************************************************\n")
	fmt.Printf("The network will have %d clients. You will be prompted to enter the number of nodes you want to concurrently request CS.\n", NUM_NODES)

	time.Sleep(time.Duration(1) * time.Second)

	globalMap := map[int]*Node{}
	for i := 0; i < NUM_NODES; i++ {
		node := NewNode(i)
		globalMap[i] = node
	}

	for i := 0; i < NUM_NODES; i++ {
		globalMap[i].NodeMap = globalMap
		globalMap[i].wg = &wg
	}

	for i := 0; i < NUM_NODES; i++ {
		go globalMap[i].chanListen()
	}

	// Get user input for the number of nodes
	numNodes := getUserInput("Enter the number of nodes to concurrently request CS: ")

	// Validate the user input
	if numNodes > NUM_NODES {
		fmt.Printf("Error: Number of nodes must be less than total no. of nodes, %d\n", NUM_NODES)
		return
	}
	fmt.Printf("You have chosen to request CS in %d nodes.\n", numNodes)

	start := time.Now()
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go globalMap[i].requestCS()
	}
	wg.Wait()
	finish := time.Now()
	time.Sleep(time.Duration(1) * time.Second)
	//fmt.Printf("Number of nodes: %d \n", NUM_NODES)
	fmt.Printf("Time Taken: %.2f seconds \n", finish.Sub(start).Seconds())
	fmt.Printf("All Nodes have entered entered and exited the Critical Section\n**********************\n")
	for i := 0; i < NUM_NODES; i++ {
		globalMap[i].quit <- 1
	}
}

func VotingRun() (result []float64) {
	for n := 1; n <= NUM_NODES; n++ {
		if n == 1 {
			result = append(result, 0)
			continue
		}

		fmt.Println("\n\n#######################################################")
		fmt.Println("#######################################################")
		fmt.Println("#######################################################")
		fmt.Printf("Number of Nodes concurrently requesting CS: %d\n", n)
		fmt.Println("#######################################################")
		fmt.Println("#######################################################")
		fmt.Println("#######################################################")

		var wg sync.WaitGroup
		globalMap := map[int]*Node{}
		for i := 0; i < NUM_NODES; i++ {
			node := NewNode(i)
			globalMap[i] = node
		}

		for i := 0; i < NUM_NODES; i++ {
			globalMap[i].NodeMap = globalMap
			globalMap[i].wg = &wg
		}

		for i := 0; i < NUM_NODES; i++ {
			go globalMap[i].chanListen()
		}

		start := time.Now()
		for i := 0; i < n; i++ {
			wg.Add(1)
			go globalMap[i].requestCS()
		}
		wg.Wait()
		end := time.Now()
		timeTaken := end.Sub(start).Seconds()
		result = append(result, timeTaken)
		// time.Sleep(time.Duration(1) * time.Second)
		//fmt.Printf("Number of nodes: %d \n", NUM_NODES)
		fmt.Printf("Time Taken: %.2f seconds \n", timeTaken)
		fmt.Printf("All Nodes have entered entered and exited the Critical Section\n**********************\n")
		for i := 0; i < NUM_NODES; i++ {
			globalMap[i].quit <- 1
		}
	}
	return
}
