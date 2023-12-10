package RicartAgrawala

import (
	"fmt"
	"sync"
	"time"
)

const NUM_NODES = 10

func RicartAgrawalaManual() {
	var wg sync.WaitGroup

	globalNodeMap := map[int]*Node{}
	// Initiate New Nodes
	for i := 1; i <= NUM_NODES; i++ {
		newNode := NewNode(i)
		globalNodeMap[i] = newNode
	}

	//Give everyone the global pointer map
	for i := 1; i <= NUM_NODES; i++ {
		globalNodeMap[i].setPtrMap(globalNodeMap)
	}

	// Start listen() go routine for each node
	for i := 1; i <= NUM_NODES; i++ {
		globalNodeMap[i].globalWG = &wg
		go globalNodeMap[i].listen()
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
	for i := 1; i <= numNodes; i++ {
		wg.Add(1)
		go globalNodeMap[i].requestCS()
	}
	wg.Wait()
	end := time.Now()
	// time.Sleep(time.Duration(3) * time.Second)
	fmt.Printf("Number of nodes: %d \n", NUM_NODES)
	fmt.Printf("Time Taken: %.2f seconds \n", end.Sub(start).Seconds())

	fmt.Println("All Nodes have entered entered and exited the Critical Section\nEnding programme now")
	for i := 1; i <= NUM_NODES; i++ {
		globalNodeMap[i].quit <- 1
	}

}

func RicartAgrawalaRun() (result []float64) {

	for n := 1; n <= NUM_NODES; n++ {
		fmt.Println("\n\n#######################################################")
		fmt.Println("#######################################################")
		fmt.Println("#######################################################")
		fmt.Printf("Number of Nodes concurrently requesting CS: %d\n", n)
		fmt.Println("#######################################################")
		fmt.Println("#######################################################")
		fmt.Println("#######################################################")

		var wg sync.WaitGroup
		globalNodeMap := map[int]*Node{}
		// Initiate New Nodes
		for i := 1; i <= NUM_NODES; i++ {
			newNode := NewNode(i)
			globalNodeMap[i] = newNode
		}

		//Give everyone the global pointer map
		for i := 1; i <= NUM_NODES; i++ {
			globalNodeMap[i].setPtrMap(globalNodeMap)
		}

		// Start listen() go routine for each node
		for i := 1; i <= NUM_NODES; i++ {
			globalNodeMap[i].globalWG = &wg
			go globalNodeMap[i].listen()
		}
		start := time.Now()
		for i := 1; i <= n; i++ {
			wg.Add(1)
			go globalNodeMap[i].requestCS()
		}
		wg.Wait()
		end := time.Now()
		// time.Sleep(time.Duration(3) * time.Second)
		fmt.Printf("Number of nodes: %d \n", NUM_NODES)
		timeTaken := end.Sub(start).Seconds()
		fmt.Printf("Time Taken: %.2f seconds \n", end.Sub(start).Seconds())
		result = append(result, timeTaken)
		fmt.Println("All Nodes have entered entered and exited the Critical Section\nEnding programme now")
		for i := 1; i <= NUM_NODES; i++ {
			globalNodeMap[i].quit <- 1
		}
	}
	return
}
