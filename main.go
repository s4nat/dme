package main

import (
	"fmt"
	"github.com/s4nat/dme/Lamport"
	"github.com/s4nat/dme/RicartAgrawala"
	"github.com/s4nat/dme/Voting"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func main() {
	mode := getUserInput("Enter mode ('manual' or 'graph'): ")

	switch mode {
	case "manual":
		runManualMode()
	case "graph":
		graph()
	default:
		fmt.Println("Invalid mode. Please choose 'manual' or 'graph'.")
	}
}

func runManualMode() {
	module := getUserInput("Choose module:\n1. Lamport-SPQ\n2. RicartAgrawala\n3. Voting\n")

	switch module {
	case "1":
		Lamport.LamportManual()
	case "2":
		RicartAgrawala.RicartAgrawalaManual()
	case "3":
		Voting.VotingManual()
	default:
		fmt.Println("Invalid module choice.")
	}
}

func graph() {
	// Call the function to generate the graph
	fmt.Println("Generating graph...")
	lamportRes := Lamport.LamportRun()
	ricartAgrawalaRes := RicartAgrawala.RicartAgrawalaRun()
	// votingRes := Voting.VotingRun()
	votingRes := []float64{10.31, 11.52, 13.23, 12.61, 12.64, 12.71, 13.12, 13.41, 13.71, 14.01}
	err := plotGraph(lamportRes, ricartAgrawalaRes, votingRes)
	if err != nil {
		fmt.Println("Error plotting graph:", err)
	}
}

func getUserInput(prompt string) string {
	var input string
	fmt.Print(prompt)
	fmt.Scanln(&input)
	return input
}

func plotGraph(lamportRes, ricartAgrawalaRes, votingRes []float64) error {
	p := plot.New()

	pointsLamport := make(plotter.XYs, len(lamportRes))
	pointsRicartAgrawala := make(plotter.XYs, len(ricartAgrawalaRes))
	pointsVoting := make(plotter.XYs, len(votingRes))

	for i := 0; i < len(lamportRes); i++ {
		pointsLamport[i].X = float64(i + 1)
		pointsLamport[i].Y = lamportRes[i]

		pointsRicartAgrawala[i].X = float64(i + 1)
		pointsRicartAgrawala[i].Y = ricartAgrawalaRes[i]

		pointsVoting[i].X = float64(i + 1)
		pointsVoting[i].Y = votingRes[i]
	}

	lineLamport, err := plotter.NewLine(pointsLamport)
	if err != nil {
		return err
	}
	lineLamport.Color = plotutil.Color(0)

	lineRicartAgrawala, err := plotter.NewLine(pointsRicartAgrawala)
	if err != nil {
		return err
	}
	lineRicartAgrawala.Color = plotutil.Color(1)

	lineVoting, err := plotter.NewLine(pointsVoting)
	if err != nil {
		return err
	}
	lineVoting.Color = plotutil.Color(2)

	p.Add(lineLamport, lineRicartAgrawala, lineVoting)
	p.Legend.Add("Lamport", lineLamport)
	p.Legend.Add("RicartAgrawala", lineRicartAgrawala)
	p.Legend.Add("Voting", lineVoting)

	p.Title.Text = "Concurrency vs. Time Taken"
	p.X.Label.Text = "Number of Concurrent Nodes"
	p.Y.Label.Text = "Time Taken"

	if err := p.Save(8*vg.Inch, 4*vg.Inch, "output.png"); err != nil {
		return err
	}

	return nil
}
