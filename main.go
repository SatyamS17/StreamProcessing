package main

import (
	"fmt"
	"io"
	"log"
	"mp4/dht"
	"mp4/membership"
	"mp4/rainstorm"
	"mp4/util"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/peterh/liner"
)

func main() {
	fmt.Println("whelfaj") // DO NOT DELETE

	// Get current server address.
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Unable to identify current hostname.", err)
	}

	membershipServer := membership.NewServer(hostname)
	dhtServer, err := dht.NewServer(membershipServer)
	if err != nil {
		log.Fatal("Unable to create DHT server.", err)
	}

	rainstormServer := rainstorm.NewServer(dhtServer)

	file, err := openLogFile(util.AddressToID(membershipServer.CurrentServer().Address))
	if err != nil {
		log.Fatalf("Could not open log file: %v", err)
	}
	defer file.Close()

	go membershipServer.RunUDPServer()
	go dhtServer.RunHTTPServer()
	go dhtServer.RunTCPServer()
	go rainstormServer.RunRPCServer()

	time.Sleep(500 * time.Millisecond)

	line := liner.NewLiner()
	defer line.Close()

	go membershipServer.RunPeriodicPings()

	line.SetMultiLineMode(true)
	line.SetCtrlCAborts(true)

	// Load command history
	if historyFile, err := os.Open("history.txt"); err == nil {
		defer historyFile.Close()
		line.ReadHistory(historyFile)
	}

	for {
		in, err := line.Prompt("> ")

		if err == liner.ErrPromptAborted || err == io.EOF {
			fmt.Println("\nExiting...")
			break
		}

		line.AppendHistory(in)
		text := strings.Fields(in)

		if len(text) == 0 {
			continue
		}

		switch text[0] {
		case "list_mem":
			l := []string{}
			for _, m := range membershipServer.Members() {
				l = append(l, formatMember(&m))
			}

			sort.Slice(l, func(i, j int) bool {
				return l[i] < l[j]
			})

			for _, m := range l {
				fmt.Println(m)
			}
		case "list_self":
			fmt.Println(formatMember(membershipServer.CurrentServer()))
		case "leave":
			membershipServer.Leave()
		case "disable_sus":
			membershipServer.DisableSus()
		case "enable_sus":
			membershipServer.EnableSus()
		case "status_sus":
			if membershipServer.UseSus() {
				fmt.Println("Suspicion enabled")
			} else {
				fmt.Println("Suspicion disabled")
			}
		case "create":
			if len(text) < 3 {
				fmt.Println("Missing parameters")
				continue
			}
			dhtServer.Create(text[1], text[2])
		case "get":
			if len(text) < 3 {
				fmt.Println("Missing parameters")
				continue
			}
			dhtServer.Get(text[1], text[2])
		case "append":
			if len(text) < 3 {
				fmt.Println("Missing parameters")
				continue
			}
			dhtServer.Append(text[1], text[2])
		case "ls":
			if len(text) < 2 {
				util.Ls()
			} else {
				dhtServer.Ls(text[1])
			}
		case "cat":
			if len(text) < 2 {
				fmt.Println("Missing parameters")
				continue
			}
			util.Cat(text[1])
		case "store":
			dhtServer.Store()
		case "getfromreplica":
			if len(text) < 3 {
				fmt.Println("Missing parameters")
				continue
			}
			dhtServer.GetFromReplica(text[1], text[2], text[3])
		case "list_mem_ids":
			l := []string{}
			for _, m := range membershipServer.Members() {
				l = append(l, formatMember(&m))
			}

			sort.Slice(l, func(i, j int) bool {
				return l[i] < l[j]
			})

			for _, m := range l {
				fmt.Printf("%s (%d)\n", m, util.AddressToID(m))
			}
		case "multiappend": // multiappend dfsFile #1,#2,#3... localfile#1,localfile#2,localfile#3
			if len(text) < 4 {
				fmt.Println("Missing parameters")
				continue
			}
			vms := strings.Split(text[2], ",")

			vmIds := make([]int, len(vms))

			for i, vm := range vms {
				// Trim any extra whitespace or commas, then convert to int
				num, err := strconv.Atoi(strings.TrimSuffix(vm, ","))
				if err != nil {
					fmt.Println("Error converting string to int:", err)
					return
				}
				vmIds[i] = num
			}

			localFiles := strings.Split(text[3], ",")
			fmt.Println(localFiles)

			if len(vmIds) > len(localFiles) {
				fmt.Printf("Missing filenames, expecting %d but got %d\n", len(vmIds), len(localFiles))
			} else if len(localFiles) > len(vmIds) {
				fmt.Printf("Missing Vms, expecting %d but got %d\n", len(localFiles), len(vmIds))
			} else {
				dhtServer.Multiappend(text[1], vmIds, localFiles)
			}
		case "merge":
			if len(text) < 2 {
				fmt.Println("Missing parameters")
				continue
			}
			dhtServer.Merge(text[1])
		case "RainStorm":
			if len(text) < 6 {
				fmt.Println("Missing parameters")
				continue
			}

			numTasks, err := strconv.Atoi(text[5])
			if err != nil {
				fmt.Println("Invalid numTasks")
				continue
			}

			rainstorm.Run(membershipServer, text[1], text[2], text[3], text[4], numTasks)
		default:
			fmt.Printf("Invalid command\n")
		}
	}
}

func formatMember(m *membership.Member) string {
	return fmt.Sprintf("%s (joined at %s)", m.Address, m.TimeJoined.Format("15:04:05"))
}
