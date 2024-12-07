package rainstorm

import (
	"bufio"
	"fmt"
	"log"
	"mp4/dht"
	"mp4/util"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	rpcPortNumber = "8003"
)

type Server struct {
	dhtServer *dht.Server
	address   string

	command            *Command
	processedRecordIDs map[string]struct{}

	outputBatchLogger *BatchLogger
}

func NewServer(dhtServer *dht.Server) *Server {
	s := Server{dhtServer, dhtServer.GetCurrentServerAddress(), nil, make(map[string]struct{}), nil}

	// register failed worker callback
	s.dhtServer.Membership().OnRemoveWorker(s.HandleFailedWorker)
	return &s
}

func (s *Server) RunRPCServer() {
	rpc.Register(s)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+rpcPortNumber)
	if err != nil {
		fmt.Println("Unable to start RPC server.", err)
		os.Exit(1)
	}
	http.Serve(l, nil)
}

type SetCommandArgs struct {
	Command Command
}

func (s *Server) SetCommand(args *SetCommandArgs, reply *struct{}) error {
	s.command = &args.Command

	// Check if current server in source machine
	if s.isAddressInMap(s.command.Assignments.SourceMachineAddresses, s.address) {
		go s.Source()
	} else if s.isAddressInMap(s.command.Assignments.Op2MachineAddresses, s.address) {
		s.outputBatchLogger = NewBatchLogger(s.dhtServer, s.command.HydfsDestFile)
	}

	return nil
}

func (s *Server) ProcessRecord(args *ProcessRecordArgs, reply *struct{}) error {
	var currentStage Stage
	if args.FromStage == SourceStage && s.isAddressInMap(s.command.Assignments.Op1MachineAddresses, s.address) {
		currentStage = Op1Stage
	} else if args.FromStage == Op1Stage && s.isAddressInMap(s.command.Assignments.Op2MachineAddresses, s.address) {
		currentStage = Op2Stage
	} else {
		fmt.Println("asdf")
		return nil
	}

	// Check if record is duplicate
	if _, ok := s.processedRecordIDs[args.Record.ID]; ok {
		log.Println("Duplicate record - not processing")
		return nil
	}

	s.processedRecordIDs[args.Record.ID] = struct{}{}

	// Log RECEIVED to DFS
	s.outputBatchLogger.Append(args.Record.String(RECEIVED, currentStage))

	// Perform op
	opCmd := s.command.Op1Exe
	if currentStage == Op2Stage {
		opCmd = s.command.Op2Exe
	}
	out, err := exec.Command("./"+opCmd, args.Record.Key, args.Record.Value).Output()
	if err != nil {
		fmt.Println(err)
		return nil
	}

	outSplit := strings.Split(strings.TrimSpace(string(out)), "\n")
	records := make([]Record, len(outSplit)/2)
	for i := 0; i < len(outSplit); i += 2 {
		records[i/2] = Record{uuid.NewString(), outSplit[i], outSplit[i+1]}
	}

	go func() {
		if currentStage == Op1Stage {
			for _, record := range records {
				// Log OUTPUTTED to DFS
				s.outputBatchLogger.Append(record.String(OUTPUTTED, currentStage))

				// Send record to op2
				nextStageServerAddress := s.GetTaskAddressFromIndex(s.command.Assignments.Op2MachineAddresses, util.Hash(record.Key))
				args := ProcessRecordArgs{currentStage, record}
				err := util.RpcCall(nextStageServerAddress, rpcPortNumber, "Server.ProcessRecord", args)
				if err != nil {
					fmt.Println(err)
				}

				// Log ACK to DFS
				s.outputBatchLogger.Append(record.String(ACKED, currentStage))
			}
		} else if currentStage == Op2Stage {
			for _, record := range records {
				// Log OUTPUTTED to DFS
				s.outputBatchLogger.Append(record.String(OUTPUTTED, currentStage))

				// Send record to leader
				args := OutputRecordArgs{record}
				err := util.RpcCall(s.command.Assignments.LeaderMachineAddress, rpcPortNumber, "Server.OutputRecord", args)
				if err != nil {
					fmt.Println(err)
				}

				// Log ACK to DFS
				s.outputBatchLogger.Append(record.String(ACKED, currentStage))
			}
		}
	}()

	return nil
}

func (s *Server) Source() {
	// Get hydfs file
	localFileName := "source.txt"
	s.dhtServer.Get(s.command.HydfsSrcFile, localFileName)

	// Get server index
	indexes := s.GetTaskIndexesFromAddress(s.command.Assignments.SourceMachineAddresses, s.address)

	// Read file line by line and send tuples
	file, err := os.Open(localFileName)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0

	for scanner.Scan() {
		line := scanner.Text()
		lineNumber++
		hashLine := util.HashLine(line)

		for _, index := range indexes {
			go func() { // TODO: WILL THIS BREAK SHIT?
				if hashLine%len(s.command.Assignments.SourceMachineAddresses) == index {
					// Format record
					key := fmt.Sprintf("%s:%s", s.command.HydfsSrcFile, strconv.Itoa(lineNumber))
					record := Record{uuid.NewString(), key, line}

					// Use hash to send to right machine
					nextStageServerAddress := s.GetTaskAddressFromIndex(s.command.Assignments.Op1MachineAddresses, util.Hash(record.Key))
					args := ProcessRecordArgs{SourceStage, record}
					err := util.RpcCall(nextStageServerAddress, rpcPortNumber, "Server.ProcessRecord", args)

					if err != nil {
						fmt.Println("uh oh")
					}
				}
			}()
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}
}

func (s *Server) OutputRecord(args *OutputRecordArgs, reply *struct{}) error {
	fmt.Println(args.Record.String(PROCESSED, 0))
	return nil
}

func (s *Server) HandleFailedWorker(failedAddress string) {
	// Check to see if failed machine is actually a worker
	if _, ok := s.command.Assignments.AssignedMachines[failedAddress]; !ok {
		log.Printf("Failed node:%s, not assigned a task", failedAddress)
		return
	}

	fmt.Println(failedAddress) // For testing purposes ~delete later~ TODO: Is address string formated correctly? - SATYAM SINGH

	// Find which stage the address was working in
	stageMap, stage := s.getStageMap(failedAddress)
	if stageMap == nil {
		log.Printf("Failed node: %s not found in any stage maps", failedAddress)
		return
	}

	// Find a new worker for the task
	newWorkerAddress := s.FindNewWorker(stageMap, failedAddress)
	if newWorkerAddress == "" {
		log.Printf("No suitable worker found for failed node: %s", failedAddress)
		return
	}

	// Assign the new worker to the task by replacing all of the failed address with the new worker address
	for key, value := range stageMap {
		if value == failedAddress {
			stageMap[key] = newWorkerAddress
		}
	}

	go s.AssignWorker(newWorkerAddress, stageMap, stage.String())
}

func (s *Server) FindNewWorker(workerAddresses map[int]string, failedAddress string) string {
	// Check if there is a machine with no work
	for _, member := range s.dhtServer.Membership().Members() {
		if _, ok := s.command.Assignments.AssignedMachines[member.Address]; !ok {
			return member.Address
		}
	}

	// Pick the machine in the same stage with the "least work"
	addressCount := make(map[string]int)
	for _, address := range workerAddresses {
		addressCount[address]++
	}

	var minCount int
	var minAddress string
	for address, count := range addressCount {
		if minCount == 0 || count < minCount {
			minCount = count
			minAddress = address
		}
	}

	return minAddress
}

func (s *Server) AssignWorker(address string, stageMap map[int]string, workerStage string) {
	// Parse log file and find all work that still needs to be done from the old failed machine
	localFileName := "oldLogs.txt"
	s.dhtServer.Get(s.command.HydfsSrcFile, localFileName)

	indexes := s.GetTaskIndexesFromAddress(stageMap, address)

	file, err := os.Open(localFileName)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	// Parse file
	scanner := bufio.NewScanner(file)

	pattern := `(\S+):(\S+):(\S+)<(\S+),(\S+)>`
	re := regexp.MustCompile(pattern)

	// Keep track of each status
	received := make(map[string]Record)
	outputted := make(map[string]Record)
	acked := make(map[string]Record)

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)

		if len(matches) > 0 {
			// Extract values from the string
			ID := matches[1]     // ID
			status := matches[2] // status
			stage := matches[3]  // stage
			key := matches[4]    // Key
			value := matches[5]  // Value

			// Determine if tuple belongs to this machine | Check correct stage and correct index
			if workerStage == stage && slices.Contains(indexes, util.Hash(key)%len(stageMap)) {
				switch status {
				case "RECEIVED":
					received[ID] = Record{ID, key, value}
				case "OUTPUTTED":
					// Delete from receieved
					delete(received, ID)

					// Add to outputted
					outputted[ID] = Record{ID, key, value}
				case "ACKED":
					// Delete from outputted
					delete(outputted, ID)

					// Add to acked
					acked[ID] = Record{ID, key, value}

				default:
					fmt.Println("Invalid state")
				}
			}

		}
	}

	// Process all received but not outputted

	// Process all outputted but not acked

}
