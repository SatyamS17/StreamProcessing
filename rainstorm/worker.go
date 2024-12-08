package rainstorm

import (
	"bufio"
	"fmt"
	"log"
	"mp4/dht"
	"mp4/membership"
	"mp4/util"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	rpcPortNumber = "8003"
)

type Server struct {
	dhtServer            *dht.Server
	currentServerAddress string

	command            *Command
	processedRecordIDs map[string]struct{}

	outputBatchLogger     *BatchLogger
	operationsBatchLogger *BatchLogger

	prevStageMachinesDone int

	state map[string]int

	mu sync.Mutex
}

func NewServer(dhtServer *dht.Server) *Server {
	s := Server{dhtServer, dhtServer.GetCurrentServerAddress(), nil, make(map[string]struct{}), nil, nil, 0, make(map[string]int), sync.Mutex{}}
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

func (s *Server) Run(membershipServer *membership.Server, op1Exe string, op2Exe string, hydfsSrcFile string, hydfsDestFile string, numTasks int, pattern string) {
	machineAssignments := MachineAssignments{
		membershipServer.CurrentServer().Address,
		make([]string, numTasks),
		make([]string, numTasks),
		make([]string, numTasks),
	}

	members := membershipServer.Members()
	i := 0
	for taskIdx := range numTasks {
		machineAssignments.SourceMachineAddresses[taskIdx] = members[i].Address
		i = (i + 1) % len(members)
		machineAssignments.Op1MachineAddresses[taskIdx] = members[i].Address
		i = (i + 1) % len(members)
		machineAssignments.Op2MachineAddresses[taskIdx] = members[i].Address
		i = (i + 1) % len(members)
	}

	fmt.Printf("Source machines: %v\n", machineAssignments.SourceMachineAddresses)
	fmt.Printf("Op1 machines: %v\n", machineAssignments.Op1MachineAddresses)
	fmt.Printf("Op2 machines: %v\n", machineAssignments.Op2MachineAddresses)

	s.command = &Command{
		uuid.NewString(),
		op1Exe,
		op2Exe,
		hydfsSrcFile,
		hydfsDestFile,
		numTasks,
		pattern,
		machineAssignments,
	}

	// Register callback
	s.dhtServer.Membership().OnRemoveWorker(s.HandleFailedWorker)

	var wg sync.WaitGroup
	for _, member := range members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := &SetCommandArgs{*s.command}
			err := util.RpcCall(member.Address, rpcPortNumber, "Server.SetCommand", args, &struct{}{})
			if err != nil {
				fmt.Println(err)
			}
		}()
	}

	wg.Wait()

	fmt.Println("Assigned tasks to all workers")

}

type SetCommandArgs struct {
	Command Command
}

func (s *Server) SetCommand(args *SetCommandArgs, reply *struct{}) error {
	s.mu.Lock()

	fmt.Println(args.Command.ID + "_ops.txt")
	s.operationsBatchLogger = NewBatchLogger(s.dhtServer, args.Command.ID+"_ops.txt")
	if args.Command.Assignments.isOp2Machine(s.currentServerAddress) {
		s.outputBatchLogger = NewBatchLogger(s.dhtServer, args.Command.HydfsDestFile)
	}

	s.command = &args.Command

	s.mu.Unlock()

	if s.command.Assignments.isSourceMachine(s.currentServerAddress) {
		go s.Source()
	}
	return nil
}

type SetNewAssignmentsArgs struct {
	NewAssignments MachineAssignments

	NewAssignedTasks []Task
}

func (s *Server) SetNewAssignments(args *SetNewAssignmentsArgs, reply *struct{}) error {
	s.mu.Lock()

	s.command.Assignments = args.NewAssignments

	fmt.Println("New tasks", args.NewAssignedTasks)

	// Check if we were assigned any new tasks. If so, we have to parse through the ops log
	if len(args.NewAssignedTasks) == 0 {
		s.mu.Unlock()
		return nil
	}

	// Parse log file and find all work that still needs to be done from the old failed machine
	localFileName := "oldLogs.txt"
	s.dhtServer.Get(s.command.ID+"_ops.txt", localFileName)

	file, err := os.Open(localFileName)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		s.mu.Unlock()
		return nil
	}
	defer file.Close()

	// Parse file
	scanner := bufio.NewScanner(file)

	pattern := `([\w-]+):([\w-]+):([\w-]+)<([\w.-]+:\d+),\s*(\w+)>`
	state_pattern := `^([^:]+):(.*)\+1$`

	re := regexp.MustCompile(pattern)
	state_re := regexp.MustCompile(state_pattern)

	// Keep track of each status
	outputted := make(map[Record]string)

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)
		state_matches := state_re.FindStringSubmatch(line)

		if len(matches) > 0 {
			// Extract values from the string
			ID := matches[1]     // ID
			status := matches[2] // status
			stage := matches[3]  // stage
			key := matches[4]    // Key
			value := matches[5]  // Value

			// Determine if tuple belongs to this machine | Check correct stage and correct index
			for _, task := range args.NewAssignedTasks {
				if task.Stage.String() == stage && util.Hash(key)%s.command.NumTasks == task.Index {
					switch status {
					case "RECEIVED":
						s.processedRecordIDs[ID] = struct{}{}
						fmt.Printf("Adding %s to processed\n", ID)
					case "OUTPUTTED":
						// Add to outputted
						outputted[Record{ID, key, value}] = stage
						fmt.Printf("Adding %s to be outputted\n", ID)
					case "ACKED":
						// Delete from outputted
						delete(outputted, Record{ID, key, value})
						fmt.Printf("Removing %s to be outputted\n", ID)
					default:
						fmt.Println("Invalid state")
					}
				}
			}

		}

		if len(state_matches) > 0 {
			stage := state_matches[1] // ID
			key := state_matches[2]   // status

			// Determine if tuple belongs to this machine | Check correct stage and correct index
			for _, task := range args.NewAssignedTasks {
				fmt.Print(state_matches)
				if task.Stage.String() == stage && util.Hash(key)%s.command.NumTasks == task.Index {
					fmt.Println("RECOLLECTING")
					s.state[key] = s.state[key] + 1
				}
			}

		}
	}

	fmt.Print("CHECK OLD STATE:\n")
	fmt.Print(s.state)

	for record, stage := range outputted {
		switch stage {
		case "SOURCE":
			fmt.Println("oops")
		case "OP1":
			fmt.Println("Resending to OP2")
			for {
				nextStageServerAddress := s.command.Assignments.Op2MachineAddresses[util.Hash(record.Key)%s.command.NumTasks]
				args := ProcessRecordArgs{Op1Stage, record}
				var ok bool
				err := util.RpcCall(nextStageServerAddress, rpcPortNumber, "Server.ProcessRecord", args, &ok)
				if err != nil || !ok {
					fmt.Println(err)
					time.Sleep(500 * time.Millisecond)
				} else {
					break
				}
			}

			// Log ACK to DFS
			s.operationsBatchLogger.Append(record.String(ACKED, Op1Stage))
		case "OP2":
			fmt.Println("Resending to leader")
			// Send record to leader
			args := OutputRecordArgs{record}
			err := util.RpcCall(s.command.Assignments.LeaderMachineAddress, rpcPortNumber, "Server.OutputRecord", args, &struct{}{})
			if err != nil {
				fmt.Println(err)
			}

			// Log to output file
			s.outputBatchLogger.Append(record.String(PROCESSED, 0))

			// Log ACK to DFS
			s.operationsBatchLogger.Append(record.String(ACKED, Op2Stage))
		}
	}

	s.mu.Unlock()
	return nil
}

func (s *Server) ProcessRecord(args *ProcessRecordArgs, reply *bool) error {
	if s.command == nil {
		*reply = false
		return nil
	}

	var currentStage Stage
	if args.FromStage == SourceStage && s.command.Assignments.isOp1Machine(s.currentServerAddress) {
		currentStage = Op1Stage
	} else if args.FromStage == Op1Stage && s.command.Assignments.isOp2Machine(s.currentServerAddress) {
		currentStage = Op2Stage
	} else {
		*reply = false
		return nil
	}

	// Check if record is duplicate
	if _, ok := s.processedRecordIDs[args.Record.ID]; ok {
		log.Println("Duplicate record - not processing")
		*reply = true
		return nil
	}

	s.processedRecordIDs[args.Record.ID] = struct{}{}

	// Log RECEIVED to DFS
	s.operationsBatchLogger.Append(args.Record.String(RECEIVED, currentStage))

	// Perform op
	opCmd := s.command.Op1Exe
	if currentStage == Op2Stage {
		opCmd = s.command.Op2Exe
	}

	out, err := exec.Command("./"+opCmd, args.Record.Key, args.Record.Value, currentStage.String(), s.command.ID+"_ops.txt.tmp", s.command.Pattern).Output()

	if err != nil {
		fmt.Println(err)
		*reply = true
		return nil
	}

	outSplit := strings.Split(strings.TrimSpace(string(out)), "\n")

	// Could return nothing
	if len(outSplit) < 1 {
		*reply = true
		return nil
	}

	var records []Record

	if len(outSplit) == 1 {
		records = make([]Record, 1)

		s.state[outSplit[0]] = s.state[outSplit[0]] + 1
		records[0] = Record{uuid.NewString(), outSplit[0], strconv.Itoa(s.state[outSplit[0]])}

		stateChange := fmt.Sprintf("%s:%s+1", args.Record.Key, args.Record.Key)
		s.operationsBatchLogger.Append(stateChange)
	} else {
		records = make([]Record, len(outSplit)/2)
		for i := 0; i < len(outSplit); i += 2 {
			records[i/2] = Record{uuid.NewString(), outSplit[i], outSplit[i+1]}
		}
	}

	go func() {
		if currentStage == Op1Stage {
			for _, record := range records {
				// Log OUTPUTTED to DFS
				s.operationsBatchLogger.Append(record.String(OUTPUTTED, currentStage))

				// Send record to op2
				for {
					nextStageServerAddress := s.command.Assignments.Op2MachineAddresses[util.Hash(record.Key)%s.command.NumTasks]
					args := ProcessRecordArgs{currentStage, record}
					var ok bool
					fmt.Printf("Sending key %s to op2\n", record.Key)
					err := util.RpcCall(nextStageServerAddress, rpcPortNumber, "Server.ProcessRecord", args, &ok)
					if err != nil || !ok {
						fmt.Println(err)
						time.Sleep(500 * time.Millisecond)
					} else {
						fmt.Printf("Sent key %s\n", record.Key)
						break
					}
				}

				// Log ACK to DFS
				s.operationsBatchLogger.Append(record.String(ACKED, currentStage))
			}
		} else if currentStage == Op2Stage {
			for _, record := range records {
				// Log OUTPUTTED to DFS
				s.operationsBatchLogger.Append(record.String(OUTPUTTED, currentStage))

				// Send record to leader
				args := OutputRecordArgs{record}
				fmt.Printf("Sending key %s to leader\n", record.Key)
				err := util.RpcCall(s.command.Assignments.LeaderMachineAddress, rpcPortNumber, "Server.OutputRecord", args, &struct{}{})
				if err != nil {
					fmt.Println(err)
				}

				// Log to output file
				s.outputBatchLogger.Append(record.String(PROCESSED, 0))

				// Log ACK to DFS
				s.operationsBatchLogger.Append(record.String(ACKED, currentStage))
			}
		}
	}()

	*reply = true
	return nil
}

func (s *Server) Source() {
	// Get hydfs file
	localFileName := "source.txt"
	s.dhtServer.Get(s.command.HydfsSrcFile, localFileName)

	// Get server index
	indexes := s.GetTaskIndexesFromAddress(s.command.Assignments.SourceMachineAddresses, s.currentServerAddress)

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

		// go func() { // TODO: WILL THIS BREAK SHIT?
		if slices.Contains(indexes, hashLine%s.command.NumTasks) {
			// Format record
			key := fmt.Sprintf("%s:%s", s.command.HydfsSrcFile, strconv.Itoa(lineNumber))
			record := Record{uuid.NewString(), key, line}

			// Use hash to send to right machine
			for {
				nextStageServerAddress := s.command.Assignments.Op1MachineAddresses[util.Hash(record.Key)%s.command.NumTasks]
				args := ProcessRecordArgs{SourceStage, record}
				var ok bool
				fmt.Printf("Sending key %s to op1\n", record.Key)
				err := util.RpcCall(nextStageServerAddress, rpcPortNumber, "Server.ProcessRecord", args, &ok)

				if err != nil || !ok {
					fmt.Println(err, ok)
					time.Sleep(500 * time.Millisecond)
				} else {
					break
				}
			}
		}
		// }()
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}

	// for _, machine := range s.command.Assignments.Op1MachineAddresses {
	// 	util.RpcCall(machine, rpcPortNumber, "Server.MarkPrevStageMachineDone", &struct{}{}, &struct{}{})
	// }
}

func (s *Server) OutputRecord(args *OutputRecordArgs, reply *struct{}) error {
	fmt.Print(args.Record.String(PROCESSED, 0))
	return nil
}

func (s *Server) MarkPrevStageMachineDone(args *struct{}, reply *struct{}) error {
	s.prevStageMachinesDone++

	if s.prevStageMachinesDone == s.command.NumTasks {
		// Forward to next stage
		if s.command.Assignments.isOp1Machine(s.dhtServer.GetCurrentServerAddress()) {
			for _, machine := range s.command.Assignments.Op2MachineAddresses {
				util.RpcCall(machine, rpcPortNumber, "Server.MarkPrevStageMachineDone", &struct{}{}, &struct{}{})
			}
		} else if s.command.Assignments.isOp2Machine(s.dhtServer.GetCurrentServerAddress()) {
			util.RpcCall(s.command.Assignments.LeaderMachineAddress, rpcPortNumber, "Server.MarkPrevStageMachineDone", &struct{}{}, &struct{}{})
		}

		if s.outputBatchLogger != nil {
			s.outputBatchLogger.Stop()
			s.outputBatchLogger = nil
		}

		if s.operationsBatchLogger != nil {
			s.operationsBatchLogger.Stop()
			s.operationsBatchLogger = nil
		}

		if s.command.Assignments.LeaderMachineAddress == s.dhtServer.GetCurrentServerAddress() {
			// Remove callback
			s.dhtServer.Membership().OnRemoveMember(nil)
			fmt.Println("Done")
		}

		// Reset internal state
		s.command = nil
		s.prevStageMachinesDone = 0
		clear(s.processedRecordIDs)
	}

	return nil
}

func (s *Server) HandleFailedWorker(failedAddress string) {
	if s.command == nil {
		return
	}

	newAssignedTasks := make(map[string][]Task)

	// Check if failed machine was running any source tasks
	for i, address := range s.command.Assignments.SourceMachineAddresses {
		if address == failedAddress {
			worker := s.findNewWorker()
			s.command.Assignments.SourceMachineAddresses[i] = worker
			newAssignedTasks[worker] = append(newAssignedTasks[worker], Task{SourceStage, i})
		}
	}

	// Check if failed machine was running any op1 tasks
	for i, address := range s.command.Assignments.Op1MachineAddresses {
		if address == failedAddress {
			worker := s.findNewWorker()
			s.command.Assignments.Op1MachineAddresses[i] = worker
			newAssignedTasks[worker] = append(newAssignedTasks[worker], Task{Op1Stage, i})
		}
	}

	// Check if failed machine was running any op2 tasks
	for i, address := range s.command.Assignments.Op2MachineAddresses {
		if address == failedAddress {
			worker := s.findNewWorker()
			s.command.Assignments.Op2MachineAddresses[i] = worker
			newAssignedTasks[worker] = append(newAssignedTasks[worker], Task{Op2Stage, i})
		}
	}

	// Send new machine assignments to all workers
	var wg sync.WaitGroup
	for _, member := range s.dhtServer.Membership().Members() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := &SetNewAssignmentsArgs{
				s.command.Assignments,
				newAssignedTasks[member.Address],
			}
			err := util.RpcCall(member.Address, rpcPortNumber, "Server.SetNewAssignments", args, &struct{}{})
			if err != nil {
				fmt.Println(err)
			}
		}()
	}

	wg.Wait()
}

func (s *Server) findNewWorker() string {
	taskCount := make(map[string]int)

	for _, address := range s.command.Assignments.SourceMachineAddresses {
		taskCount[address]++
	}

	type Machine struct {
		taskCount int
		address   string
	}

	addressToTaskCount := make([]Machine, 0)

	for address, taskCount := range taskCount {
		addressToTaskCount = append(addressToTaskCount, Machine{taskCount, address})
	}

	sort.Slice(addressToTaskCount, func(i, j int) bool {
		return addressToTaskCount[i].taskCount < addressToTaskCount[j].taskCount
	})

	return addressToTaskCount[0].address
}
