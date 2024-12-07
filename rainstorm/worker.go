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

type ProcessRecordArgs struct {
	FromStage Stage
	Record    Record
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
	s.outputBatchLogger.Append(args.Record.String(RECEIVED))

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
				s.outputBatchLogger.Append(record.String(OUTPUTTED))

				// Send record to op2
				nextStageServerAddress := s.GetTaskAddressFromIndex(s.command.Assignments.Op2MachineAddresses, util.Hash(record.Key))
				args := ProcessRecordArgs{currentStage, record}
				err := util.RpcCall(nextStageServerAddress, rpcPortNumber, "Server.ProcessRecord", args)
				if err != nil {
					fmt.Println(err)
				}

				// Log ACK to DFS
				s.outputBatchLogger.Append(record.String(ACKED))
			}
		} else if currentStage == Op2Stage {
			for _, record := range records {
				// Log OUTPUTTED to DFS
				s.outputBatchLogger.Append(record.String(OUTPUTTED))

				// Send record to leader
				args := OutputRecordArgs{record}
				err := util.RpcCall(s.command.Assignments.LeaderMachineAddress, rpcPortNumber, "Server.OutputRecord", args)
				if err != nil {
					fmt.Println(err)
				}

				// Log ACK to DFS
				s.outputBatchLogger.Append(record.String(ACKED))
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
	index := s.GetTaskIndexFromAddress(s.command.Assignments.SourceMachineAddresses, s.address)

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

		if hashLine%len(s.command.Assignments.SourceMachineAddresses) == index {
			// Format record
			key := fmt.Sprintf("%s:%s", s.command.HydfsSrcFile, strconv.Itoa(lineNumber))
			record := Record{uuid.NewString(), key, line}

			// Use hash to send to right machine
			nextStageServerAddress := s.GetTaskAddressFromIndex(s.command.Assignments.Op1MachineAddresses, hashLine)
			args := ProcessRecordArgs{SourceStage, record}
			err := util.RpcCall(nextStageServerAddress, rpcPortNumber, "Server.ProcessRecord", args)

			if err != nil {
				fmt.Println("uh oh")
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}
}

type OutputRecordArgs struct {
	Record Record
}

func (s *Server) OutputRecord(args *OutputRecordArgs, reply *struct{}) error {
	fmt.Println(args.Record.String(PROCESSED))
	return nil
}
