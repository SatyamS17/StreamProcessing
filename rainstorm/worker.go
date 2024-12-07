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

	command            *Command
	processedRecordIDs map[string]struct{}

	outputBatchLogger *BatchLogger
}

func NewServer(dhtServer *dht.Server) *Server {
	s := Server{dhtServer, nil, make(map[string]struct{}), nil}
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

	if slices.Contains(s.command.Assignments.SourceMachineAddresses, s.dhtServer.Membership().CurrentServer().Address) {
		go s.Source()
	} else if slices.Contains(s.command.Assignments.Op2MachineAddresses, s.dhtServer.Membership().CurrentServer().Address) {
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
	if args.FromStage == SourceStage && slices.Contains(s.command.Assignments.Op1MachineAddresses, s.dhtServer.Membership().CurrentServer().Address) {
		currentStage = Op1Stage
	} else if args.FromStage == Op1Stage && slices.Contains(s.command.Assignments.Op2MachineAddresses, s.dhtServer.Membership().CurrentServer().Address) {
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
	fmt.Println(outSplit)
	records := make([]Record, len(outSplit)/2)
	for i := 0; i < len(outSplit); i += 2 {
		records[i/2] = Record{uuid.NewString(), outSplit[i], outSplit[i+1]}
	}

	go func() {
		if currentStage == Op1Stage {
			for _, record := range records {
				nextStageServerAddress := s.command.Assignments.Op2MachineAddresses[util.Hash(record.Key)%len(s.command.Assignments.Op2MachineAddresses)]
				args := ProcessRecordArgs{currentStage, record}
				err := util.RpcCall(nextStageServerAddress, rpcPortNumber, "Server.ProcessRecord", args)
				if err != nil {
					fmt.Println(err)
				}
			}
		} else if currentStage == Op2Stage {
			for _, record := range records {
				// Log to DFS
				s.outputBatchLogger.Append(record.String())

				// Send record to leader
				args := OutputRecordArgs{record}
				err := util.RpcCall(s.command.Assignments.LeaderMachineAddress, rpcPortNumber, "Server.OutputRecord", args)
				if err != nil {
					fmt.Println(err)
				}
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
	index := 0
	for i, address := range s.command.Assignments.SourceMachineAddresses {
		if address == s.dhtServer.Membership().CurrentServer().Address {
			index = i
		}
	}

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
			record := Record{uuid.NewString(), strconv.Itoa(lineNumber), line}

			// Use hash to send to right machine
			nextStageServerAddress := s.command.Assignments.Op1MachineAddresses[hashLine%len(s.command.Assignments.Op1MachineAddresses)]
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
	fmt.Println(args.Record.String())
	return nil
}
