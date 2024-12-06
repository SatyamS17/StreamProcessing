package rainstorm

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	rpcPortNumber = "8003"
)

type WorkerServer struct {
	command *Command
}

func NewWorkerServer() *WorkerServer {
	s := WorkerServer{nil}
	return &s
}

func (s *WorkerServer) RunRPCServer() {
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

func (s *WorkerServer) SetCommand(args *SetCommandArgs, reply *struct{}) error {
	s.command = &args.Command
	fmt.Println("Set command to")
	fmt.Println(s.command)
	return nil
}
