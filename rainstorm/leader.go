package rainstorm

import (
	"fmt"
	"mp4/membership"
	"net/rpc"
)

func Run(membershipServer *membership.Server, op1Exe string, op2Exe string, hydfsSrcFile string, hydfsDestFile string, numTasks int) {
	machineAssignments := MachineAssignments{
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

	command := Command{
		op1Exe,
		op2Exe,
		hydfsSrcFile,
		hydfsDestFile,
		machineAssignments,
	}

	for _, member := range members {
		client, err := rpc.DialHTTP("tcp", member.Address+":"+rpcPortNumber)
		if err != nil {
			fmt.Println("lakewfj")
			continue
		}

		args := &SetCommandArgs{command}
		var reply struct{}
		err = client.Call("WorkerServer.SetCommand", args, &reply)
		if err != nil {
			fmt.Println("kwalej")
			continue
		}
	}
}
