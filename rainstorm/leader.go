package rainstorm

import (
	"fmt"
	"mp4/membership"
	"mp4/util"
	"sync"
)

func Run(membershipServer *membership.Server, op1Exe string, op2Exe string, hydfsSrcFile string, hydfsDestFile string, numTasks int) {
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

	command := Command{
		op1Exe,
		op2Exe,
		hydfsSrcFile,
		hydfsDestFile,
		machineAssignments,
	}

	var wg sync.WaitGroup
	for _, member := range members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := &SetCommandArgs{command}
			err := util.RpcCall(member.Address, rpcPortNumber, "Server.SetCommand", args)
			if err != nil {
				fmt.Println(err)
			}
		}()
	}

	wg.Wait()
}
