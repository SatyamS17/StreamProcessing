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
		make(map[int]string),
		make(map[int]string),
		make(map[int]string),
		make(map[string]struct{}),
	}

	members := membershipServer.Members()
	i := 0
	for taskIdx := range numTasks {
		assignMachine(taskIdx, machineAssignments.SourceMachineAddresses, members, machineAssignments.AssignedMachines, &i)
		assignMachine(taskIdx, machineAssignments.Op1MachineAddresses, members, machineAssignments.AssignedMachines, &i)
		assignMachine(taskIdx, machineAssignments.Op2MachineAddresses, members, machineAssignments.AssignedMachines, &i)
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

func assignMachine(taskIdx int, addressField map[int]string, members []membership.Member, assignedMachines map[string]struct{}, i *int) {
	address := members[*i].Address
	addressField[taskIdx] = address
	assignedMachines[address] = struct{}{}
	*i = (*i + 1) % len(members)
}
