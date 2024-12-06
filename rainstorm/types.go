package rainstorm

type Command struct {
	Op1Exe        string
	Op2Exe        string
	HydfsSrcFile  string
	HydfsDestFile string

	Assignments MachineAssignments
}

type MachineAssignments struct {
	SourceMachineAddresses []string
	Op1MachineAddresses    []string
	Op2MachineAddresses    []string
}
