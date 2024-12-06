package rainstorm

type Command struct {
	Op1Exe        string
	Op2Exe        string
	HydfsSrcFile  string
	HydfsDestFile string

	Assignments MachineAssignments
}

type MachineAssignments struct {
	LeaderMachineAddress   string
	SourceMachineAddresses []string
	Op1MachineAddresses    []string
	Op2MachineAddresses    []string
}

type Stage int

const (
	SourceStage Stage = iota
	Op1Stage
	Op2Stage
)

type Record struct {
	ID    string
	Key   string
	Value string
}
