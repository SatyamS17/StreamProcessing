package rainstorm

import "fmt"

type Command struct {
	Op1Exe        string
	Op2Exe        string
	HydfsSrcFile  string
	HydfsDestFile string

	Assignments MachineAssignments
}

type MachineAssignments struct {
	LeaderMachineAddress   string
	SourceMachineAddresses map[int]string
	Op1MachineAddresses    map[int]string
	Op2MachineAddresses    map[int]string

	AssignedMachines map[string]struct{}
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

type RecordStatus int

const (
	RECEIVED RecordStatus = iota
	OUTPUTTED
	ACKED

	PROCESSED
)

type OutputRecordArgs struct {
	Record Record
}

type ProcessRecordArgs struct {
	FromStage Stage
	Record    Record
}

func (r *Record) String(status RecordStatus, currentStage Stage) string {
	if status != PROCESSED {
		return fmt.Sprintf("%s:%s:%s<%s, %s>\n", r.ID, status, currentStage, r.Key, r.Value)
	} else {
		return fmt.Sprintf("<%s, %s>\n", r.Key, r.Value)
	}
}
