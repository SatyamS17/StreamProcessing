package rainstorm

import (
	"fmt"
	"slices"
)

type Command struct {
	ID string

	Op1Exe        string
	Op2Exe        string
	HydfsSrcFile  string
	HydfsDestFile string
	NumTasks      int
	Pattern       string
	Assignments   MachineAssignments
}

type MachineAssignments struct {
	LeaderMachineAddress   string
	SourceMachineAddresses []string
	Op1MachineAddresses    []string
	Op2MachineAddresses    []string
}

func (m *MachineAssignments) isSourceMachine(address string) bool {
	return slices.Contains(m.SourceMachineAddresses, address)
}

func (m *MachineAssignments) isOp1Machine(address string) bool {
	return slices.Contains(m.Op1MachineAddresses, address)
}

func (m *MachineAssignments) isOp2Machine(address string) bool {
	return slices.Contains(m.Op2MachineAddresses, address)
}

func (m *MachineAssignments) isAssigned(address string) bool {
	return m.isSourceMachine(address) || m.isOp1Machine(address) || m.isOp2Machine(address)
}

type Stage int

const (
	SourceStage Stage = iota
	Op1Stage
	Op2Stage
)

type Task struct {
	Stage Stage
	Index int
}

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
