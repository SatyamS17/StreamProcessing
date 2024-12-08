package rainstorm

// func rpcCall(address string, function string, args any) error {
// 	client, err := rpc.DialHTTP("tcp", address+":"+rpcPortNumber)
// 	if err != nil {
// 		return err
// 	}

// 	var reply struct{}
// 	err = client.Call(function, args, &reply)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (s *Server) GetTaskIndexesFromAddress(addresses []string, address string) []int {
	var indexes []int
	for key, value := range addresses {
		if value == address {
			indexes = append(indexes, key)
		}
	}
	return indexes
}

func (m *MachineAssignments) getStageMap(address string) ([]string, Stage) {
	switch {
	case m.isSourceMachine(address):
		return m.SourceMachineAddresses, SourceStage
	case m.isSourceMachine(address):
		return m.Op1MachineAddresses, SourceStage
	case m.isSourceMachine(address):
		return m.Op2MachineAddresses, SourceStage
	default:
		return nil, 0
	}
}

func (status RecordStatus) String() string {
	switch status {
	case RECEIVED:
		return "RECEIVED"
	case OUTPUTTED:
		return "OUTPUTTED"
	case ACKED:
		return "ACKED"
	default:
		return "Unknown"
	}
}

func (status Stage) String() string {
	switch status {
	case SourceStage:
		return "SOURCE"
	case Op1Stage:
		return "OP1"
	case Op2Stage:
		return "OP2"
	default:
		return "Unknown"
	}
}
