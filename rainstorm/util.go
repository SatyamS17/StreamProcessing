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

func (s *Server) isAddressInMap(addresses map[int]string, address string) bool {
	for _, value := range addresses {
		if value == address {
			return true
		}
	}
	return false
}

func (s *Server) GetTaskIndexesFromAddress(addresses map[int]string, address string) []int {
	var indexes []int
	for key, value := range addresses {
		if value == address {
			indexes = append(indexes, key)
		}
	}
	return indexes
}

func (s *Server) GetTaskAddressFromIndex(addresses map[int]string, idx int) string {
	target := idx % len(addresses)
	value := addresses[target]
	return value
}

func (s *Server) getStageMap(address string) (map[int]string, Stage) {
	switch {
	case s.isAddressInMap(s.command.Assignments.SourceMachineAddresses, address):
		return s.command.Assignments.SourceMachineAddresses, SourceStage
	case s.isAddressInMap(s.command.Assignments.Op1MachineAddresses, address):
		return s.command.Assignments.Op1MachineAddresses, Op1Stage
	case s.isAddressInMap(s.command.Assignments.Op2MachineAddresses, address):
		return s.command.Assignments.Op2MachineAddresses, Op2Stage
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
