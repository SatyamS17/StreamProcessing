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

func (s *Server) GetTaskIndexFromAddress(addresses map[int]string, address string) int {
	for key, value := range addresses {
		if value == address {
			return key
		}
	}
	return 0
}

func (s *Server) GetTaskAddressFromIndex(addresses map[int]string, idx int) string {
	target := idx % len(addresses)
	value := addresses[target]
	return value
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
