package rainstorm

// Used to get all indexes(tasks) that a machine/worker is assigned
func (s *Server) GetTaskIndexesFromAddress(addresses []string, address string) []int {
	var indexes []int
	for key, value := range addresses {
		if value == address {
			indexes = append(indexes, key)
		}
	}
	return indexes
}

// Convert RecordStatus enum to string
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

// Convert Stage enum to string
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
