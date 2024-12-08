package dht

import (
	"strings"

	"github.com/google/uuid"
)

type PacketType int

const (
	PACKET_ADD PacketType = iota
	PACKET_ADD_ACK

	PACKET_GET
	PACKET_GET_ACK

	REQUEST_APPEND
	REQUEST_MERGE
	REQUEST_ACK

	PACKET_MERGE
	PACKET_MERGE_ACK

	PACKET_STORE
	PACKET_STORE_ACK
)

type Packet struct {
	T      PacketType
	Source string

	FileName      string // DFS file name
	Hash          string // hash of the contents
	LocalFileName string // Used for multiappend to refer to localfile used to append
	ChunkCount    int    // Number of chunks that exist for a file for GET_ACK packets

	StoredFileNames string
	ID              string
}

func NewAddPacket(source string, fileName string, hash string) *Packet {
	return &Packet{
		T:        PACKET_ADD,
		Source:   source,
		FileName: fileName,
		Hash:     hash,
		ID:       uuid.NewString(),
	}
}

func NewAddAckPacket(source string, id string) *Packet {
	return &Packet{
		T:      PACKET_ADD_ACK,
		Source: source,
		ID:     id,
	}
}

func NewGetPacket(source string, fileName string) *Packet {
	return &Packet{
		T:        PACKET_GET,
		Source:   source,
		FileName: fileName,
		ID:       uuid.NewString(),
	}
}

func NewGetAckPacket(source string, chunkCount int, id string) *Packet {
	return &Packet{
		T:          PACKET_GET_ACK,
		Source:     source,
		ChunkCount: chunkCount,
		ID:         id,
	}
}

func NewStorePacket(source string) *Packet {
	return &Packet{
		T:      PACKET_STORE,
		Source: source,
		ID:     uuid.NewString(),
	}
}

func NewStoreAckPacket(source string, fileNames []string, id string) *Packet {
	return &Packet{
		T:               PACKET_STORE_ACK,
		Source:          source,
		StoredFileNames: strings.Join(fileNames, ","),
		ID:              id,
	}
}

func NewMergePacket(source string, fileName string) *Packet {
	return &Packet{
		T:        PACKET_MERGE,
		Source:   source,
		FileName: fileName,
	}
}

func NewRequestAppendPacket(source string, dfsfileName string, localfilename string) *Packet {
	return &Packet{
		T:             REQUEST_APPEND,
		Source:        source,
		FileName:      dfsfileName,
		LocalFileName: localfilename,
	}
}

func NewRequestMergePacket(source string, dfsfileName string) *Packet {
	return &Packet{
		T:        REQUEST_MERGE,
		Source:   source,
		FileName: dfsfileName,
	}
}

func NewRequestAckPacket(source string) *Packet {
	return &Packet{
		T:      REQUEST_ACK,
		Source: source,
	}
}
