package dht

import (
	"strings"
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
}

func NewAddPacket(source string, fileName string, hash string) *Packet {
	return &Packet{PACKET_ADD, source, fileName, hash, "", 0, ""}
}

func NewAddAckPacket(source string) *Packet {
	return &Packet{PACKET_ADD_ACK, source, "", "", "", 0, ""}
}

func NewGetPacket(source string, fileName string) *Packet {
	return &Packet{PACKET_GET, source, fileName, "", "", 0, ""}
}

func NewGetAckPacket(source string, chunkCount int) *Packet {
	return &Packet{PACKET_GET_ACK, source, "", "", "", chunkCount, ""}
}

func NewStorePacket(source string) *Packet {
	return &Packet{PACKET_STORE, source, "", "", "", 0, ""}
}

func NewStoreAckPacket(source string, fileNames []string) *Packet {
	return &Packet{PACKET_STORE_ACK, source, "", "", "", 0, strings.Join(fileNames, ",")}
}

func NewMergePacket(source string, fileName string) *Packet {
	return &Packet{PACKET_MERGE, source, fileName, "", "", 0, ""}
}

func NewRequestAppendPacket(source string, dfsfileName string, localfilename string) *Packet {
	return &Packet{REQUEST_APPEND, source, dfsfileName, "", localfilename, 0, ""}
}

func NewRequestMergePacket(source string, dfsfileName string) *Packet {
	return &Packet{REQUEST_MERGE, source, dfsfileName, "", "", 0, ""}
}

func NewRequestAckPacket(source string) *Packet {
	return &Packet{REQUEST_ACK, source, "", "", "", 0, ""}
}
