package dht

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"mp4/membership"
	"mp4/util"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	portNumber         = "8001" // TCP server port number
	httpPortNumber     = "8002" // HTTP server port number
	dataDir            = "./data"
	MapJSON            = "mapdata.json"
	replicasCount  int = 3
	ackTimeout         = 5 * time.Second
)

type Server struct {
	membershipServer *membership.Server

	files               map[string]int           // Map of file names to number of chunks this server is storing
	fileHashes          map[string][]string      // Map of file names to hashing order of the chunks
	getChunkCounts      map[string]int           // Map of server address to number of chunks available (used in Get())
	acks                map[string]chan struct{} // Map of server address to ack channel
	requestAcks         map[string]chan struct{} // Map of server address to ack requests channel
	neighborStoredFiles []string                 // List of files a neighboring server is storing (used for STORE packets)
	lock                sync.Mutex
}

func NewServer(membershipServer *membership.Server) (*Server, error) {
	// Check if the data directory exists, and delete it if it does.
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		err := os.RemoveAll(dataDir)
		if err != nil {
			return nil, err
		}
	}

	// Make data directory.
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, err
	}

	s := Server{
		membershipServer: membershipServer,
		files:            make(map[string]int),
		fileHashes:       make(map[string][]string),
		getChunkCounts:   make(map[string]int),
		acks:             make(map[string]chan struct{}),
		requestAcks:      make(map[string]chan struct{}),
	}

	// Setup membership callbacks.
	s.membershipServer.OnMembershipInit(s.handleMembershipInit)
	s.membershipServer.OnRemoveMember(s.handleRemoveMember)
	s.membershipServer.OnAddMember(s.handleAddMember)

	return &s, nil
}

// Run TCP server to handle DHT packets.
func (s *Server) RunTCPServer() {
	pc, err := net.Listen("tcp", ":"+portNumber)
	if err != nil {
		fmt.Println("Unable to start DHT server.", err)
		os.Exit(1)
	}
	defer pc.Close()

	for {
		conn, err := pc.Accept()
		if err != nil {
			fmt.Println("Unable to accept connection.", err)
			continue
		}

		raw := make([]byte, 1024)
		n, err := conn.Read(raw)
		if err != nil {
			fmt.Println("Unable to read connection. ", err)
			continue
		}
		buf := bytes.NewBuffer(raw[:n])
		dec := gob.NewDecoder(buf)

		var p Packet
		if err := dec.Decode(&p); err != nil {
			log.Fatal(err)
		}

		go func() {
			s.HandlePacket(p)
			conn.Close()
		}()

	}
}

func (s *Server) HandlePacket(p Packet) {
	switch p.T {
	case PACKET_ADD:
		s.lock.Lock()

		if _, ok := s.files[p.FileName]; !ok {
			s.files[p.FileName] = 0
			s.fileHashes[p.FileName] = make([]string, 0)
		}

		url := fmt.Sprintf("http://%s:%s/%d", p.Source, httpPortNumber, util.Hash(p.FileName))
		to := fmt.Sprintf("%s/%d+%d", dataDir, util.Hash(p.FileName), s.files[p.FileName])

		if err := util.DownloadFile(url, to); err != nil {
			fmt.Printf("Unable to download file from %s to %s. %v\n", url, to, err)
			s.lock.Unlock()
			return
		}

		log.Printf("Downloaded %s from %s\n", to, url)

		s.files[p.FileName]++
		hashOrder := s.fileHashes[p.FileName]

		s.fileHashes[p.FileName] = append(hashOrder, p.Hash)
		toHash := fmt.Sprintf("%s/%s%s", dataDir, p.FileName, MapJSON)
		util.WriteJSONFile(toHash, s.fileHashes[p.FileName])

		s.lock.Unlock()

		ackP := NewAddAckPacket(s.membershipServer.CurrentServer().Address)
		if err := util.SendTCPPacket(ackP, p.Source, portNumber); err != nil {
			log.Println("Unable to send ACK")
			return
		}

	case PACKET_ADD_ACK:
		m, ok := s.acks[p.Source]
		if !ok {
			return
		}

		log.Printf("Received ACK from %s\n", p.Source)
		m <- struct{}{}
	case PACKET_GET:
		log.Println("Responding to get")
		chunks := s.files[p.FileName]
		ackP := NewGetAckPacket(s.membershipServer.CurrentServer().Address, chunks)
		if err := util.SendTCPPacket(ackP, p.Source, portNumber); err != nil {
			log.Println("Unable to send ACK")
			return
		}
		log.Println("Done responding")
	case PACKET_GET_ACK:
		m, ok := s.acks[p.Source]
		if !ok {
			return
		}

		log.Printf("Received ACK from %s\n", p.Source)

		s.getChunkCounts[p.Source] = p.ChunkCount
		m <- struct{}{}
	case REQUEST_APPEND:
		s.Append(p.LocalFileName, p.FileName)

		ackP := NewRequestAckPacket(s.membershipServer.CurrentServer().Address)
		if err := util.SendTCPPacket(ackP, p.Source, portNumber); err != nil {
			log.Println("Unable to send ACK")
			return
		}
	case REQUEST_MERGE:
		s.mergePrimary(p.FileName)

		ackP := NewRequestAckPacket(s.membershipServer.CurrentServer().Address)
		if err := util.SendTCPPacket(ackP, p.Source, portNumber); err != nil {
			log.Println("Unable to send ACK")
			return
		}
	case REQUEST_ACK:
		m, ok := s.requestAcks[p.Source]
		if !ok {
			return
		}

		log.Printf("Received ACK from %s\n", p.Source)
		m <- struct{}{}
	case PACKET_MERGE:
		s.mergeBackups(p.FileName, p.Source)

		ackP := NewRequestAckPacket(s.membershipServer.CurrentServer().Address)
		if err := util.SendTCPPacket(ackP, p.Source, portNumber); err != nil {
			log.Println("Unable to send ACK")
			return
		}
	case PACKET_STORE:
		ackP := NewStoreAckPacket(s.membershipServer.CurrentServer().Address, slices.Collect(maps.Keys(s.files)))
		if err := util.SendTCPPacket(ackP, p.Source, portNumber); err != nil {
			log.Println("Unable to send ACK")
			return
		}
	case PACKET_STORE_ACK:
		m, ok := s.acks[p.Source]
		if !ok {
			return
		}

		log.Printf("Received ACK from %s\n", p.Source)

		s.neighborStoredFiles = strings.Split(p.StoredFileNames, ",")
		m <- struct{}{}
	}
}

// Run HTTP server to serve data file publically.
func (s *Server) RunHTTPServer() {
	// Serve static files from the specified directory
	fileServer := http.FileServer(http.Dir(dataDir))

	// Handle the root URL by serving files
	http.Handle("/", fileServer)

	err := http.ListenAndServe(":"+httpPortNumber, nil)
	if err != nil {
		log.Fatal("Error starting server:", err)
	}
}

// Create a file called `dfsFileName` with the contents from `localFileName`.
func (s *Server) Create(localFileName string, dfsFileName string) {
	_, chunks := s.getBestReplica(dfsFileName)

	if chunks == -1 {
		fmt.Printf("Unable to create %s\n", dfsFileName)
		return
	}

	if chunks > 0 {
		fmt.Printf("File %s already exists\n", dfsFileName)
		return
	}

	s.add(localFileName, dfsFileName)
}

// Append to the file called `dfsFileName` with the contents from `localFileName`.
func (s *Server) Append(localFileName string, dfsFileName string) {
	_, chunks := s.getBestReplica(dfsFileName)

	if chunks == -1 {
		fmt.Printf("Unable to append %s\n", dfsFileName)
		return
	}

	if chunks == 0 {
		fmt.Printf("File %s doesn't exist\n", dfsFileName)
		return
	}

	s.add(localFileName, dfsFileName)
}

func (s *Server) add(localFileName string, dfsFileName string) {
	// Check if local file is valid
	_, err := os.Stat(localFileName)
	if err != nil {
		fmt.Printf("Local file (%s) not valid\n", localFileName)
		return
	}

	hashValue, err := util.HashFile(localFileName, sha256.New)
	if err != nil {
		log.Fatalf("Failed to hash file: %v", err)
	}

	tempFilePath := fmt.Sprintf("%s/%d", dataDir, util.Hash(dfsFileName))
	//defer os.Remove(tempFilePath)

	if err := util.CopyFile(localFileName, tempFilePath); err != nil {
		fmt.Println("Unable to copy local file.", err)
		return
	}

	log.Printf("Copied %s to %s\n", localFileName, tempFilePath)

	replicaAddresses := s.getReplicaAddresses(dfsFileName)
	done := make(chan bool, replicasCount)
	for _, replicaAddress := range replicaAddresses {
		fmt.Printf("Replicating on %s\n", replicaAddress)
		go func() {
			if s.membershipServer.CurrentServer().Address == replicaAddress {
				s.lock.Lock()

				if _, ok := s.files[dfsFileName]; !ok {
					s.files[dfsFileName] = 0
					s.fileHashes[dfsFileName] = make([]string, 0)
				}

				to := fmt.Sprintf("%s/%d+%d", dataDir, util.Hash(dfsFileName), s.files[dfsFileName])

				if err := util.CopyFile(tempFilePath, to); err != nil {
					log.Println("Unable to copy local file.", err)
					done <- false
					s.lock.Unlock()
					return
				}

				s.files[dfsFileName]++
				hashOrder := s.fileHashes[dfsFileName]

				s.fileHashes[dfsFileName] = append(hashOrder, hashValue)
				toHash := fmt.Sprintf("%s/%s%s", dataDir, dfsFileName, MapJSON)
				util.WriteJSONFile(toHash, s.fileHashes[dfsFileName])

				done <- true
				s.lock.Unlock()
			} else {
				p := NewAddPacket(s.membershipServer.CurrentServer().Address, dfsFileName, hashValue)
				err := s.sendPacketAndWaitAck(replicaAddress, *p)
				if err != nil {
					fmt.Println(err)
					done <- false
				} else {
					done <- true
				}
			}
		}()
	}

	responses := 0
	successes := 0
	for success := range done {
		responses++
		if success {
			successes++
		}

		if successes >= int(math.Ceil(float64(replicasCount)/2)) {
			break
		} else if responses == replicasCount {
			fmt.Printf("Unable to add (received %d successful responses)\n", successes)
			return
		}
	}

	fmt.Printf("Successfully created/appended %s\n", dfsFileName)
}

// Get the file called `dfsFileName` and save it to `localFileName`.
func (s *Server) Get(dfsFileName string, localFileName string) {
	replicaAddress, chunks := s.getBestReplica(dfsFileName)

	if chunks == -1 {
		fmt.Printf("Unable to get %s\n", dfsFileName)
		return
	}

	if chunks == 0 {
		fmt.Printf("File %s doesn't exist\n", dfsFileName)
		return
	}

	s.downloadFromReplica(replicaAddress, dfsFileName, localFileName, chunks)
}

func (s *Server) GetFromReplica(replicaAddress string, dfsFileName string, localFileName string) {
	chunks := s.getChunkCount(replicaAddress, dfsFileName)

	if chunks == -1 {
		fmt.Printf("Unable to get %s\n", dfsFileName)
		return
	}

	if chunks == 0 {
		fmt.Printf("%s not found on this address\n", dfsFileName)
		return
	}

	s.downloadFromReplica(replicaAddress, dfsFileName, localFileName, chunks)
}

func (s *Server) handleMembershipInit() {
	succAddress := s.succMember(s.membershipServer.CurrentServer().Address, 0)
	p := NewStorePacket(s.membershipServer.CurrentServer().Address)

	// Populates s.neighborStoredFiles
	if err := s.sendPacketAndWaitAck(succAddress, *p); err != nil {
		log.Println(err)
		return
	}

	for _, fileName := range s.neighborStoredFiles {
		if slices.Contains(s.getReplicaAddresses(fileName), s.membershipServer.CurrentServer().Address) {
			chunks := s.getChunkCount(succAddress, fileName)
			if chunks < 1 {
				return
			}

			log.Printf("Getting a copy of %s from %s\n", fileName, succAddress)

			s.files[fileName] = 0

			for chunk := range chunks {
				url := fmt.Sprintf("http://%s:%s/%d+%d", succAddress, httpPortNumber, util.Hash(fileName), chunk)
				to := fmt.Sprintf("%s/%d+%d", dataDir, util.Hash(fileName), chunk)

				if err := util.DownloadFile(url, to); err != nil {
					fmt.Printf("Unable to download file from %s to %s. %v\n", url, to, err)
					continue
				}

				log.Printf("Downloaded %s from %s\n", to, url)
				s.files[fileName]++
			}

			toHash := fmt.Sprintf("%s/%s%s", dataDir, fileName, MapJSON)
			urlHash := fmt.Sprintf("http://%s:%s/%s%s", succAddress, httpPortNumber, fileName, MapJSON)

			if err := util.DownloadFile(urlHash, toHash); err != nil {
				fmt.Printf("Unable to download file from %s to %s. %v\n", toHash, toHash, err)
				continue
			}

			s.UpdateFileHashesFromJSON(toHash, fileName)
		}
	}
}

func (s *Server) handleRemoveMember(removedMember string) {
	inRange := false
	for i := range replicasCount {
		if s.membershipServer.CurrentServer().Address == s.succMember(removedMember, i) {
			inRange = true
			break
		}
	}

	if !inRange {
		return
	}

	predAddress := s.predMember(s.membershipServer.CurrentServer().Address, 0)
	p := NewStorePacket(s.membershipServer.CurrentServer().Address)

	// Populates s.neighborStoredFiles
	if err := s.sendPacketAndWaitAck(predAddress, *p); err != nil {
		fmt.Println(err)
		return
	}

	// For each file the predecessor is storing, if we currently don't have it AND we find that the file's
	// hash now includes the current server as as replica, download it from the predecessor.
	for _, fileName := range s.neighborStoredFiles {
		if _, ok := s.files[fileName]; ok {
			continue
		}

		if slices.Contains(s.getReplicaAddresses(fileName), s.membershipServer.CurrentServer().Address) {
			chunks := s.getChunkCount(predAddress, fileName)
			if chunks < 1 {
				return
			}

			log.Printf("Getting a copy of %s from %s\n", fileName, predAddress)

			s.files[fileName] = 0

			for chunk := range chunks {
				url := fmt.Sprintf("http://%s:%s/%d+%d", predAddress, httpPortNumber, util.Hash(fileName), chunk)
				to := fmt.Sprintf("%s/%d+%d", dataDir, util.Hash(fileName), chunk)

				if err := util.DownloadFile(url, to); err != nil {
					fmt.Printf("Unable to download file from %s to %s. %v\n", url, to, err)
					continue
				}

				log.Printf("Downloaded %s from %s\n", to, url)
				s.files[fileName]++
			}

			toHash := fmt.Sprintf("%s/%s%s", dataDir, fileName, MapJSON)
			urlHash := fmt.Sprintf("http://%s:%s/%s%s", predAddress, httpPortNumber, fileName, MapJSON)

			if err := util.DownloadFile(urlHash, toHash); err != nil {
				fmt.Printf("Unable to download file from %s to %s. %v\n", toHash, toHash, err)
				continue
			}

			s.UpdateFileHashesFromJSON(toHash, fileName)
		}
	}
}

func (s *Server) handleAddMember(addedMember string) {
	inRange := false
	for i := range replicasCount {
		if s.membershipServer.CurrentServer().Address == s.succMember(addedMember, i) {
			inRange = true
			break
		}
	}

	if !inRange {
		return
	}

	for fileName := range s.files {
		if !slices.Contains(s.getReplicaAddresses(fileName), s.membershipServer.CurrentServer().Address) {
			log.Printf("Removing copy of %s\n", fileName)

			for chunk := range s.files[fileName] {
				os.Remove(fmt.Sprintf("%s/%d+%d", dataDir, util.Hash(fileName), chunk))
			}

			delete(s.files, fileName)
			delete(s.fileHashes, fileName)
		}
	}
}

func (s *Server) Ls(dfsFileName string) {
	replicas := make([]string, 0)

	// Check if current server is a replica
	_, ok := s.files[dfsFileName]
	if ok {
		replicas = append(replicas, s.membershipServer.CurrentServer().Address)
	}

	// Check if other servers are replicas
	for _, m := range s.membershipServer.Members() {
		chunks := s.getChunkCount(m.Address, dfsFileName)
		if chunks > 0 {
			replicas = append(replicas, m.Address)
		}
	}

	// Sort servers by ID
	sort.Slice(replicas, func(i, j int) bool {
		return util.AddressToID(replicas[i]) < util.AddressToID(replicas[j])
	})

	fmt.Println("File ID", util.Hash(dfsFileName)%10)
	for _, m := range replicas {
		fmt.Printf("%s (%d)\n", m, util.AddressToID(m))
	}
}

// For a file called `dfsFileName`, get the address of the best replica and the number of chunks it contains.
func (s *Server) getBestReplica(dfsFileName string) (string, int) {
	replicaAddresses := s.getReplicaAddresses(dfsFileName)
	done := make(chan bool, replicasCount)
	clear(s.getChunkCounts)
	for _, replicaAddress := range replicaAddresses {
		go func() {
			chunks := s.getChunkCount(replicaAddress, dfsFileName)
			done <- chunks != -1
		}()
	}

	responses := 0
	successes := 0
	for success := range done {
		responses++
		if success {
			successes++
		}

		if successes >= int(math.Ceil(float64(replicasCount)/2)) {
			break
		} else if responses == replicasCount {
			fmt.Printf("Unable to get (received %d successful responses)\n", successes)
			return "", -1
		}
	}

	maxChunks := 0
	var replicaAddress string
	for a, chunks := range s.getChunkCounts {
		if chunks > maxChunks {
			maxChunks = chunks
			replicaAddress = a
		}
	}

	return replicaAddress, maxChunks
}

// For a file called `dfsFileName`, get the number of chunks stored on `replicaAddress` server
// (0 indiciates file not found on server, -1 indicates an error communicating with the server).
func (s *Server) getChunkCount(replicaAddress string, dfsFileName string) int {
	delete(s.getChunkCounts, replicaAddress)

	p := NewGetPacket(s.membershipServer.CurrentServer().Address, dfsFileName)

	if err := s.sendPacketAndWaitAck(replicaAddress, *p); err != nil {
		log.Println(err)
		return -1
	}

	return s.getChunkCounts[replicaAddress]
}

// Download a given number of chunks of the file `dfsFileName` from `replicaAddress` and save to `localFileName`.
func (s *Server) downloadFromReplica(replicaAddress string, dfsFileName string, localFileName string, chunks int) {
	localFile, err := os.OpenFile(localFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Unable to write to %s\n", localFileName)
		return
	}
	defer localFile.Close()

	fmt.Printf("Downloading %d chunks of %s from %s\n", chunks, dfsFileName, replicaAddress)

	tmpFileName := "tmp"

	for i := range chunks {
		var tmpFile *os.File

		if s.membershipServer.CurrentServer().Address == replicaAddress {
			chunkFilePath := fmt.Sprintf("%s/%d+%d", dataDir, util.Hash(dfsFileName), i)
			tmpFile, err = os.Open(chunkFilePath)
			if err != nil {
				fmt.Printf("Unable to read chunk locally from %s\n", chunkFilePath)
				fmt.Println(err)

			}
		} else {
			url := fmt.Sprintf("http://%s:%s/%d+%d", replicaAddress, httpPortNumber, util.Hash(dfsFileName), i)
			if err := util.DownloadFile(url, tmpFileName); err != nil {
				fmt.Printf("Unable to download chunk from %s to %s. %v\n", url, tmpFileName, err)
				return
			}

			tmpFile, err = os.Open(tmpFileName)
			if err != nil {
				fmt.Printf("Unable to open temp download file #%d\n", i)
				return
			}
		}

		if _, err := io.Copy(localFile, tmpFile); err != nil {
			fmt.Printf("Unable to append chunk #%d\n", i)
			return
		}

		tmpFile.Close()
	}

	os.Remove(tmpFileName)

	fmt.Printf("Successfully saved to %s\n", localFileName)
}

func (s *Server) Store() {
	fmt.Printf("%s (%d) ------------\n", s.membershipServer.CurrentServer().Address, util.AddressToID(s.membershipServer.CurrentServer().Address)+1)

	for file := range s.files {
		if s.files[file] > 0 {
			fmt.Printf("%s (%d)\n", file, util.Hash(file)%10)
		}
	}
}

// Get the addresses of the replicas a file should be stored on.
func (s *Server) getReplicaAddresses(dfsFileName string) []string {
	fileID := util.Hash(dfsFileName) % 10

	// Get addresses to copy file over
	replicaAddresses := make([]string, replicasCount)

	serverIDs := s.sortedServerIDs()
	primaryServerID := -1
	for _, serverID := range serverIDs {
		if serverID >= fileID {
			primaryServerID = serverID
			break
		}
	}

	if primaryServerID == -1 {
		primaryServerID = serverIDs[0]
	}

	replicaAddresses[0] = util.IDToAddress(primaryServerID)
	for j := 1; j < replicasCount; j++ {
		replicaAddresses[j] = s.succMember(replicaAddresses[j-1], 0)
	}

	return replicaAddresses
}

// Get the k'th predecessor member of a given server address (k is 0-indexed).
func (s *Server) predMember(of string, k int) string {
	serverIDs := s.sortedServerIDs()
	slices.Reverse(serverIDs)

	for i, serverID := range serverIDs {
		if serverID < util.AddressToID(of) {
			return util.IDToAddress(serverIDs[(i-k+len(serverIDs))%len(serverIDs)])
		}
	}

	return util.IDToAddress(serverIDs[k%len(serverIDs)])
}

// Get the k'th successor member of a given server address (k is 0-indexed).
func (s *Server) succMember(of string, k int) string {
	serverIDs := s.sortedServerIDs()

	for i, serverID := range serverIDs {
		if serverID > util.AddressToID(of) {
			return util.IDToAddress(serverIDs[(i+k)%len(serverIDs)])
		}
	}

	return util.IDToAddress(serverIDs[k%len(serverIDs)])
}

// Get a list of all the server ID's in the ring sorted ascending (including current server).
func (s *Server) sortedServerIDs() []int {
	currentServerID := util.AddressToID(s.membershipServer.CurrentServer().Address)
	serverIDs := make([]int, 0, len(s.membershipServer.Members())+1)
	for _, m := range s.membershipServer.Members() {
		serverIDs = append(serverIDs, util.AddressToID(m.Address))
	}
	serverIDs = append(serverIDs, currentServerID)

	sort.Ints(serverIDs)

	return serverIDs
}

func (s *Server) sendPacketAndWaitAck(target string, p Packet) error {
	s.acks[target] = make(chan struct{}, 1)
	defer delete(s.acks, target)

	if err := util.SendTCPPacket(p, target, portNumber); err != nil {
		return fmt.Errorf("unable to send packet to %s", target)
	}

	log.Printf("Sent packet to %s\n", target)

	select {
	case <-s.acks[target]:
		log.Printf("Received ACK from %s\n", target)
		return nil
	case <-time.After(ackTimeout):
		return fmt.Errorf("timed out waiting for ACK from %s", target)
	}
}

func (s *Server) Multiappend(dfsFileName string, vmIds []int, localFiles []string) {

	for i, vmId := range vmIds {
		requestAddress := util.IDToAddress(vmId - 1)
		currentServerAdress := s.membershipServer.CurrentServer().Address
		go func() {
			if currentServerAdress == requestAddress {
				s.Append(localFiles[i], dfsFileName)
				log.Printf("Finished appending to %s\n", dfsFileName)
				fmt.Printf("%s appended successfully\n", currentServerAdress)
			} else {
				s.requestAcks[requestAddress] = make(chan struct{}, 1)

				p := NewRequestAppendPacket(currentServerAdress, dfsFileName, localFiles[i])

				if err := util.SendTCPPacket(p, requestAddress, portNumber); err != nil {
					log.Printf("Unable to send append request to %s. %v\n", requestAddress, err)
					return
				}

				log.Printf("Sent append request to %s\n", requestAddress)

				select {
				case <-s.requestAcks[requestAddress]:
					log.Printf("Received ACK from %s\n", requestAddress)
					fmt.Printf("%s appended successfully\n", requestAddress)
				case <-time.After(ackTimeout):
					log.Printf("Timed out waiting for ACK from %s\n", requestAddress)
					fmt.Printf("%s failed to append\n", requestAddress)
				}

				delete(s.requestAcks, requestAddress)
			}
		}()
	}
}

func (s *Server) Merge(dfsFileName string) {
	replicaAddresses := s.getReplicaAddresses(dfsFileName)
	primaryServerAddresses := replicaAddresses[0]

	go func() {
		// Check if current server is the primary one
		if s.membershipServer.CurrentServer().Address == primaryServerAddresses {
			// fufill merge request
			s.mergePrimary(dfsFileName)
		} else {
			// send merge request
			s.requestAcks[primaryServerAddresses] = make(chan struct{}, 1)

			p := NewRequestMergePacket(s.membershipServer.CurrentServer().Address, dfsFileName)

			if err := util.SendTCPPacket(p, primaryServerAddresses, portNumber); err != nil {
				log.Printf("Unable to send append request to %s. %v\n", primaryServerAddresses, err)
				return
			}

			log.Printf("Sent append request to %s\n", primaryServerAddresses)

			select {
			case <-s.requestAcks[primaryServerAddresses]:
				log.Printf("Received ACK from %s\n", primaryServerAddresses)
			case <-time.After(ackTimeout * 2):
				log.Printf("Timed out waiting for ACK from %s\n", primaryServerAddresses)
			}

			delete(s.requestAcks, primaryServerAddresses)
		}
	}()

	fmt.Printf("Successfully merged %s\n", dfsFileName)
}

func (s *Server) mergePrimary(dfsFileName string) {
	replicaAddresses := s.getReplicaAddresses(dfsFileName)

	for _, replicaAddress := range replicaAddresses {
		fmt.Printf("Merging on %s\n", replicaAddress)
		go func() {
			if s.membershipServer.CurrentServer().Address != replicaAddress {
				s.requestAcks[replicaAddress] = make(chan struct{}, 1)

				p := NewMergePacket(s.membershipServer.CurrentServer().Address, dfsFileName)

				if err := util.SendTCPPacket(p, replicaAddress, portNumber); err != nil {
					log.Printf("Unable to send append request to %s. %v\n", replicaAddress, err)
					return
				}

				log.Printf("Sent append request to %s\n", replicaAddress)

				select {
				case <-s.requestAcks[replicaAddress]:
					log.Printf("Received ACK from %s\n", replicaAddress)
				case <-time.After(ackTimeout * 2):
					log.Printf("Timed out waiting for ACK from %s\n", replicaAddress)
				}

				delete(s.requestAcks, replicaAddress)
			}
		}()
	}
}

func (s *Server) mergeBackups(dfsFileName string, source string) {
	to := fmt.Sprintf("%s/%s%s", dataDir, dfsFileName, "mapdata.json")
	url := fmt.Sprintf("http://%s:%s/%s%s", source, httpPortNumber, dfsFileName, MapJSON)

	if err := util.DownloadFile(url, to); err != nil {
		fmt.Printf("Unable to download file from %s to %s. %v\n", url, to, err)
		return
	}

	log.Printf("Downloaded %s from %s\n", url, to)

	file, err := os.Open(to)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	backupHashing := s.fileHashes[dfsFileName]
	var primaryHashes []string
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&primaryHashes)
	if err != nil {
		log.Fatal(err)
	}

	indexMap := make(map[string][]int)
	for i, value := range primaryHashes {
		indexMap[value] = append(indexMap[value], i)
	}

	// Track the "next available position" for each hash
	positionMap := make(map[string]int)

	// Iterate over backupHashing to rearrange based on primaryHashes
	for i := 0; i < len(backupHashing); i++ {
		currentElement := backupHashing[i]

		// Get the next available target index for the current element from indexMap
		targetIndexes := indexMap[currentElement]
		targetIndex := targetIndexes[positionMap[currentElement]]

		// Print the current index and the target index if they are different
		if i != targetIndex {
			log.Printf("Chunk '%d' will be switched with Chunk '%d'.\n", i, targetIndex)
			file1 := fmt.Sprintf("%s/%d+%d", dataDir, util.Hash(dfsFileName), i)
			file2 := fmt.Sprintf("%s/%d+%d", dataDir, util.Hash(dfsFileName), targetIndex)
			util.SwapFiles(file1, file2)

			backupHashing[i], backupHashing[targetIndex] = backupHashing[targetIndex], backupHashing[i]
			s.fileHashes[dfsFileName] = backupHashing

		} else {
			positionMap[currentElement]++
		}

	}

}

func (s *Server) UpdateFileHashesFromJSON(JSONfileName string, dfsFileName string) {
	file, err := os.Open(JSONfileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var hashes []string

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&hashes)
	if err != nil {
		log.Fatal(err)
	}

	s.fileHashes[dfsFileName] = hashes
}
