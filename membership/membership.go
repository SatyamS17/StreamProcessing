package membership

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"maps"
	"math/rand/v2"
	"mp4/util"
	"net"
	"os"
	"slices"
	"time"
)

const (
	portNumber              = "8000"
	protocolPeriod          = 2000 * time.Millisecond
	introducerServerAddress = "fa24-cs425-3901.cs.illinois.edu"
)

type Server struct {
	// Membership list stored as a map from server address to member details.
	membershipList map[Member]*MemberData

	onMembershipInit func()
	onRemoveMember   func(string)
	onAddMember      func(string)

	onRemoveWorker func(string)

	// Shuffled list of servers to ping.
	serversToPing []Member

	// Address of the current server.
	currentServer Member

	// The current buffer of the machine
	updates MemberUpdateBuffer

	// Current state (SUS or no).
	useSus bool

	// Incarnation number of current server.
	currentIncarnation int

	receivedJoinAck bool
}

func NewServer(hostname string) *Server {
	currentServer := Member{hostname, time.Now()}

	return &Server{
		membershipList:  make(map[Member]*MemberData),
		currentServer:   currentServer,
		useSus:          true,
		receivedJoinAck: currentServer.Address == introducerServerAddress, // don't need JOIN_ACK if we are the introducer
	}
}

func (s *Server) RunUDPServer() {
	// Start listening for UDP packets.
	pc, err := net.ListenPacket("udp", ":"+portNumber)
	if err != nil {
		log.Fatal("Unable to start membership server.", err)
	}
	defer pc.Close()

	for {
		// Decode incoming UDP packet into a packet type.
		raw := make([]byte, 2048) // TODO buffer size
		n, _, err := pc.ReadFrom(raw)
		if err != nil {
			fmt.Println(err)
			continue
		}
		buf := bytes.NewBuffer(raw[:n])
		dec := gob.NewDecoder(buf)

		var p Packet
		if err := dec.Decode(&p); err != nil {
			log.Fatal(err)
		}

		// Special case for processing JOIN_ACK. Buffer contains full membership list
		if p.T == PACKET_JOIN_ACK {
			for p.Updates.Len > 0 {
				u := p.Updates.pop()
				if u.T == UPDATE_JOINED && u.Member.Address != s.currentServer.Address {
					s.addMember(u.Member)
				}
			}

			s.receivedJoinAck = true
			s.onMembershipInit()
			continue
		}

		// Add any new updates to our current membership update buffer.
		for p.Updates.Len > 0 {
			s.updates.push(p.Updates.pop(), s.useSus)
		}

		// Process all membership updates.
		s.processMembershipUpdates()

		switch p.T {
		case PACKET_PING:
			log.Printf("Received PING from %s\n", p.Source.Address)

			// Send a ACK message back.
			ackP := Packet{PACKET_ACK, s.currentServer, s.updates}

			if err := util.SendUDPPacket(ackP, p.Source.Address, portNumber); err != nil {
				log.Printf("Unable to send ACK to %s\n", p.Source.Address)
				continue
			}

			log.Printf("Sent ACK to %s\n", p.Source.Address)
		case PACKET_ACK:
			m, ok := s.membershipList[p.Source]
			if !ok {
				log.Printf("Recived ACK from %s, but not in membership list\n", p.Source.Address)
				continue
			}

			log.Printf("Received ACK from %s\n", p.Source.Address)

			// Unlock channel for the server that sent the ACK.
			m.Ack <- struct{}{}
		case PACKET_JOIN_REQ:
			// Make a temporary buffer with the full membership list
			b := MemberUpdateBuffer{}
			for m := range s.membershipList {
				b.push(MemberUpdate{UPDATE_JOINED, m, 0, time.Now()}, s.useSus)
			}
			b.push(MemberUpdate{UPDATE_JOINED, s.currentServer, 0, time.Now()}, s.useSus)

			// Send a JOIN_ACK back to hte new machine
			ackP := Packet{PACKET_JOIN_ACK, s.currentServer, b}
			if err := util.SendUDPPacket(ackP, p.Source.Address, portNumber); err != nil {
				log.Printf("Unable to send JOIN_ACK to %s\n", p.Source.Address)
				continue
			}

			log.Printf("Sent JOIN_ACK to %s with buffer size %d\n", p.Source.Address, b.Len)

			s.addMember(p.Source)
			s.updates.push(MemberUpdate{UPDATE_JOINED, p.Source, s.membershipList[p.Source].Incarnation, time.Now()}, s.useSus)
		}
	}
}

func (s *Server) RunPeriodicPings() {
	for {
		nextPeriod := time.After(protocolPeriod)

		if !s.receivedJoinAck {
			log.Printf("Sending a ping to introducer (%s) to join.\n", introducerServerAddress)

			p := Packet{PACKET_JOIN_REQ, s.currentServer, MemberUpdateBuffer{}}
			if err := util.SendUDPPacket(p, introducerServerAddress, portNumber); err != nil {
				log.Println("Unable to send ping to introducer.")
			}
		}

		// Shuffle a list of servers to ping
		if len(s.serversToPing) == 0 {
			aliveServers := []Member{}
			for a := range s.membershipList {
				aliveServers = append(aliveServers, a)
			}

			if len(aliveServers) == 0 {
				log.Println("No servers to ping")
				<-nextPeriod
				continue
			}

			s.serversToPing = make([]Member, len(aliveServers))
			perm := rand.Perm(len(aliveServers))
			for i, v := range perm {
				s.serversToPing[v] = aliveServers[i]
			}
		}

		// Pop the last server in the server traversal list.
		target := s.serversToPing[len(s.serversToPing)-1]
		s.serversToPing = s.serversToPing[:len(s.serversToPing)-1]

		// Send a PING message.
		p := Packet{PACKET_PING, s.currentServer, s.updates}

		if err := util.SendUDPPacket(p, target.Address, portNumber); err != nil {
			log.Printf("Unable to send PING to %s\n", target.Address)
			<-nextPeriod
			continue
		}
		log.Printf("Sent PING to %s\n", target.Address)

		targetMemberData := s.membershipList[target]

		// Await an ack response with a timeout.
		select {
		case <-targetMemberData.Ack:
			if s.useSus && targetMemberData.Status == MEMBER_SUSPECT {
				targetMemberData.Status = MEMBER_CONNECTED
				s.updates.push(MemberUpdate{UPDATE_ALIVE, target, targetMemberData.Incarnation, time.Now()}, s.useSus)
			}
		case <-time.After(protocolPeriod - 200*time.Millisecond):
			if s.useSus {
				if targetMemberData.Status == MEMBER_CONNECTED {
					log.Printf("Didn't receive an ACK from %s. Marking as Suspect\n", target.Address)

					fmt.Printf("Marking %s as Suspect!!\n", target.Address)

					// Update status
					targetMemberData.Status = MEMBER_SUSPECT

					// Add update to buffer.
					s.updates.push(MemberUpdate{UPDATE_SUSPECT, target, targetMemberData.Incarnation, time.Now()}, s.useSus)

					go func() {
						time.Sleep(protocolPeriod * 4)
						if targetMemberData.Status == MEMBER_SUSPECT {
							log.Printf("Didn't receive an ALIVE for %s. Marking as failed\n", target.Address)
							// Remove member from membership list
							s.removeMember(target)

							// Add update to buffer.
							s.updates.push(MemberUpdate{UPDATE_FAILED, target, targetMemberData.Incarnation, time.Now()}, s.useSus)
						}
					}()
				}
			} else {
				log.Printf("Didn't receive an ACK from %s. Marking as failed\n", target.Address)

				fmt.Printf("Marking %s as failed!!\n", target.Address)

				// Remove member from membership list
				s.removeMember(target)

				// Add update to buffer.
				s.updates.push(MemberUpdate{UPDATE_FAILED, target, targetMemberData.Incarnation, time.Now()}, s.useSus)
			}
		}

		<-nextPeriod
	}
}

func (s *Server) CurrentServer() *Member {
	return &s.currentServer
}

func (s *Server) Members() []Member {
	return slices.Collect(maps.Keys(s.membershipList))
}

func (s *Server) OnMembershipInit(f func()) {
	s.onMembershipInit = f
}

func (s *Server) OnRemoveMember(f func(string)) {
	s.onRemoveMember = f
}

func (s *Server) OnRemoveWorker(f func(string)) {
	s.onRemoveWorker = f
}

func (s *Server) OnAddMember(f func(string)) {
	s.onAddMember = f
}

func (s *Server) Leave() {
	s.updates.push(MemberUpdate{UPDATE_FAILED, s.currentServer, s.currentIncarnation, time.Now()}, s.useSus)
	p := Packet{PACKET_PING, s.currentServer, s.updates}
	if err := util.SendUDPPacket(p, s.randomMember().Address, portNumber); err != nil {
		log.Println("Unable to send ping.")
	}

	os.Exit(0)
}

func (s *Server) DisableSus() {
	s.useSus = false

	s.updates.push(MemberUpdate{UPDATE_DISABLE_SUS, s.currentServer, s.currentIncarnation, time.Now()}, s.useSus)
	p := Packet{PACKET_PING, s.currentServer, s.updates}
	if err := util.SendUDPPacket(p, s.randomMember().Address, portNumber); err != nil {
		log.Println("Unable to send ping.")
	}
}

func (s *Server) EnableSus() {
	s.useSus = true

	s.updates.push(MemberUpdate{UPDATE_ENABLE_SUS, s.currentServer, s.currentIncarnation, time.Now()}, s.useSus)
	p := Packet{PACKET_PING, s.currentServer, s.updates}
	if err := util.SendUDPPacket(p, s.randomMember().Address, portNumber); err != nil {
		log.Println("Unable to send ping.")
	}
}

func (s *Server) UseSus() bool {
	return s.useSus
}

// Used to handle all updates in the update stack
func (s *Server) processMembershipUpdates() {
	if s.updates.Len == 0 {
		return
	}

	for i := s.updates.Len - 1; i >= 0; i-- {
		u := s.updates.Stack[i]

		// Ignore updates about ourselves
		if u.Member.Address == s.currentServer.Address {
			if s.useSus && u.T == UPDATE_SUSPECT {
				s.currentIncarnation++

				s.updates.push(MemberUpdate{UPDATE_ALIVE, s.currentServer, s.currentIncarnation, time.Now()}, s.useSus)
			}
			continue
		}

		switch u.T {
		case UPDATE_JOINED:
			// If not already joined, add to membership list.
			if _, ok := s.membershipList[u.Member]; !ok {
				s.addMember(u.Member)
			}
		case UPDATE_FAILED:
			// If not already failed, remove membership list.
			if _, ok := s.membershipList[u.Member]; ok {
				s.removeMember(u.Member)
			}
		case UPDATE_SUSPECT:
			// Mark machine as suspect.
			if d, ok := s.membershipList[u.Member]; ok && d.Status != MEMBER_SUSPECT {
				d.Status = MEMBER_SUSPECT
			}
		case UPDATE_ALIVE:
			// Unmark machine from being suspect.
			if d, ok := s.membershipList[u.Member]; ok {
				d.Status = MEMBER_CONNECTED
				d.Incarnation = u.Incarnation
			}
		case UPDATE_DISABLE_SUS:
			s.useSus = false
		case UPDATE_ENABLE_SUS:
			s.useSus = true
		}
	}
}

func (s *Server) removeMember(target Member) {
	log.Printf("Removing member %s.\n", target.Address)
	fmt.Printf("Removing member %s.\n", target.Address)

	// Remove from membership list.
	delete(s.membershipList, target)
	if s.onRemoveMember != nil {
		s.onRemoveMember(target.Address)
	}
	if s.onRemoveWorker != nil {
		s.onRemoveWorker(target.Address)
	}

	// Remove from shuffled list of servers to ping if present.
	s.serversToPing = slices.DeleteFunc(s.serversToPing, func(m Member) bool {
		return m == target
	})
}

func (s *Server) addMember(target Member) {
	log.Printf("Adding member %s.\n", target.Address)
	fmt.Printf("Adding member %s.\n", target.Address)

	// Add to membership list.
	s.membershipList[target] = &MemberData{MEMBER_CONNECTED, 0, make(chan struct{}, 10)}
	if s.receivedJoinAck {
		if s.onAddMember != nil {
			s.onAddMember(target.Address)
		}
	}

	// Add to shuffled list of servers to ping at random position.
	i := rand.IntN(len(s.serversToPing) + 1)
	s.serversToPing = slices.Insert(s.serversToPing, i, target)
}

func (s *Server) randomMember() *Member {
	for k := range s.membershipList {
		return &k
	}
	return nil
}
