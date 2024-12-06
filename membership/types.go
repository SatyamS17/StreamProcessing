package membership

import (
	"log"
	"time"
)

// A member in the membership group.
type Member struct {
	Address    string    // Machine address.
	TimeJoined time.Time // Time the machine joined group.
}

// Declare status "enum"
type MemberStatus int

const (
	MEMBER_CONNECTED MemberStatus = iota
	MEMBER_SUSPECT
)

// Data associated with a member in the group.
type MemberData struct {
	Status      MemberStatus
	Incarnation int
	Ack         chan struct{}
}

type MemberUpdateType int

const (
	UPDATE_NONE MemberUpdateType = iota
	UPDATE_JOINED
	UPDATE_FAILED
	UPDATE_SUSPECT
	UPDATE_ALIVE

	UPDATE_ENABLE_SUS
	UPDATE_DISABLE_SUS
)

// A membership update.
type MemberUpdate struct {
	T           MemberUpdateType
	Member      Member
	Incarnation int
	UpdateTime  time.Time // Time the update occured.
}

// A buffer of recent membership updates.
type MemberUpdateBuffer struct {
	Stack [10]MemberUpdate
	Len   int
}

// Delete an update at a specified index.
func (b *MemberUpdateBuffer) delete(idx int) {
	for i := idx; i < len(b.Stack)-1; i++ {
		b.Stack[i] = b.Stack[i+1]
	}
	b.Stack[len(b.Stack)-1] = MemberUpdate{}
	b.Len--
}

// Push a new membership update (sorted by newest first).
func (b *MemberUpdateBuffer) push(u MemberUpdate, useSus bool) {
	for i := b.Len - 1; i >= 0; i-- {
		cur := b.Stack[i]

		if time.Now().After(cur.UpdateTime.Add(10 * time.Second)) {
			b.delete(i)
			continue
		}

		if cur.Member == u.Member {
			if cur.T == u.T {
				b.delete(i)
				continue
			}

			// Prioritize failures over joins.
			if cur.T == UPDATE_JOINED && u.T == UPDATE_FAILED {
				b.delete(i)
				continue
			}

			if cur.T == UPDATE_FAILED && u.T == UPDATE_JOINED {
				return
			}

			// Suspicion logic with incarnations.
			if useSus {
				if u.T == UPDATE_ALIVE {
					if (cur.T == UPDATE_SUSPECT || cur.T == UPDATE_ALIVE) && u.Incarnation > cur.Incarnation {
						b.delete(i)
						continue
					}
				}

				if u.T == UPDATE_SUSPECT {
					if (cur.T == UPDATE_SUSPECT && u.Incarnation > cur.Incarnation) || (cur.T == UPDATE_ALIVE && u.Incarnation >= cur.Incarnation) {
						b.delete(i)
						continue
					}
				}

				if u.T == UPDATE_FAILED && (cur.T == UPDATE_SUSPECT || cur.T == UPDATE_ALIVE) {
					b.delete(i)
					continue
				}

				if (cur.T == UPDATE_DISABLE_SUS || cur.T == UPDATE_ENABLE_SUS) && (u.T == UPDATE_DISABLE_SUS || u.T == UPDATE_ENABLE_SUS) {
					b.delete(i)
					continue
				}
			}
		}
	}

	added := false
	for i := 0; i < len(b.Stack); i++ {
		if u.UpdateTime.After(b.Stack[i].UpdateTime) {
			for j := len(b.Stack) - 2; j >= i; j-- {
				b.Stack[j+1] = b.Stack[j]
			}
			b.Stack[i] = u
			added = true
			break
		}
	}

	if added {
		b.Len++
		if b.Len == len(b.Stack)+1 {
			b.Len--
		}
	}
}

// Pop and return a value.
func (b *MemberUpdateBuffer) pop() MemberUpdate {
	if b.Len == 0 {
		log.Fatal("Attempted to pop empty buffer")
	}

	u := b.Stack[b.Len-1]
	b.Stack[b.Len-1] = MemberUpdate{}
	b.Len--
	return u
}

type PacketType int

const (
	PACKET_PING PacketType = iota
	PACKET_ACK
	PACKET_JOIN_REQ // Initial join request to the introducer.
	PACKET_JOIN_ACK // Initial join request ack from the introducer.
)

type Packet struct {
	T       PacketType
	Source  Member // Server that sent this packet
	Updates MemberUpdateBuffer
}
