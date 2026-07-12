package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	// DefaultPingInterval is how often each node pings a random peer.
	DefaultPingInterval = 500 * time.Millisecond

	// DefaultPingTimeout is how long to wait for a ping ack.
	DefaultPingTimeout = 200 * time.Millisecond

	// DefaultSuspicionTimeout is how long a node stays in SUSPECT before DEAD.
	DefaultSuspicionTimeout = 3 * time.Second

	// DefaultPingReqCount is the number of indirect ping-req probes (K).
	DefaultPingReqCount = 3

	// maxUDPPacketSize is the maximum UDP packet size.
	maxUDPPacketSize = 65507

	// maxPiggybackUpdates is the max number of membership updates piggybacked per message.
	maxPiggybackUpdates = 10
)

// MessageType identifies the type of SWIM protocol message.
type MessageType uint8

const (
	MsgPing    MessageType = 1
	MsgAck     MessageType = 2
	MsgPingReq MessageType = 3
	MsgJoin    MessageType = 4
)

// SwimMessage is a SWIM protocol message sent over UDP.
type SwimMessage struct {
	Type        MessageType       `json:"type"`
	SenderID    string            `json:"sender_id"`
	SenderAddr  string            `json:"sender_addr"` // Gossip addr of sender.
	TargetID    string            `json:"target_id"`    // For ping-req: the indirect target.
	Incarnation uint64            `json:"incarnation"`
	Updates     []MembershipUpdate `json:"updates,omitempty"` // Piggybacked updates.
}

// MembershipUpdate represents a change in a node's membership status.
type MembershipUpdate struct {
	NodeID      string     `json:"node_id"`
	GRPCAddr    string     `json:"grpc_addr"`
	GossipAddr  string     `json:"gossip_addr"`
	HTTPAddr    string     `json:"http_addr"`
	Status      NodeStatus `json:"status"`
	Incarnation uint64     `json:"incarnation"`
}

// MembershipEventType is the type of membership change.
type MembershipEventType int

const (
	EventJoin    MembershipEventType = iota
	EventAlive
	EventSuspect
	EventDead
	EventLeft
)

// MembershipEvent is emitted when membership changes.
type MembershipEvent struct {
	Type   MembershipEventType
	NodeID string
}

// Membership implements the SWIM protocol for decentralized failure detection
// and membership management.
type Membership struct {
	mu sync.RWMutex

	selfID     string
	selfInfo   *NodeInfo
	registry   *NodeRegistry
	incarnation uint64

	// UDP listener for gossip.
	conn *net.UDPConn
	addr string

	// Pending acks: maps target nodeID → ack channel.
	pendingAcks map[string]chan struct{}

	// Updates to disseminate (piggybacked on messages).
	updateQueue []MembershipUpdate
	updateMu    sync.Mutex

	// Suspicion timers: nodeID → timer.
	suspicionTimers map[string]*time.Timer

	// Configuration.
	pingInterval      time.Duration
	pingTimeout       time.Duration
	suspicionTimeout  time.Duration
	pingReqCount      int

	// Callbacks.
	onEvent func(MembershipEvent)

	// Lifecycle.
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewMembership creates a new SWIM membership manager.
func NewMembership(selfInfo *NodeInfo, registry *NodeRegistry) *Membership {
	return &Membership{
		selfID:           selfInfo.ID,
		selfInfo:         selfInfo,
		registry:         registry,
		incarnation:      0,
		pendingAcks:      make(map[string]chan struct{}),
		updateQueue:      make([]MembershipUpdate, 0),
		suspicionTimers:  make(map[string]*time.Timer),
		pingInterval:     DefaultPingInterval,
		pingTimeout:      DefaultPingTimeout,
		suspicionTimeout: DefaultSuspicionTimeout,
		pingReqCount:     DefaultPingReqCount,
		stopCh:           make(chan struct{}),
	}
}

// SetOnEvent sets the callback for membership change events.
func (m *Membership) SetOnEvent(fn func(MembershipEvent)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onEvent = fn
}

// Start begins the SWIM protocol goroutines.
func (m *Membership) Start(listenAddr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return fmt.Errorf("resolving UDP address %s: %w", listenAddr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("listening on UDP %s: %w", listenAddr, err)
	}

	m.conn = conn
	m.addr = listenAddr

	// Register self in the registry.
	m.registry.Register(m.selfInfo)

	// Broadcast our join.
	m.queueUpdate(MembershipUpdate{
		NodeID:      m.selfID,
		GRPCAddr:    m.selfInfo.GRPCAddr,
		GossipAddr:  m.selfInfo.GossipAddr,
		HTTPAddr:    m.selfInfo.HTTPAddr,
		Status:      StatusAlive,
		Incarnation: m.incarnation,
	})

	// Start receiver goroutine.
	m.wg.Add(1)
	go m.receiveLoop()

	// Start ping goroutine.
	m.wg.Add(1)
	go m.pingLoop()

	log.Printf("[swim] started on %s (node: %s)", listenAddr, m.selfID)
	return nil
}

// Stop gracefully shuts down the SWIM protocol.
func (m *Membership) Stop() {
	// Broadcast leave.
	m.selfInfo.SetStatus(StatusLeft)
	m.queueUpdate(MembershipUpdate{
		NodeID:      m.selfID,
		Status:      StatusLeft,
		Incarnation: m.incarnation,
	})

	// Give a brief moment for the leave message to disseminate.
	time.Sleep(50 * time.Millisecond)

	close(m.stopCh)
	if m.conn != nil {
		m.conn.Close()
	}
	m.wg.Wait()

	// Cancel all suspicion timers.
	m.mu.Lock()
	for _, timer := range m.suspicionTimers {
		timer.Stop()
	}
	m.mu.Unlock()

	log.Printf("[swim] stopped (node: %s)", m.selfID)
}

// JoinCluster contacts seed nodes to join an existing cluster.
func (m *Membership) JoinCluster(seedAddrs []string) error {
	if len(seedAddrs) == 0 {
		return nil
	}

	joinMsg := SwimMessage{
		Type:        MsgJoin,
		SenderID:    m.selfID,
		SenderAddr:  m.selfInfo.GossipAddr,
		Incarnation: m.incarnation,
		Updates: []MembershipUpdate{
			{
				NodeID:      m.selfID,
				GRPCAddr:    m.selfInfo.GRPCAddr,
				GossipAddr:  m.selfInfo.GossipAddr,
				HTTPAddr:    m.selfInfo.HTTPAddr,
				Status:      StatusAlive,
				Incarnation: m.incarnation,
			},
		},
	}

	data, err := json.Marshal(joinMsg)
	if err != nil {
		return fmt.Errorf("marshaling join message: %w", err)
	}

	joined := false
	for _, addr := range seedAddrs {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			log.Printf("[swim] could not resolve seed %s: %v", addr, err)
			continue
		}

		_, err = m.conn.WriteToUDP(data, udpAddr)
		if err != nil {
			log.Printf("[swim] could not send join to %s: %v", addr, err)
			continue
		}

		log.Printf("[swim] sent join request to %s", addr)
		joined = true
	}

	if !joined {
		return fmt.Errorf("failed to contact any seed nodes")
	}

	return nil
}

// --- Protocol loops ---

// receiveLoop reads incoming UDP messages.
func (m *Membership) receiveLoop() {
	defer m.wg.Done()

	buf := make([]byte, maxUDPPacketSize)
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		m.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, remoteAddr, err := m.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-m.stopCh:
				return
			default:
				log.Printf("[swim] read error: %v", err)
				continue
			}
		}

		var msg SwimMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Printf("[swim] invalid message from %s: %v", remoteAddr, err)
			continue
		}

		m.handleMessage(msg, remoteAddr)
	}
}

// pingLoop periodically pings a random member.
func (m *Membership) pingLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.pingRandomMember()
		}
	}
}

// --- Message handling ---

// handleMessage processes an incoming SWIM message.
func (m *Membership) handleMessage(msg SwimMessage, from *net.UDPAddr) {
	// Process piggybacked membership updates.
	for _, update := range msg.Updates {
		m.applyUpdate(update)
	}

	switch msg.Type {
	case MsgPing:
		m.handlePing(msg, from)
	case MsgAck:
		m.handleAck(msg)
	case MsgPingReq:
		m.handlePingReq(msg, from)
	case MsgJoin:
		m.handleJoin(msg, from)
	}
}

// handlePing responds to a ping with an ack.
func (m *Membership) handlePing(msg SwimMessage, from *net.UDPAddr) {
	ack := SwimMessage{
		Type:        MsgAck,
		SenderID:    m.selfID,
		SenderAddr:  m.selfInfo.GossipAddr,
		Incarnation: m.incarnation,
		Updates:     m.drainUpdates(),
	}

	data, err := json.Marshal(ack)
	if err != nil {
		log.Printf("[swim] error marshaling ack: %v", err)
		return
	}

	m.conn.WriteToUDP(data, from)
}

// handleAck processes an ack response — signals waiting goroutine.
func (m *Membership) handleAck(msg SwimMessage) {
	m.mu.Lock()
	if ch, ok := m.pendingAcks[msg.SenderID]; ok {
		close(ch)
		delete(m.pendingAcks, msg.SenderID)
	}
	m.mu.Unlock()

	// Mark the node as alive.
	if info, ok := m.registry.Get(msg.SenderID); ok {
		info.Touch()
	}
}

// handlePingReq performs an indirect probe on behalf of the requester.
func (m *Membership) handlePingReq(msg SwimMessage, from *net.UDPAddr) {
	targetInfo, ok := m.registry.Get(msg.TargetID)
	if !ok {
		return
	}

	// Ping the target on behalf of the requester.
	acked := m.sendPingAndWait(targetInfo.GossipAddr, msg.TargetID)

	if acked {
		// Forward the ack back to the original requester.
		ack := SwimMessage{
			Type:        MsgAck,
			SenderID:    msg.TargetID,
			SenderAddr:  targetInfo.GossipAddr,
			Incarnation: m.incarnation,
		}

		data, _ := json.Marshal(ack)
		m.conn.WriteToUDP(data, from)
	}
}

// handleJoin processes a join request — adds the new node and responds with membership.
func (m *Membership) handleJoin(msg SwimMessage, from *net.UDPAddr) {
	log.Printf("[swim] received join from %s at %s", msg.SenderID, from)

	// Send our full membership list back.
	allNodes := m.registry.GetAll()
	updates := make([]MembershipUpdate, 0, len(allNodes))
	for _, node := range allNodes {
		updates = append(updates, MembershipUpdate{
			NodeID:      node.ID,
			GRPCAddr:    node.GRPCAddr,
			GossipAddr:  node.GossipAddr,
			HTTPAddr:    node.HTTPAddr,
			Status:      node.GetStatus(),
			Incarnation: node.Incarnation,
		})
	}

	resp := SwimMessage{
		Type:        MsgAck,
		SenderID:    m.selfID,
		SenderAddr:  m.selfInfo.GossipAddr,
		Incarnation: m.incarnation,
		Updates:     updates,
	}

	data, _ := json.Marshal(resp)
	m.conn.WriteToUDP(data, from)
}

// --- Ping mechanics ---

// pingRandomMember picks a random member and pings them.
func (m *Membership) pingRandomMember() {
	allNodes := m.registry.GetAll()

	// Filter to only alive/suspect nodes (excluding self).
	var candidates []*NodeInfo
	for _, node := range allNodes {
		if node.ID != m.selfID && (node.GetStatus() == StatusAlive || node.GetStatus() == StatusSuspect) {
			candidates = append(candidates, node)
		}
	}

	if len(candidates) == 0 {
		return
	}

	// Pick a random target.
	target := candidates[rand.Intn(len(candidates))]

	// Direct ping.
	acked := m.sendPingAndWait(target.GossipAddr, target.ID)
	if acked {
		return
	}

	// Direct ping failed — try indirect probes via ping-req.
	acked = m.sendPingReqs(target)
	if acked {
		return
	}

	// All probes failed — mark as suspect.
	m.markSuspect(target.ID)
}

// sendPingAndWait sends a ping and waits for an ack within the timeout.
func (m *Membership) sendPingAndWait(addr string, targetID string) bool {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return false
	}

	// Register pending ack.
	ackCh := make(chan struct{})
	m.mu.Lock()
	m.pendingAcks[targetID] = ackCh
	m.mu.Unlock()

	// Send ping.
	ping := SwimMessage{
		Type:        MsgPing,
		SenderID:    m.selfID,
		SenderAddr:  m.selfInfo.GossipAddr,
		Incarnation: m.incarnation,
		Updates:     m.drainUpdates(),
	}

	data, err := json.Marshal(ping)
	if err != nil {
		return false
	}

	m.conn.WriteToUDP(data, udpAddr)

	// Wait for ack.
	select {
	case <-ackCh:
		return true
	case <-time.After(m.pingTimeout):
		// Clean up pending ack.
		m.mu.Lock()
		delete(m.pendingAcks, targetID)
		m.mu.Unlock()
		return false
	case <-m.stopCh:
		return false
	}
}

// sendPingReqs sends indirect ping-req probes to K random members.
func (m *Membership) sendPingReqs(target *NodeInfo) bool {
	allNodes := m.registry.GetAll()

	// Find K random members (excluding self and target).
	var proxies []*NodeInfo
	for _, node := range allNodes {
		if node.ID != m.selfID && node.ID != target.ID && node.GetStatus() == StatusAlive {
			proxies = append(proxies, node)
		}
	}

	if len(proxies) == 0 {
		return false
	}

	// Shuffle and take up to K.
	rand.Shuffle(len(proxies), func(i, j int) {
		proxies[i], proxies[j] = proxies[j], proxies[i]
	})
	if len(proxies) > m.pingReqCount {
		proxies = proxies[:m.pingReqCount]
	}

	// Register pending ack for the target.
	ackCh := make(chan struct{})
	m.mu.Lock()
	m.pendingAcks[target.ID] = ackCh
	m.mu.Unlock()

	// Send ping-req to each proxy.
	for _, proxy := range proxies {
		udpAddr, err := net.ResolveUDPAddr("udp", proxy.GossipAddr)
		if err != nil {
			continue
		}

		pingReq := SwimMessage{
			Type:        MsgPingReq,
			SenderID:    m.selfID,
			SenderAddr:  m.selfInfo.GossipAddr,
			TargetID:    target.ID,
			Incarnation: m.incarnation,
		}

		data, _ := json.Marshal(pingReq)
		m.conn.WriteToUDP(data, udpAddr)
	}

	// Wait for indirect ack.
	select {
	case <-ackCh:
		return true
	case <-time.After(m.pingTimeout * 2): // Double timeout for indirect.
		m.mu.Lock()
		delete(m.pendingAcks, target.ID)
		m.mu.Unlock()
		return false
	case <-m.stopCh:
		return false
	}
}

// --- State transitions ---

// markSuspect transitions a node to SUSPECT status and starts a suspicion timer.
func (m *Membership) markSuspect(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.registry.Get(nodeID)
	if !ok {
		return
	}

	if info.GetStatus() == StatusDead || info.GetStatus() == StatusLeft {
		return // Already dead or left.
	}

	if info.GetStatus() == StatusSuspect {
		return // Already suspect.
	}

	info.SetStatus(StatusSuspect)
	log.Printf("[swim] node %s marked as SUSPECT", nodeID)

	m.queueUpdateLocked(MembershipUpdate{
		NodeID:      nodeID,
		Status:      StatusSuspect,
		Incarnation: info.Incarnation,
	})

	m.emitEvent(MembershipEvent{Type: EventSuspect, NodeID: nodeID})

	// Start suspicion timer.
	if timer, exists := m.suspicionTimers[nodeID]; exists {
		timer.Stop()
	}

	m.suspicionTimers[nodeID] = time.AfterFunc(m.suspicionTimeout, func() {
		m.markDead(nodeID)
	})
}

// markDead transitions a node to DEAD status.
func (m *Membership) markDead(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.registry.Get(nodeID)
	if !ok {
		return
	}

	if info.GetStatus() == StatusDead {
		return
	}

	info.SetStatus(StatusDead)
	log.Printf("[swim] node %s marked as DEAD", nodeID)

	// Clean up suspicion timer.
	if timer, exists := m.suspicionTimers[nodeID]; exists {
		timer.Stop()
		delete(m.suspicionTimers, nodeID)
	}

	m.queueUpdateLocked(MembershipUpdate{
		NodeID:      nodeID,
		Status:      StatusDead,
		Incarnation: info.Incarnation,
	})

	m.emitEvent(MembershipEvent{Type: EventDead, NodeID: nodeID})
}

// applyUpdate processes a piggybacked membership update from another node.
func (m *Membership) applyUpdate(update MembershipUpdate) {
	if update.NodeID == m.selfID {
		// If someone thinks we're dead/suspect, refute it.
		if update.Status == StatusSuspect || update.Status == StatusDead {
			m.refute(update)
		}
		return
	}

	existing, ok := m.registry.Get(update.NodeID)
	if !ok {
		// New node — register it.
		if update.Status == StatusAlive {
			newNode := NewNodeInfo(update.NodeID, update.GRPCAddr, update.GossipAddr, update.HTTPAddr)
			newNode.Incarnation = update.Incarnation
			m.registry.Register(newNode)

			log.Printf("[swim] discovered new node %s at %s", update.NodeID, update.GRPCAddr)
			m.emitEvent(MembershipEvent{Type: EventJoin, NodeID: update.NodeID})
		}
		return
	}

	// Apply only if incarnation is newer.
	if update.Incarnation < existing.Incarnation {
		return // Stale update.
	}

	// Apply status change.
	switch update.Status {
	case StatusAlive:
		if existing.GetStatus() != StatusAlive {
			existing.SetStatus(StatusAlive)
			existing.Incarnation = update.Incarnation
			existing.Touch()

			// Cancel suspicion timer if exists.
			m.mu.Lock()
			if timer, ok := m.suspicionTimers[update.NodeID]; ok {
				timer.Stop()
				delete(m.suspicionTimers, update.NodeID)
			}
			m.mu.Unlock()

			log.Printf("[swim] node %s is ALIVE (via gossip)", update.NodeID)
			m.emitEvent(MembershipEvent{Type: EventAlive, NodeID: update.NodeID})
		}

	case StatusSuspect:
		if existing.GetStatus() == StatusAlive {
			m.mu.Lock()
			m.markSuspectLocked(update.NodeID, existing)
			m.mu.Unlock()
		}

	case StatusDead:
		if existing.GetStatus() != StatusDead {
			m.mu.Lock()
			existing.SetStatus(StatusDead)
			if timer, ok := m.suspicionTimers[update.NodeID]; ok {
				timer.Stop()
				delete(m.suspicionTimers, update.NodeID)
			}
			m.mu.Unlock()

			log.Printf("[swim] node %s is DEAD (via gossip)", update.NodeID)
			m.emitEvent(MembershipEvent{Type: EventDead, NodeID: update.NodeID})
		}

	case StatusLeft:
		existing.SetStatus(StatusLeft)
		log.Printf("[swim] node %s LEFT (via gossip)", update.NodeID)
		m.emitEvent(MembershipEvent{Type: EventLeft, NodeID: update.NodeID})
	}
}

// markSuspectLocked is the inner version that requires m.mu to be held.
func (m *Membership) markSuspectLocked(nodeID string, info *NodeInfo) {
	info.SetStatus(StatusSuspect)
	log.Printf("[swim] node %s marked as SUSPECT (via gossip)", nodeID)

	if timer, exists := m.suspicionTimers[nodeID]; exists {
		timer.Stop()
	}
	m.suspicionTimers[nodeID] = time.AfterFunc(m.suspicionTimeout, func() {
		m.markDead(nodeID)
	})

	m.emitEvent(MembershipEvent{Type: EventSuspect, NodeID: nodeID})
}

// refute responds to false death/suspect reports by incrementing our incarnation.
func (m *Membership) refute(update MembershipUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Increment incarnation to override the false report.
	if update.Incarnation >= m.incarnation {
		m.incarnation = update.Incarnation + 1
	}

	log.Printf("[swim] refuting %s report (incarnation now %d)",
		update.Status, m.incarnation)

	m.queueUpdateLocked(MembershipUpdate{
		NodeID:      m.selfID,
		GRPCAddr:    m.selfInfo.GRPCAddr,
		GossipAddr:  m.selfInfo.GossipAddr,
		HTTPAddr:    m.selfInfo.HTTPAddr,
		Status:      StatusAlive,
		Incarnation: m.incarnation,
	})
}

// --- Update queue ---

// queueUpdate adds a membership update to the dissemination queue.
func (m *Membership) queueUpdate(update MembershipUpdate) {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()
	m.updateQueue = append(m.updateQueue, update)
}

// queueUpdateLocked adds an update when m.mu is already held.
func (m *Membership) queueUpdateLocked(update MembershipUpdate) {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()
	m.updateQueue = append(m.updateQueue, update)
}

// drainUpdates returns and clears up to maxPiggybackUpdates from the queue.
func (m *Membership) drainUpdates() []MembershipUpdate {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()

	if len(m.updateQueue) == 0 {
		return nil
	}

	n := len(m.updateQueue)
	if n > maxPiggybackUpdates {
		n = maxPiggybackUpdates
	}

	updates := make([]MembershipUpdate, n)
	copy(updates, m.updateQueue[:n])
	m.updateQueue = m.updateQueue[n:]

	return updates
}

// emitEvent sends a membership event to the callback.
func (m *Membership) emitEvent(event MembershipEvent) {
	if m.onEvent != nil {
		go m.onEvent(event)
	}
}
