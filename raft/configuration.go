package raft

import "fmt"

// ServerSuffrage 决定一个配置中的服务器是否得到投票。
type ServerSuffrage int

// Note: 不要对这些进行重新编号，因为这些数字会被写进日志。
const (
	// Voter 选民
	Voter ServerSuffrage = iota
	// Nonvoter 非选民，只用来接收日志
	Nonvoter
	// Staging 介于  Voter 与 Nonvoter 之间的一个状态
	// 是一个像非选民一样行事的服务器，但有一个例外：一旦中转服务器收到足够多的日志条目，足以赶上领导者的日志，领导者将调用成员变更，将中转服务器改为选民。
	Staging
)

func (s ServerSuffrage) String() string {
	switch s {
	case Voter:
		return "Voter"
	case Nonvoter:
		return "Nonvoter"
	case Staging:
		return "Staging"
	}
	return "ServerSuffrage"
}



type nopConfigurationStore struct{}

func (s nopConfigurationStore) StoreConfiguration(_ uint64, _ Configuration) {}

// ServerID 是一个唯一的字符串，用于识别服务器。
type ServerID string

// ServerAddress 一个可以通信的服务端点
type ServerAddress string

// Server 追踪配置中单个服务器的信息。
type Server struct {
	// Suffrage 决定了服务器是否获得投票。
	Suffrage ServerSuffrage
	// ID 是一个唯一的字符串，用于在任何时间识别该节点
	ID ServerID
	// Address 可以通信的服务地址
	Address ServerAddress
}

// Configuration 追踪集群中的哪些服务器，以及它们是否有投票权。这应该包括本地服务器，如果它是集群的成员。
// 这些服务器没有特定的顺序，但每个服务器应该只出现一次。这些条目在成员变化时被追加到日志中。
type Configuration struct {
	Servers []Server
}

// Clone makes a deep copy of a Configuration.
func (c *Configuration) Clone() (copy Configuration) {
	copy.Servers = append(copy.Servers, c.Servers...)
	return
}

// ConfigurationChangeCommand is the different ways to change the cluster
// configuration.
type ConfigurationChangeCommand uint8

const (
	// AddStaging makes a server Staging unless its Voter.
	AddStaging ConfigurationChangeCommand = iota
	// AddNonvoter makes a server Nonvoter unless its Staging or Voter.
	AddNonvoter
	// DemoteVoter makes a server Nonvoter unless its absent.
	DemoteVoter
	// RemoveServer removes a server entirely from the cluster membership.
	RemoveServer
	// Promote is created automatically by a leader; it turns a Staging server
	// into a Voter.
	Promote
)

func (c ConfigurationChangeCommand) String() string {
	switch c {
	case AddStaging:
		return "AddStaging"
	case AddNonvoter:
		return "AddNonvoter"
	case DemoteVoter:
		return "DemoteVoter"
	case RemoveServer:
		return "RemoveServer"
	case Promote:
		return "Promote"
	}
	return "ConfigurationChangeCommand"
}

// configurationChangeRequest describes a change that a leader would like to
// make to its current configuration. It's used only within a single server
// (never serialized into the log), as part of `configurationChangeFuture`.
type configurationChangeRequest struct {
	command       ConfigurationChangeCommand
	serverID      ServerID
	serverAddress ServerAddress // only present for AddStaging, AddNonvoter
	// prevIndex, if nonzero, is the index of the only configuration upon which
	// this change may be applied; if another configuration entry has been
	// added in the meantime, this request will fail.
	prevIndex uint64
}

// configurations is state tracked on every server about its Configurations.
// Note that, per Diego's dissertation, there can be at most one uncommitted
// configuration at a time (the next configuration may not be created until the
// prior one has been committed).
//
// One downside to storing just two configurations is that if you try to take a
// snapshot when your state machine hasn't yet applied the committedIndex, we
// have no record of the configuration that would logically fit into that
// snapshot. We disallow snapshots in that case now. An alternative approach,
// which LogCabin uses, is to track every configuration change in the
// log.
type configurations struct {
	// committed 最新的【日志、快照】配置 且已提交的
	committed Configuration
	// committedIndex 已经提交了的日志索引
	committedIndex uint64
	// latest 最新的【日志、快照】配置   不知道有没有提交
	latest Configuration
	// latestIndex 最新的日志索引、 不知道有没有提交
	latestIndex uint64
}

// Clone makes a deep copy of a configurations object.
func (c *configurations) Clone() (copy configurations) {
	copy.committed = c.committed.Clone()
	copy.committedIndex = c.committedIndex
	copy.latest = c.latest.Clone()
	copy.latestIndex = c.latestIndex
	return
}

// hasVote 如果'id'标识的服务器是所提供的配置中的一个选民，则返回true。
func hasVote(configuration Configuration, id ServerID) bool {
	for _, server := range configuration.Servers {
		if server.ID == id {
			return server.Suffrage == Voter
		}
	}
	return false
}

// checkConfiguration tests a cluster membership configuration for common
// errors.
func checkConfiguration(configuration Configuration) error {
	idSet := make(map[ServerID]bool)
	addressSet := make(map[ServerAddress]bool)
	var voters int
	for _, server := range configuration.Servers {
		if server.ID == "" {
			return fmt.Errorf("empty ID in configuration: %v", configuration)
		}
		if server.Address == "" {
			return fmt.Errorf("empty address in configuration: %v", server)
		}
		if idSet[server.ID] {
			return fmt.Errorf("found duplicate ID in configuration: %v", server.ID)
		}
		idSet[server.ID] = true
		if addressSet[server.Address] {
			return fmt.Errorf("found duplicate address in configuration: %v", server.Address)
		}
		addressSet[server.Address] = true
		if server.Suffrage == Voter {
			voters++
		}
	}
	if voters == 0 {
		return fmt.Errorf("need at least one voter in configuration: %v", configuration)
	}
	return nil
}

// nextConfiguration generates a new Configuration from the current one and a
// configuration change request. It's split from appendConfigurationEntry so
// that it can be unit tested easily.
func nextConfiguration(current Configuration, currentIndex uint64, change configurationChangeRequest) (Configuration, error) {
	if change.prevIndex > 0 && change.prevIndex != currentIndex {
		return Configuration{}, fmt.Errorf("configuration changed since %v (latest is %v)", change.prevIndex, currentIndex)
	}

	configuration := current.Clone()
	switch change.command {
	case AddStaging:
		// TODO: barf on new address?
		newServer := Server{
			// TODO: This should add the server as Staging, to be automatically
			// promoted to Voter later. However, the promotion to Voter is not yet
			// implemented, and doing so is not trivial with the way the leader loop
			// coordinates with the replication goroutines today. So, for now, the
			// server will have a vote right away, and the Promote case below is
			// unused.
			Suffrage: Voter,
			ID:       change.serverID,
			Address:  change.serverAddress,
		}
		found := false
		for i, server := range configuration.Servers {
			if server.ID == change.serverID {
				if server.Suffrage == Voter {
					configuration.Servers[i].Address = change.serverAddress
				} else {
					configuration.Servers[i] = newServer
				}
				found = true
				break
			}
		}
		if !found {
			configuration.Servers = append(configuration.Servers, newServer)
		}
	case AddNonvoter:
		newServer := Server{
			Suffrage: Nonvoter,
			ID:       change.serverID,
			Address:  change.serverAddress,
		}
		found := false
		for i, server := range configuration.Servers {
			if server.ID == change.serverID {
				if server.Suffrage != Nonvoter {
					configuration.Servers[i].Address = change.serverAddress
				} else {
					configuration.Servers[i] = newServer
				}
				found = true
				break
			}
		}
		if !found {
			configuration.Servers = append(configuration.Servers, newServer)
		}
	case DemoteVoter:
		for i, server := range configuration.Servers {
			if server.ID == change.serverID {
				configuration.Servers[i].Suffrage = Nonvoter
				break
			}
		}
	case RemoveServer:
		for i, server := range configuration.Servers {
			if server.ID == change.serverID {
				configuration.Servers = append(configuration.Servers[:i], configuration.Servers[i+1:]...)
				break
			}
		}
	case Promote:
		for i, server := range configuration.Servers {
			if server.ID == change.serverID && server.Suffrage == Staging {
				configuration.Servers[i].Suffrage = Voter
				break
			}
		}
	}

	// Make sure we didn't do something bad like remove the last voter
	if err := checkConfiguration(configuration); err != nil {
		return Configuration{}, err
	}

	return configuration, nil
}

// encodePeers is used to serialize a Configuration into the old peers format.
// This is here for backwards compatibility when operating with a mix of old
// servers and should be removed once we deprecate support for protocol version 1.
func encodePeers(configuration Configuration, trans Transport) []byte {
	// Gather up all the voters, other suffrage types are not supported by
	// this data format.
	var encPeers [][]byte
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			encPeers = append(encPeers, trans.EncodePeer(server.ID, server.Address))
		}
	}

	// Encode the entire array.
	buf, err := encodeMsgPack(encPeers)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}

	return buf.Bytes()
}

// decodePeers is used to deserialize an old list of peers into a Configuration.
// This is here for backwards compatibility with old log entries and snapshots;
// it should be removed eventually.
func decodePeers(buf []byte, trans Transport) (Configuration, error) {
	// Decode the buffer first.
	var encPeers [][]byte
	if err := decodeMsgPack(buf, &encPeers); err != nil {
		return Configuration{}, fmt.Errorf("failed to decode peers: %v", err)
	}

	// Deserialize each peer.
	var servers []Server
	for _, enc := range encPeers {
		p := trans.DecodePeer(enc)
		servers = append(servers, Server{
			Suffrage: Voter,
			ID:       ServerID(p),
			Address:  p,
		})
	}

	return Configuration{Servers: servers}, nil
}

// EncodeConfiguration serializes a Configuration using MsgPack, or panics on
// errors.
func EncodeConfiguration(configuration Configuration) []byte {
	buf, err := encodeMsgPack(configuration)
	if err != nil {
		panic(fmt.Errorf("failed to encode configuration: %v", err))
	}
	return buf.Bytes()
}

// DecodeConfiguration 使用MsgPack对配置进行反序列化，或在出现错误时panic。
func DecodeConfiguration(buf []byte) Configuration {
	var configuration Configuration
	if err := decodeMsgPack(buf, &configuration); err != nil {
		panic(fmt.Errorf("failed to decode configuration: %v", err))
	}
	return configuration
}
