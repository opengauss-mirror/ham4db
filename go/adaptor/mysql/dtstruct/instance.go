/*
	Copyright 2021 SANGFOR TECHNOLOGIES

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package dtstruct

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/math"
	"strconv"
	"strings"
)

func init() {
	dtstruct.InstanceAdaptorMap[constant.DBTMysql] = &MysqlInstance{}
}

type MysqlInstance struct {
	*dtstruct.Instance

	VersionComment      string
	CountMySQLSnapshots int

	ServerUUID                   string
	MasterUUID                   string
	AncestryUUID                 string
	Binlog_format                string
	BinlogRowImage               string
	LogBinEnabled                bool
	LogSlaveUpdatesEnabled       bool // for API backwards compatibility. Equals `LogReplicationUpdatesEnabled`
	LogReplicationUpdatesEnabled bool
	SelfBinlogCoordinates        dtstruct.LogCoordinates
	MasterExecutedGtidSet        string // Not exported

	//
	Slave_SQL_Running          bool // for API backwards compatibility. Equals `ReplicationSQLThreadRuning`
	ReplicationSQLThreadRuning bool
	Slave_IO_Running           bool // for API backwards compatibility. Equals `ReplicationIOThreadRuning`
	ReplicationIOThreadRuning  bool
	ReplicationSQLThreadState  ReplicationThreadState
	ReplicationIOThreadState   ReplicationThreadState

	GTIDMode              string
	SupportsOracleGTID    bool
	UsingOracleGTID       bool
	UsingMariaDBGTID      bool
	UsingPseudoGTID       bool
	ReadBinlogCoordinates dtstruct.LogCoordinates
	ExecBinlogCoordinates dtstruct.LogCoordinates
	IsDetached            bool
	RelaylogCoordinates   dtstruct.LogCoordinates
	LastSQLError          string
	LastIOError           string
	SecondsBehindMaster   sql.NullInt64
	SQLDelay              uint
	ExecutedGtidSet       string
	GtidPurged            string
	GtidErrant            string

	SemiSyncAvailable                 bool // when both semi sync plugins (master & replica) are loaded
	SemiSyncEnforced                  bool
	SemiSyncMasterEnabled             bool
	SemiSyncReplicaEnabled            bool
	SemiSyncMasterTimeout             uint64
	SemiSyncMasterWaitForReplicaCount uint
	SemiSyncMasterStatus              bool
	SemiSyncMasterClients             uint
	SemiSyncReplicaStatus             bool

	/* All things Group Replication below */

	// Group replication global variables
	ReplicationGroupName            string
	ReplicationGroupIsSinglePrimary bool

	// Replication group members information. See
	// https://dev.mysql.com/doc/refman/8.0/en/replication-group-members-table.html for details.
	ReplicationGroupMemberState string
	ReplicationGroupMemberRole  string

	// List of all known members of the same group
	ReplicationGroupMembers dtstruct.InstanceKeyMap

	// Primary of the replication group
	ReplicationGroupPrimaryInstanceKey dtstruct.InstanceKey
}

func (this *MysqlInstance) GetDatabaseType() string {
	return this.Instance.Key.DBType
}

func (this *MysqlInstance) GetHandler() dtstruct.InstanceAdaptor {
	return this
}

func (this *MysqlInstance) SetInstance(instance *dtstruct.Instance) {
	this.Instance = instance
}

func (this *MysqlInstance) Less(handler dtstruct.InstanceAdaptor, dataCenter string) bool {
	other := handler.(*MysqlInstance)
	if this.ExecBinlogCoordinates.Equals(&other.ExecBinlogCoordinates) {
		// Secondary sorting: "smaller" if not logging replica updates
		if other.LogReplicationUpdatesEnabled && !this.LogReplicationUpdatesEnabled {
			return true
		}
		// Next sorting: "smaller" if of higher version (this will be reversed eventually)
		// Idea is that given 5.6 a& 5.7 both of the exact position, we will want to promote
		// the 5.6 on top of 5.7, as the other way around is invalid
		if other.Instance.IsSmallerMajorVersion(this.Instance) {
			return true
		}
		// Next sorting: "smaller" if of larger binlog-format (this will be reversed eventually)
		// Idea is that given ROW & STATEMENT both of the exact position, we will want to promote
		// the STATEMENT on top of ROW, as the other way around is invalid
		if IsSmallerBinlogFormat(other.Binlog_format, this.Binlog_format) {
			return true
		}
		// Prefer local datacenter:
		if other.DataCenter == dataCenter && this.DataCenter != dataCenter {
			return true
		}
		// Prefer if not having errant GTID
		if other.GtidErrant == "" && this.GtidErrant != "" {
			return true
		}
		// Prefer candidates:
		if other.PromotionRule.BetterThan(this.PromotionRule) {
			return true
		}
	}

	return this.ExecBinlogCoordinates.SmallerThan(&other.ExecBinlogCoordinates)
}

func (this *MysqlInstance) SetHstPrtAndClusterName(hostname string, port int, upstreamHostname string, upstreamPort int, clusterName string) {
	panic("implement me")
}

func (this *MysqlInstance) SetPromotionRule(promotionRule dtstruct.CandidatePromotionRule) {
	this.PromotionRule = promotionRule
}

// IsReplicationGroup checks whether the host thinks it is part of a known replication group. Notice that this might
// return True even if the group has decided to expel the member represented by this instance, as the instance might not
// know that under certain circumstances
func (this *MysqlInstance) IsReplicationGroupMember() bool {
	return this.ReplicationGroupName != ""
}

func (this *MysqlInstance) IsReplicationGroupPrimary() bool {
	return this.IsReplicationGroupMember() && this.ReplicationGroupPrimaryInstanceKey.Equals(&this.Key)
}

func (this *MysqlInstance) IsReplicationGroupSecondary() bool {
	return this.IsReplicationGroupMember() && !this.ReplicationGroupPrimaryInstanceKey.Equals(&this.Key)
}

// ReplicaRunning returns true when this instance's status is of a replicating replica.
func (this *MysqlInstance) ReplicaRunning() bool {
	return this.IsReplica() && this.ReplicationSQLThreadState.IsRunning() && this.ReplicationIOThreadState.IsRunning()
}

// NoReplicationThreadRunning returns true when neither SQL nor IO threads are running (including the case where isn't even a replica)
func (this *MysqlInstance) ReplicationThreadsStopped() bool {
	return this.ReplicationSQLThreadState.IsStopped() && this.ReplicationIOThreadState.IsStopped()
}

// NoReplicationThreadRunning returns true when neither SQL nor IO threads are running (including the case where isn't even a replica)
func (this *MysqlInstance) ReplicationThreadsExist() bool {
	return this.ReplicationSQLThreadState.Exists() && this.ReplicationIOThreadState.Exists()
}

// SQLThreadUpToDate returns true when the instance had consumed all relay logs.
func (this *MysqlInstance) SQLThreadUpToDate() bool {
	return this.ReadBinlogCoordinates.Equals(&this.ExecBinlogCoordinates)
}

// UsingGTID returns true when this replica is currently replicating via GTID (either Oracle or MariaDB)
func (this *MysqlInstance) UsingGTID() bool {
	return this.UsingOracleGTID || this.UsingMariaDBGTID
}

func (this *MysqlInstance) IsMySQL51() bool {
	return this.MajorVersionString() == "5.1"
}

func (this *MysqlInstance) IsMySQL55() bool {
	return this.MajorVersionString() == "5.5"
}

func (this *MysqlInstance) IsMySQL56() bool {
	return this.MajorVersionString() == "5.6"
}

func (this *MysqlInstance) IsMySQL57() bool {
	return this.MajorVersionString() == "5.7"
}

func (this *MysqlInstance) IsMySQL80() bool {
	return this.MajorVersionString() == "8.0"
}

func (this *MysqlInstance) MarshalJSON() ([]byte, error) {
	i := struct {
		MysqlInstance
	}{}
	i.MysqlInstance = *this
	// change terminology. Users of the ham4db API can switch to new terminology and avoid using old terminology
	// flip
	i.SlaveLagSeconds = this.ReplicationLagSeconds
	i.LogSlaveUpdatesEnabled = this.LogReplicationUpdatesEnabled
	i.Slave_SQL_Running = this.ReplicationSQLThreadRuning
	i.Slave_IO_Running = this.ReplicationIOThreadRuning

	return json.Marshal(i)
}

// Equals tests that this instance is the same instance as other. The function does not test
// configuration or status.
func (this *MysqlInstance) Equals(other *dtstruct.Instance) bool {
	return this.Key == other.Key
}

// IsMariaDB checks whether this is any version of MariaDB
func (this *MysqlInstance) IsMariaDB() bool {
	return strings.Contains(this.Version, "MariaDB")
}

// IsPercona checks whether this is any version of Percona Server
func (this *MysqlInstance) IsPercona() bool {
	return strings.Contains(this.VersionComment, "Percona")
}

// isMaxScale checks whether this is any version of MaxScale
func (this *MysqlInstance) IsMaxScale() bool {
	return strings.Contains(this.Version, "maxscale")
}

// isNDB check whether this is NDB Cluster (aka MySQL Cluster)
func (this *MysqlInstance) IsNDB() bool {
	return strings.Contains(this.Version, "-ndb-")
}

// IsReplicaServer checks whether this is any type of a binlog server (currently only maxscale)
func (this *MysqlInstance) IsReplicaServer() bool {
	if this.IsMaxScale() {
		return true
	}
	return false
}

// IsOracleMySQL checks whether this is an Oracle MySQL distribution
func (this *MysqlInstance) IsOracleMySQL() bool {
	if this.IsMariaDB() {
		return false
	}
	if this.IsPercona() {
		return false
	}
	if this.IsMaxScale() {
		return false
	}
	if this.IsReplicaServer() {
		return false
	}
	return true
}

// applyFlavorName
func (this *MysqlInstance) SetFlavorName() {
	if this == nil {
		return
	}
	if this.IsOracleMySQL() {
		this.FlavorName = "MySQL"
	} else if this.IsMariaDB() {
		this.FlavorName = "MariaDB"
	} else if this.IsPercona() {
		this.FlavorName = "Percona"
	} else if this.IsMaxScale() {
		this.FlavorName = "MaxScale"
	} else {
		this.FlavorName = "unknown"
	}
}

// FlavorNameAndMajorVersion returns a string of the combined
// flavor and major version which is useful in some checks.
func (this *MysqlInstance) FlavorNameAndMajorVersion() string {
	if this.FlavorName == "" {
		this.SetFlavorName()
	}

	return this.FlavorName + "-" + this.MajorVersionString()
}

// IsReplica makes simple heuristics to decide whether this instance is a replica of another instance
func (this *MysqlInstance) IsReplica() bool {
	return this.UpstreamKey.Hostname != "" && this.UpstreamKey.Hostname != "_" && this.UpstreamKey.Port != 0 && (this.ReadBinlogCoordinates.LogFile != "" || this.UsingGTID())
}

// NextGTID returns the next (Oracle) GTID to be executed. Useful for skipping queries
func (this *MysqlInstance) NextGTID() (string, error) {
	if this.ExecutedGtidSet == "" {
		return "", fmt.Errorf("No value found in Executed_Gtid_Set; cannot compute NextGTID")
	}

	firstToken := func(s string, delimiter string) string {
		tokens := strings.Split(s, delimiter)
		return tokens[0]
	}
	lastToken := func(s string, delimiter string) string {
		tokens := strings.Split(s, delimiter)
		return tokens[len(tokens)-1]
	}
	// executed GTID set: 4f6d62ed-df65-11e3-b395-60672090eb04:1,b9b4712a-df64-11e3-b391-60672090eb04:1-6
	executedGTIDsFromMaster := lastToken(this.ExecutedGtidSet, ",")
	// executedGTIDsFromMaster: b9b4712a-df64-11e3-b391-60672090eb04:1-6
	executedRange := lastToken(executedGTIDsFromMaster, ":")
	// executedRange: 1-6
	lastExecutedNumberToken := lastToken(executedRange, "-")
	// lastExecutedNumber: 6
	lastExecutedNumber, err := strconv.Atoi(lastExecutedNumberToken)
	if err != nil {
		return "", err
	}
	nextNumber := lastExecutedNumber + 1
	nextGTID := fmt.Sprintf("%s:%d", firstToken(executedGTIDsFromMaster, ":"), nextNumber)
	return nextGTID, nil
}

// AddGroupMemberKey adds a group member to the list of this instance's group members.
func (this *MysqlInstance) AddGroupMemberKey(groupMemberKey *dtstruct.InstanceKey) {
	this.ReplicationGroupMembers.AddKey(*groupMemberKey)
}

// GetNextBinaryLog returns the successive, if any, binary log file to the one given
func (this *MysqlInstance) GetNextBinaryLog(binlogCoordinates dtstruct.LogCoordinates) (dtstruct.LogCoordinates, error) {
	if binlogCoordinates.LogFile == this.SelfBinlogCoordinates.LogFile {
		return binlogCoordinates, fmt.Errorf("Cannot find next binary log for %+v", binlogCoordinates)
	}
	return binlogCoordinates.NextFileCoordinates()
}

// IsReplicaOf returns true if this instance claims to replicate from given master
func (this *MysqlInstance) IsReplicaOf(master *MysqlInstance) bool {
	return this.UpstreamKey.Equals(&master.Key)
}

// IsReplicaOf returns true if this i supposed master of given replica
func (this *MysqlInstance) IsMasterOf(replica *MysqlInstance) bool {
	return replica.IsReplicaOf(this)
}

// IsDescendantOf returns true if this is replication directly or indirectly from other
func (this *MysqlInstance) IsDescendantOf(other *MysqlInstance) bool {
	for _, uuid := range strings.Split(this.AncestryUUID, ",") {
		if uuid == other.InstanceId && uuid != "" {
			return true
		}
	}
	return false
}

// HasReasonableMaintenanceReplicationLag returns true when the replica lag is reasonable, and maintenance operations should have a green light to go.
func (this *MysqlInstance) HasReasonableMaintenanceReplicationLag() bool {
	// replicas with SQLDelay are a special case
	if this.SQLDelay > 0 {
		return math.AbsInt64(this.SecondsBehindMaster.Int64-int64(this.SQLDelay)) <= int64(config.Config.ReasonableMaintenanceReplicationLagSeconds)
	}
	return this.SecondsBehindMaster.Int64 <= int64(config.Config.ReasonableMaintenanceReplicationLagSeconds)
}

// CanMove returns true if this instance's state allows it to be repositioned. For example,
// if this instance lags too much, it will not be moveable.
func (this *MysqlInstance) CanMove() (bool, error) {
	if !this.IsLastCheckValid {
		return false, fmt.Errorf("%+v: last check invalid", this.Key)
	}
	if !this.IsRecentlyChecked {
		return false, fmt.Errorf("%+v: not recently checked", this.Key)
	}
	if !this.ReplicationSQLThreadState.IsRunning() {
		return false, fmt.Errorf("%+v: instance is not replicating", this.Key)
	}
	if !this.ReplicationIOThreadState.IsRunning() {
		return false, fmt.Errorf("%+v: instance is not replicating", this.Key)
	}
	if !this.SecondsBehindMaster.Valid {
		return false, fmt.Errorf("%+v: cannot determine replication lag", this.Key)
	}
	if !this.HasReasonableMaintenanceReplicationLag() {
		return false, fmt.Errorf("%+v: lags too much", this.Key)
	}
	return true, nil
}

// CanMoveAsCoMaster returns true if this instance's state allows it to be repositioned.
func (this *MysqlInstance) CanMoveAsCoMaster() (bool, error) {
	if !this.IsLastCheckValid {
		return false, fmt.Errorf("%+v: last check invalid", this.Key)
	}
	if !this.IsRecentlyChecked {
		return false, fmt.Errorf("%+v: not recently checked", this.Key)
	}
	return true, nil
}

// CanMoveViaMatch returns true if this instance's state allows it to be repositioned via pseudo-GTID matching
func (this *MysqlInstance) CanMoveViaMatch() (bool, error) {
	if !this.IsLastCheckValid {
		return false, fmt.Errorf("%+v: last check invalid", this.Key)
	}
	if !this.IsRecentlyChecked {
		return false, fmt.Errorf("%+v: not recently checked", this.Key)
	}
	return true, nil
}

// StatusString returns a human readable description of this instance's status
func (this *MysqlInstance) StatusString() string {
	if !this.IsLastCheckValid {
		return "invalid"
	}
	if !this.IsRecentlyChecked {
		return "unchecked"
	}
	if this.IsReplica() && !this.ReplicaRunning() {
		return "nonreplicating"
	}
	if this.IsReplica() && !this.HasReasonableMaintenanceReplicationLag() {
		return "lag"
	}
	return "ok"
}

// LagStatusString returns a human readable representation of current lag
func (this *MysqlInstance) LagStatusString() string {
	if this.IsDetached {
		return "detached"
	}
	if !this.IsLastCheckValid {
		return "unknown"
	}
	if !this.IsRecentlyChecked {
		return "unknown"
	}
	if this.IsReplica() && !this.ReplicaRunning() {
		return "null"
	}
	if this.IsReplica() && !this.SecondsBehindMaster.Valid {
		return "null"
	}
	if this.IsReplica() && this.ReplicationLagSeconds.Int64 > int64(config.Config.ReasonableMaintenanceReplicationLagSeconds) {
		return fmt.Sprintf("%+vs", this.ReplicationLagSeconds.Int64)
	}
	return fmt.Sprintf("%+vs", this.ReplicationLagSeconds.Int64)
}

func (this *MysqlInstance) descriptionTokens() (tokens []string) {
	tokens = append(tokens, this.LagStatusString())
	tokens = append(tokens, this.StatusString())
	tokens = append(tokens, this.Version)
	if this.ReadOnly {
		tokens = append(tokens, "ro")
	} else {
		tokens = append(tokens, "rw")
	}
	if this.LogBinEnabled {
		tokens = append(tokens, this.Binlog_format)
	} else {
		tokens = append(tokens, "nobinlog")
	}
	{
		extraTokens := []string{}
		if this.LogBinEnabled && this.LogReplicationUpdatesEnabled {
			extraTokens = append(extraTokens, ">>")
		}
		if this.UsingGTID() || this.SupportsOracleGTID {
			token := "GTID"
			if this.GtidErrant != "" {
				token = fmt.Sprintf("%s:errant", token)
			}
			extraTokens = append(extraTokens, token)
		}
		if this.UsingPseudoGTID {
			extraTokens = append(extraTokens, "P-GTID")
		}
		if this.SemiSyncMasterStatus {
			extraTokens = append(extraTokens, "semi:master")
		}
		if this.SemiSyncReplicaStatus {
			extraTokens = append(extraTokens, "semi:replica")
		}
		if this.IsDowntimed {
			extraTokens = append(extraTokens, "downtimed")
		}
		tokens = append(tokens, strings.Join(extraTokens, ","))
	}
	return tokens
}

// HumanReadableDescription returns a simple readable string describing the status, version,
// etc. properties of this instance
func (this *MysqlInstance) HumanReadableDescription() string {
	tokens := this.descriptionTokens()
	nonEmptyTokens := []string{}
	for _, token := range tokens {
		if token != "" {
			nonEmptyTokens = append(nonEmptyTokens, token)
		}
	}
	description := fmt.Sprintf("[%s]", strings.Join(nonEmptyTokens, ","))
	return description
}

// TabulatedDescription returns a simple tabulated string of various properties
func (this *MysqlInstance) TabulatedDescription(separator string) string {
	tokens := this.descriptionTokens()
	description := fmt.Sprintf("%s", strings.Join(tokens, separator))
	return description
}

// NewInstance creates a new, empty instance
func NewInstance() *MysqlInstance {
	mi := &MysqlInstance{
		ReplicationGroupMembers: make(map[dtstruct.InstanceKey]bool),
	}
	mi.Instance = &dtstruct.Instance{}
	mi.DownstreamKeyMap = make(map[dtstruct.InstanceKey]bool)
	mi.Problems = []string{}
	return mi
}

func (mi *MysqlInstance) GetHostname() string {
	return mi.Instance.Key.Hostname
}

func (mi *MysqlInstance) GetPort() int {
	return mi.Instance.Key.Port
}

// TODO double check
func (mi *MysqlInstance) GetInstance() *dtstruct.Instance {
	return mi.Instance
}

// IsSmallerBinlogFormat tests two binlog formats and sees if one is "smaller" than the other.
// "smaller" binlog format means you can replicate from the smaller to the larger.
func IsSmallerBinlogFormat(binlogFormat string, otherBinlogFormat string) bool {
	if binlogFormat == "STATEMENT" {
		return otherBinlogFormat == "ROW" || otherBinlogFormat == "MIXED"
	}
	if binlogFormat == "MIXED" {
		return otherBinlogFormat == "ROW"
	}
	return false
}

// InstancesByCountReplicas sorts instances by umber of replicas, descending
type InstancesByCountReplicas []*MysqlInstance

func (this InstancesByCountReplicas) Len() int      { return len(this) }
func (this InstancesByCountReplicas) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountReplicas) Less(i, j int) bool {
	return this[i].Less(this[j], "")
}
