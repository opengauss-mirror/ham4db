/*
   Copyright 2014 Outbrain Inc.

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
	"regexp"
	"strings"

	"gitee.com/opengauss/ham4db/go/config"
)

// ClusterInfo makes for a cluster status/info summary
type ClusterInfo struct {
	DatabaseType                           string
	ClusterName                            string
	ClusterAlias                           string // Human friendly alias
	ClusterDomain                          string // CNAME/VIP/A-record/whatever of the master of this cluster
	CountInstances                         uint
	HeuristicLag                           int64
	HasAutomatedMasterRecovery             bool
	HasAutomatedIntermediateMasterRecovery bool
}

// isFilterMatchCluster will see whether the given filter match the given cluster details
func (ci *ClusterInfo) isFilterMatchCluster(filterList []string) bool {
	for _, filter := range filterList {

		// match all or name or alias
		if filter == "*" || filter == ci.ClusterAlias || filter == ci.ClusterName {
			return true
		}

		// match by exact cluster alias name
		if strings.HasPrefix(filter, "alias=") {
			if strings.SplitN(filter, "=", 2)[1] == ci.ClusterAlias {
				return true
			}
		}

		// match by cluster alias regex
		if strings.HasPrefix(filter, "alias~=") {
			if matched, _ := regexp.MatchString(strings.SplitN(filter, "~=", 2)[1], ci.ClusterAlias); matched {
				return true
			}
		}

		// match cluster name
		if matched, _ := regexp.MatchString(filter, ci.ClusterName); matched && filter != "" {
			return true
		}
	}
	return false
}

// ApplyClusterAlias updates the given clusterInfo's ClusterAlias property
func (ci *ClusterInfo) ApplyClusterAlias() {

	// abort if already has an alias
	if ci.ClusterAlias != "" && ci.ClusterAlias != ci.ClusterName {
		return
	}

	// get alias from config
	if alias := MappedClusterNameToAlias(ci.ClusterName); alias != "" {
		ci.ClusterAlias = alias
	}
}

// MappedClusterNameToAlias attempts to match a cluster with an alias based on configured ClusterNameToAlias map
func MappedClusterNameToAlias(clusterName string) string {
	for pattern, alias := range config.Config.ClusterNameToAlias {
		if pattern == "" {
			continue
		}
		if matched, _ := regexp.MatchString(pattern, clusterName); matched {
			return alias
		}
	}
	return ""
}

// ReadRecoveryInfo check if cluster info can match filter in config
func (ci *ClusterInfo) ReadRecoveryInfo() {
	ci.HasAutomatedMasterRecovery = ci.isFilterMatchCluster(config.Config.RecoverMasterClusterFilters)
	ci.HasAutomatedIntermediateMasterRecovery = ci.isFilterMatchCluster(config.Config.RecoverIntermediateMasterClusterFilters)
}
