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
package base

import (
	"fmt"
	"strings"
)

// figureClusterName is a convenience function to get a cluster name from hints
func FigureClusterNameByHint(hint string) (clusterName string, err error) {
	if hint == "" {
		return "", fmt.Errorf("Unable to determine cluster name by empty hint")
	}
	vals := strings.Split(hint, ":")
	hostPort := strings.Join(vals[1:], ":")
	instanceKey, _ := ParseRawInstanceKey(vals[0], hostPort)
	return FigureClusterName(hostPort, instanceKey, nil)
}
