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

package base

import (
	"errors"
	"fmt"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/dtstruct"

	"io/ioutil"
	"net/http"
	"strings"

	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

// SetGeneralAttribute sets an attribute not associated with a host. Its a key-value thing
func SetGeneralAttribute(attributeName string, attributeValue string) error {
	if attributeName == "" {
		return nil
	}
	return SetHostAttribute("*", attributeName, attributeValue)
}

// SetHostAttribute save the attribute name and value to database
func SetHostAttribute(hostname string, attributeName string, attributeValue string) (err error) {
	_, err = db.ExecSQL(`
			replace into ham_hostname_attribute (
				hostname, attribute_name, attribute_value, submit_timestamp, expire_timestamp
			) values (
				?, ?, ?, now(), null
			)
		`, hostname, attributeName, attributeValue,
	)
	return
}

//////////////////////////////TODO

// GetHeuristicClusterDomainInstanceAttribute attempts detecting the cluster domain
// for the given cluster, and return the instance key associated as writer with that domain
func GetHeuristicClusterDomainInstanceAttribute(dbt string, clusterName string) (instanceKey *dtstruct.InstanceKey, err error) {
	clusterInfo, err := ReadClusterInfo(dbt, clusterName)
	if err != nil {
		return nil, err
	}

	if clusterInfo.ClusterDomain == "" {
		return nil, fmt.Errorf("Cannot find domain name for cluster %+v", clusterName)
	}

	writerInstanceName, err := GetGeneralAttribute(clusterInfo.ClusterDomain)
	if err != nil {
		return nil, err
	}
	return ParseRawInstanceKey(clusterInfo.DatabaseType, writerInstanceName)
}
func readResponse(res *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.Status == "500" {
		return body, errors.New("Response Status 500")
	}

	return body, nil
}

func getHostAttributesByClause(whereClause string, args []interface{}) ([]dtstruct.HostAttributes, error) {
	res := []dtstruct.HostAttributes{}
	query := fmt.Sprintf(`
		select
			hostname,
			attribute_name,
			attribute_value,
			submit_timestamp ,
			ifnull(expire_timestamp, '') as expire_timestamp
		from
			ham_hostname_attribute
		%s
		order by
			hostname, attribute_name
		`, whereClause)

	err := db.Query(query, args, func(m sqlutil.RowMap) error {
		hostAttributes := dtstruct.HostAttributes{}
		hostAttributes.Hostname = m.GetString("hostname")
		hostAttributes.AttributeName = m.GetString("attribute_name")
		hostAttributes.AttributeValue = m.GetString("attribute_value")
		hostAttributes.SubmitTimestamp = m.GetString("submit_timestamp")
		hostAttributes.ExpireTimestamp = m.GetString("expire_timestamp")

		res = append(res, hostAttributes)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

// GetHostAttributesByMatch
func GetHostAttributesByMatch(hostnameMatch string, attributeNameMatch string, attributeValueMatch string) ([]dtstruct.HostAttributes, error) {
	terms := []string{}
	args := sqlutil.Args()
	if hostnameMatch != "" {
		terms = append(terms, ` hostname rlike ? `)
		args = append(args, hostnameMatch)
	}
	if attributeNameMatch != "" {
		terms = append(terms, ` attribute_name rlike ? `)
		args = append(args, attributeNameMatch)
	}
	if attributeValueMatch != "" {
		terms = append(terms, ` attribute_value rlike ? `)
		args = append(args, attributeValueMatch)
	}

	if len(terms) == 0 {
		return getHostAttributesByClause("", args)
	}
	whereCondition := fmt.Sprintf(" where %s ", strings.Join(terms, " and "))

	return getHostAttributesByClause(whereCondition, args)
}

// GetHostAttribute expects to return a single attribute for a given hostname/attribute-name combination
// or error on empty result
func GetHostAttribute(hostname string, attributeName string) (string, error) {
	whereClause := `where hostname=? and attribute_name=?`
	attributes, err := getHostAttributesByClause(whereClause, sqlutil.Args(hostname, attributeName))
	if err != nil {
		return "", err
	}
	if len(attributeName) == 0 {
		return "", log.Errorf("No attribute found for %+v, %+v", hostname, attributeName)
	}
	return attributes[0].AttributeValue, nil
}

// GetGeneralAttribute expects to return a single attribute value (not associated with a specific hostname)
func GetGeneralAttribute(attributeName string) (result string, err error) {
	return GetHostAttribute("*", attributeName)
}

// GetHostAttributesByAttribute
func GetHostAttributesByAttribute(attributeName string, valueMatch string) ([]dtstruct.HostAttributes, error) {
	if valueMatch == "" {
		valueMatch = ".?"
	}
	whereClause := ` where attribute_name = ? and attribute_value rlike ?`

	return getHostAttributesByClause(whereClause, sqlutil.Args(attributeName, valueMatch))
}
