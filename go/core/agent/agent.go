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
package agent

import (
	"context"
	oconstant "gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"google.golang.org/grpc"
	"math"
	"strconv"
	"time"
)

// HealthCheck update or insert new agent to backend db and return the agent record in backend db
func HealthCheck(ip, hostname, port, interval string) error {

	// check ip/hostname/port
	if util.CheckIP(ip) != nil || hostname == "" {
		return log.Errorf("illegal ip or hostname, ip:%s, hostname:%s", ip, hostname)
	}
	if err := util.CheckPort(port); err != nil {
		return log.Errorf("illegal port, should be number and between 1000 and 65535, port:%s", port)
	}
	if itv, err := strconv.Atoi(interval); itv <= 0 || err != nil {
		return log.Errorf("illegal interval, should be number and greater than 0, interval:%s", interval)
	}
	return db.ExecDBWrite(
		func() error {
			_, err := db.ExecSQL(`
			insert into
				ham_agent (agt_ip, agt_hostname, agt_port, check_interval, last_updated_timestamp)
			values
				(?, ?, ?, ?, current_timestamp)
			on duplicate key update
				last_updated_timestamp=current_timestamp,
				check_interval=values(check_interval)
			`,
				ip, hostname, port, interval,
			)
			return err
		},
	)
}

// GetLatestAgent get latest agent with specified addr from backend db
func GetLatestAgent(addr string) (*dtstruct.Agent, error) {
	return cache.GetAgent(addr, func() (interface{}, error) {
		var agentList []*dtstruct.Agent
		if err := db.Query(`
			select 
				agt_ip, agt_port, agt_hostname, check_interval, last_updated_timestamp 
			from 
				ham_agent 
			where 
				agt_ip = ? or agt_hostname = ?
			order by 
				last_updated_timestamp desc
		`, sqlutil.Args(addr, addr), func(rowMap sqlutil.RowMap) (err error) {
			var lut time.Time
			if lut, err = time.Parse(constant.DateFormat, rowMap.GetString("last_updated_timestamp")); err != nil {
				return
			}
			agentList = append(agentList,
				&dtstruct.Agent{
					IP:         rowMap.GetString("agt_ip"),
					Port:       rowMap.GetInt("agt_port"),
					Hostname:   rowMap.GetString("agt_hostname"),
					Interval:   rowMap.GetInt("check_interval"),
					LastUpdate: lut,
				},
			)
			return
		}); err != nil {
			return nil, err
		}

		// if there's no agent exist, just insert a new one
		if agentList == nil || len(agentList) == 0 {
			agent := &dtstruct.Agent{IP: addr, Hostname: addr, Port: oconstant.DefaultAgentPort, LastUpdate: time.Now(), Interval: math.MaxInt32}
			if _, err := db.ExecSQL(
				"insert into ham_agent(agt_ip, agt_port, agt_hostname, check_interval, last_updated_timestamp) values(?,?,?,?,current_timestamp)",
				agent.IP,
				agent.Port,
				agent.Hostname,
				agent.Interval,
			); err != nil {
				return nil, err
			}
			agentList = append(agentList, agent)
		}

		// double check
		if len(agentList) != 1 {
			log.Warning("should have only one agent for: %s, but get: %+v", addr, agentList)
		}

		return agentList[0], nil
	})
}

// ConnectToAgent create connect to instance's agent
// TODO record metric for this
func ConnectToAgent(instanceKey *dtstruct.InstanceKey) (*grpc.ClientConn, error) {

	// get agent for this instance
	agt, err := GetLatestAgent(instanceKey.Hostname)
	if agt == nil || err != nil {
		return nil, err
	}

	// get conn from cache, create if not exist
	conn, err := cache.GetGRPC(context.TODO(), agt.GetAddr(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, log.Errore(err)
	}
	return conn, nil
}
