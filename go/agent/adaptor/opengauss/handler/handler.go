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
package handler

import (
	"context"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/core"
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/handler/v2d0d1"
	adtstruct "gitee.com/opengauss/ham4db/go/agent/dtstruct"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"github.com/opentracing/opentracing-go"
	"time"
)

// OpenGaussServer implements OpengaussAgentServer interface
type OpenGaussServer struct {
	version  string
	hookTask []aodtstruct.HookFunction
}

// CollectInfo collect opengauss node info
func (ogs *OpenGaussServer) CollectInfo(ctx context.Context, req *aodtstruct.DatabaseInfoRequest) (*aodtstruct.DatabaseInfoResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Agent Collect Info")
	defer span.Finish()

	// process according to version
	version := core.GetVersion(nil, &ogs.version)
	switch version {
	case constant.Version2d0d1, constant.Version2d1d0:
		return v2d0d1.CollectInfo(req, nil)
	default:
		return nil, log.Errorf("version:%s not supported now", version)
	}
}

// ManageAction do action from request on opengauss
func (ogs *OpenGaussServer) ManageAction(ctx context.Context, req *aodtstruct.ManageActionRequest) (resp *aodtstruct.ManageActionResponse, err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Agent Exec Action")
	defer span.Finish()

	// process according to version
	version := core.GetVersion(nil, &ogs.version)
	switch version {
	case constant.Version2d0d1, constant.Version2d1d0:
		return v2d0d1.Action(req.ActionType, nil, nil)
	default:
		return nil, log.Errorf("version:%s not supported now", version)
	}
}

// UpdateSyncConfig update sync standby config
func (ogs *OpenGaussServer) UpdateSyncConfig(ctx context.Context, req *aodtstruct.UpdateSyncConfigRequest) (*aodtstruct.UpdateSyncConfigResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Agent Update Sync Info")
	defer span.Finish()

	// process according to version
	version := core.GetVersion(nil, &ogs.version)
	switch version {
	case constant.Version2d0d1, constant.Version2d1d0:
		return v2d0d1.UpdateSyncConfig(req.SyncConf, nil)
	default:
		return nil, log.Errorf("version:%s not supported now", version)
	}
}

// ReplicationConfirm confirm if replication between instance in request and local is normal
func (ogs *OpenGaussServer) ReplicationConfirm(ctx context.Context, req *aodtstruct.ReplicationConfirmRequest) (*aodtstruct.ReplicationConfirmResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Confirm Replication")
	defer span.Finish()

	// process according to version
	version := core.GetVersion(nil, &ogs.version)
	switch version {
	case constant.Version2d0d1, constant.Version2d1d0:
		return v2d0d1.ReplicationConfirm(req, nil)
	default:
		return nil, log.Errorf("version:%s not supported now", version)
	}
}

// RunTask refresh base info every 5 minutes
func (ogs *OpenGaussServer) RunTask(wp *dtstruct.WorkerPool, args *adtstruct.Args) {
	if err := wp.AsyncRun(constant.WorkerNameAgentRefreshBaseInfo, func(workerExit chan struct{}) {
		refreshTick := time.Tick(time.Duration(args.RefreshInterval) * time.Second)
		for {
			select {
			case <-refreshTick:
				if _, err := core.GetBaseInfo(ogs.hookTask, true); err != nil {
					log.Error("refresh basic info, error: %s", err)
				}
			case <-workerExit:
				return
			}
		}
	}); err != nil {
		log.Error("run worker:%s failed, error:%s", constant.WorkerNameAgentRefreshBaseInfo, err)
	}
}
