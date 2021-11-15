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
package adaptor

import (
	mconstant "gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	mdstruct "gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	mhandler "gitee.com/opengauss/ham4db/go/adaptor/mysql/handler"
	oconstant "gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	odstruct "gitee.com/opengauss/ham4db/go/adaptor/opengauss/dtstruct"
	ohandler "gitee.com/opengauss/ham4db/go/adaptor/opengauss/handler"
	"gitee.com/opengauss/ham4db/go/dtstruct"
)

func init() {
	dtstruct.HamHandlerMap[mconstant.DBTMysql] = &mhandler.Mysql{}
	dtstruct.HamHandlerMap[oconstant.DBTOpenGauss] = &ohandler.OpenGauss{}
	dtstruct.InstanceAdaptorMap[mconstant.DBTMysql] = &mdstruct.MysqlInstance{}
	dtstruct.InstanceAdaptorMap[oconstant.DBTOpenGauss] = &odstruct.OpenGaussInstance{}
	dtstruct.DBTypeMap[mconstant.DBTMysql] = false
	dtstruct.DBTypeMap[oconstant.DBTOpenGauss] = false
	dtstruct.DBTypeDefaultPortMap[mconstant.DBTMysql] = 3306
	dtstruct.DBTypeDefaultPortMap[oconstant.DBTOpenGauss] = 15400
}
