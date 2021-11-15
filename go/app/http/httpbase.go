/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package http

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/security/token"
	"gitee.com/opengauss/ham4db/go/util"
	"net/http"
	"strings"

	"github.com/martini-contrib/auth"

	"gitee.com/opengauss/ham4db/go/config"
)

func authenticateToken(publicToken string, resp http.ResponseWriter) error {
	secretToken, err := token.AcquireAccessToken(publicToken)
	if err != nil {
		return err
	}
	cookieValue := fmt.Sprintf("%s:%s", publicToken, secretToken)
	cookie := &http.Cookie{Name: "access-token", Value: cookieValue, Path: "/"}
	http.SetCookie(resp, cookie)
	return nil
}

// getUserId returns the authenticated user id, if available, depending on authertication method.
func getUserId(req *http.Request, user auth.User) string {
	if config.Config.ReadOnly {
		return ""
	}

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			return string(user)
		}
	case "multi":
		{
			return string(user)
		}
	case "proxy":
		{
			return base.GetProxyAuthUser(req)
		}
	case "token":
		{
			return ""
		}
	default:
		{
			return ""
		}
	}
}

// getClusterNameIfExists returns a cluster name by params hint, or an empty cluster name
// if no hint is given
func getClusterNameIfExists(params map[string]string) (clusterName string, err error) {
	if clusterHint := util.GetClusterHint(params); clusterHint == "" {
		return "", nil
	} else {
		return base.FigureClusterNameByHint(clusterHint)
	}
}
