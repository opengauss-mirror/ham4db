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
	"gitee.com/opengauss/ham4db/go/common"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/rcrowley/go-metrics"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

var writeResolvedHostnameCounter = metrics.NewCounter()
var writeUnresolvedHostnameCounter = metrics.NewCounter()
var readResolvedHostnameCounter = metrics.NewCounter()
var readUnresolvedHostnameCounter = metrics.NewCounter()
var readAllResolvedHostnamesCounter = metrics.NewCounter()

var hostnameResolvesLightweightCacheInit = &sync.Mutex{}
var hostnameResolvesLightweightCacheLoadedOnceFromDB bool = false

func init() {
	metrics.Register("resolve.write_resolved", writeResolvedHostnameCounter)
	metrics.Register("resolve.write_unresolved", writeUnresolvedHostnameCounter)
	metrics.Register("resolve.read_resolved", readResolvedHostnameCounter)
	metrics.Register("resolve.read_unresolved", readUnresolvedHostnameCounter)
	metrics.Register("resolve.read_resolved_all", readAllResolvedHostnamesCounter)
}

// ResolveHostname Attempt to resolve a hostname. This may return a database cached hostname or otherwise it may resolve the hostname via CNAME
func ResolveHostname(hostname string) (string, error) {

	// check hostname
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return hostname, log.Errore(errors.New("will not resolve empty hostname"))
	}
	if strings.Contains(hostname, ",") {
		return hostname, log.Errore(fmt.Errorf("will not resolve multi-hostname: %+v", hostname))
	}

	// quietly abort. Nothing to do. The hostname is detached for a reason: it
	// will not be resolved, for sure.
	if (&dtstruct.InstanceKey{Hostname: hostname}).IsDetached() {
		return hostname, nil
	}

	// get from cache, if not exist, get from database async
	resolvedHostname, found := cache.GetHostname(hostname, func() (interface{}, error) {

		// resolve hostname
		resolved, err := resolveHostname(hostname)
		if err != nil {
			return nil, err
		}

		// reject, don't even cache
		if config.Config.RejectHostnameResolvePattern != "" {
			if matched, _ := regexp.MatchString(config.Config.RejectHostnameResolvePattern, resolved); matched {
				log.Warningf("resolve hostname: %+v resolved to %+v but rejected due to reject hostname resolve pattern '%+v'", hostname, resolved, config.Config.RejectHostnameResolvePattern)
				return hostname, nil
			}
		}

		// return resolved hostname
		return resolved, nil
	})

	// Good result! Cache it, also to DB
	if found {
		log.Debugf("Cache hostname resolve %s as %s", hostname, resolvedHostname)
		go UpdateResolvedHostname(hostname, resolvedHostname)
	}

	return resolvedHostname, nil
}

// ReadResolvedHostname return the resolved hostname given a hostname, or empty if not exists
func ReadResolvedHostname(hostname string) (resolvedHostname string, err error) {
	err = db.Query(`select resolved_hostname from ham_hostname_resolve where hostname = ?`,
		sqlutil.Args(hostname),
		func(m sqlutil.RowMap) error {
			resolvedHostname = m.GetString("resolved_hostname")
			return nil
		},
	)
	readResolvedHostnameCounter.Inc(1)
	return
}

// GetAddr resolve an IP or hostname into a normalized valid CNAME
func GetAddr(hostname string) (string, error) {

	// if hostname is ipv4, should get real hostname, return the first addr
	if common.Ipv4Regexp.MatchString(hostname) {
		addr, err := net.LookupAddr(hostname)
		if err == nil && addr != nil && len(addr) > 0 {
			return addr[0], log.Errore(err)
		}
		return hostname, nil
	}

	// get a valid cname
	reslove, err := GetCNAME(hostname)
	if reslove != "" && err == nil {
		return reslove, err
	}
	return hostname, nil
}

// GetCNAME resolve an IP or hostname into a normalized valid CNAME
func GetCNAME(hostname string) (string, error) {
	res, err := net.LookupCNAME(hostname)
	if err != nil {
		return hostname, log.Errore(err)
	}
	return strings.TrimRight(res, "."), nil
}

// resolveHostname find out how to resolve hostname and do resolve it
func resolveHostname(hostname string) (string, error) {
	switch strings.ToLower(config.Config.HostnameResolveMethod) {
	case "none":
		return hostname, nil
	case "default":
		return hostname, nil
	case "addr":
		return GetAddr(hostname)
	case "cname":
		return GetCNAME(hostname)
	case "ip":
		return getHostnameIP(hostname)
	}
	return hostname, nil
}

// getHostnameIP get ip for hostname from ip cache
func getHostnameIP(hostname string) (ipString string, err error) {

	// get ip for hostname from cache, if not exit, lookup it
	ipList, err := cache.GetIP(hostname, func() (interface{}, error) {
		return net.LookupIP(hostname)
	})
	if err != nil {
		return "", log.Errore(err)
	}

	// get ipv4 and ipv6
	ipv4String, ipv6String := extractIP(ipList)
	if ipv4String != "" {
		return ipv4String, nil
	}
	return ipv6String, nil
}

// extractIP get one v4 and one v6 from ip list
func extractIP(ipList []net.IP) (ipv4String string, ipv6String string) {
	for _, ip := range ipList {
		if ip4 := ip.To4(); ip4 != nil {
			ipv4String = ip.String()
		} else {
			ipv6String = ip.String()
		}
	}
	return ipv4String, ipv6String
}

// LoadHostnameResolveCache check hostname resolve method, if not none, load all hostname from backend database to hostname cache
func LoadHostnameResolveCache() error {
	if !IsHostnameResolveMethodNone() {
		return loadHostnameResolveCacheFromDatabase()
	}
	return nil
}

// IsHostnameResolveMethodNone check if hostname resolve method in config is none
func IsHostnameResolveMethodNone() bool {
	return strings.ToLower(config.Config.HostnameResolveMethod) == "none"
}

// loadHostnameResolveCacheFromDatabase
func loadHostnameResolveCacheFromDatabase() error {
	allHostnamesResolves, err := ReadAllHostnameResolves()
	if err != nil {
		return err
	}
	for _, hostnameResolve := range allHostnamesResolves {
		cache.SetHostname(hostnameResolve.Hostname, hostnameResolve.ResolvedHostname, constant.CacheExpireByCache)
	}
	hostnameResolvesLightweightCacheLoadedOnceFromDB = true
	return nil
}

// ReadAllHostnameResolves
func ReadAllHostnameResolves() ([]dtstruct.HostnameResolve, error) {
	var res []dtstruct.HostnameResolve
	query := `
		select
			hostname,
			resolved_hostname
		from
			ham_hostname_resolve
		`
	err := db.Query(query, nil, func(m sqlutil.RowMap) error {
		hostnameResolve := dtstruct.HostnameResolve{Hostname: m.GetString("hostname"), ResolvedHostname: m.GetString("resolved_hostname")}
		res = append(res, hostnameResolve)
		return nil
	})
	readAllResolvedHostnamesCounter.Inc(1)
	if err != nil {
		return nil, log.Errore(err)
	}
	return res, nil
}

////////////////////////////////////TODO

// ResolveUnknownMasterHostnameResolves fixes missing hostname resolves based on hostname_resolve_history
// The use case is replicas replicating from some unknown-hostname which cannot be otherwise found. This could
// happen due to an expire unresolve together with clearing up of hostname cache.
func ResolveUnknownMasterHostnameResolves() error {

	hostnameResolves, err := ReadUnknownMasterHostnameResolves()
	if err != nil {
		return err
	}
	for hostname, resolvedHostname := range hostnameResolves {
		UpdateResolvedHostname(hostname, resolvedHostname)
	}

	AuditOperation("resolve-unknown-masters", nil, "", fmt.Sprintf("Num resolved hostnames: %d", len(hostnameResolves)))
	return err
}

// WriteResolvedHostname stores a hostname and the resolved hostname to backend database
func WriteResolvedHostname(hostname string, resolvedHostname string) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
			insert into
					ham_hostname_resolve (hostname, resolved_hostname, resolved_timestamp)
				values
					(?, ?, NOW())
				on duplicate key update
					resolved_hostname = VALUES(resolved_hostname),
					resolved_timestamp = VALUES(resolved_timestamp)
			`,
			hostname,
			resolvedHostname)
		if err != nil {
			return log.Errore(err)
		}
		if hostname != resolvedHostname {
			// history is only interesting when there's actually something to resolve...
			_, err = db.ExecSQL(`
			insert into
					ham_hostname_resolve_history (hostname, resolved_hostname, resolved_timestamp)
				values
					(?, ?, NOW())
				on duplicate key update
					hostname=values(hostname),
					resolved_timestamp=values(resolved_timestamp)
			`,
				hostname,
				resolvedHostname)
		}
		writeResolvedHostnameCounter.Inc(1)
		return nil
	}
	return db.ExecDBWrite(writeFunc)
}

// ReadAllHostnameUnresolves returns the content of the ham_hostname_unresolved table
func ReadAllHostnameUnresolves() ([]dtstruct.HostnameUnresolve, error) {
	unres := []dtstruct.HostnameUnresolve{}
	query := `
		select
			hostname,
			unresolved_hostname
		from
			ham_hostname_unresolved
		`
	err := db.Query(query, nil, func(m sqlutil.RowMap) error {
		hostnameUnresolve := dtstruct.HostnameUnresolve{Hostname: m.GetString("hostname"), UnresolvedHostname: m.GetString("unresolved_hostname")}

		unres = append(unres, hostnameUnresolve)
		return nil
	})

	return unres, log.Errore(err)
}

// ReadAllHostnameUnresolves returns the content of the ham_hostname_unresolved table
func ReadAllHostnameUnresolvesRegistrations() (registrations []dtstruct.HostnameRegistration, err error) {
	unresolves, err := ReadAllHostnameUnresolves()
	if err != nil {
		return registrations, err
	}
	for _, unresolve := range unresolves {
		registration := dtstruct.NewHostnameRegistration(&dtstruct.InstanceKey{Hostname: unresolve.Hostname}, unresolve.UnresolvedHostname)
		registrations = append(registrations, *registration)
	}
	return registrations, nil
}

// readUnresolvedHostname reverse-reads hostname resolve. It returns a hostname which matches given pattern and resovles to resolvedHostname,
// or, in the event no such hostname is found, the given resolvedHostname, unchanged.
func ReadUnresolvedHostname(hostname string) (string, error) {
	unresolvedHostname := hostname

	query := `
	   		select
	   			unresolved_hostname
	   		from
	   			ham_hostname_unresolved
	   		where
	   			hostname = ?
	   		`

	err := db.Query(query, sqlutil.Args(hostname), func(m sqlutil.RowMap) error {
		unresolvedHostname = m.GetString("unresolved_hostname")
		return nil
	})
	readUnresolvedHostnameCounter.Inc(1)

	if err != nil {
		log.Errore(err)
	}
	return unresolvedHostname, err
}

// readMissingHostnamesToResolve gets those (unresolved, e.g. VIP) hostnames that *should* be present in
// the hostname_resolve table, but aren't.
func readMissingKeysToResolve() (result dtstruct.InstanceKeyMap, err error) {
	query := `
   		select
   				ham_hostname_unresolved.unresolved_hostname,
   				ham_database_instance.port
   			from
   				ham_database_instance
   				join ham_hostname_unresolved on (ham_database_instance.hostname = ham_hostname_unresolved.hostname)
   				left join ham_hostname_resolve on (ham_database_instance.hostname = ham_hostname_resolve.resolved_hostname)
   			where
   				ham_hostname_resolve.hostname is null
	   		`

	err = db.Query(query, nil, func(m sqlutil.RowMap) error {
		instanceKey := dtstruct.InstanceKey{Hostname: m.GetString("unresolved_hostname"), Port: m.GetInt("port")}
		result.AddKey(instanceKey)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return result, err
}

// WriteHostnameUnresolve upserts an entry in hostname_unresolve
func WriteHostnameUnresolve(instanceKey *dtstruct.InstanceKey, unresolvedHostname string) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
        	insert into ham_hostname_unresolved (
        		hostname,
        		unresolved_hostname,
        		last_register_timestamp)
        	values (?, ?, NOW())
        	on duplicate key update
        		unresolved_hostname=values(unresolved_hostname),
        		last_register_timestamp=now()
				`, instanceKey.Hostname, unresolvedHostname,
		)
		if err != nil {
			return log.Errore(err)
		}
		_, err = db.ExecSQL(`
        	replace into ham_hostname_unresolved_history (
        		hostname,
        		unresolved_hostname,
        		last_register_timestamp)
        	values (?, ?, NOW())
				`, instanceKey.Hostname, unresolvedHostname,
		)
		writeUnresolvedHostnameCounter.Inc(1)
		return nil
	}
	return db.ExecDBWrite(writeFunc)
}

// DeleteHostnameUnresolve removes an unresolve entry
func DeleteHostnameUnresolve(instanceKey *dtstruct.InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
      	delete from ham_hostname_unresolved
				where hostname=?
				`, instanceKey.Hostname,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// ExpireHostnameUnresolve expires ham_hostname_unresolved entries that haven't been updated recently.
func ExpireHostnameUnresolve() error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
      	delete from ham_hostname_unresolved
				where last_register_timestamp < NOW() - INTERVAL ? MINUTE
				`, config.Config.ExpiryHostnameResolvesMinutes,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// ForgetExpiredHostnameResolves
func ForgetExpiredHostnameResolves() error {
	_, err := db.ExecSQL(`
			delete
				from ham_hostname_resolve
			where
				resolved_timestamp < NOW() - interval ? minute`,
		2*config.Config.ExpiryHostnameResolvesMinutes,
	)
	return err
}

// DeleteInvalidHostnameResolves removes invalid resolves. At this time these are:
// - infinite loop resolves (A->B and B->A), remove earlier mapping
func DeleteInvalidHostnameResolves() error {
	var invalidHostnames []string

	query := `
		select
		    early.hostname
		  from
		    ham_hostname_resolve as latest
		    join ham_hostname_resolve early on (latest.resolved_hostname = early.hostname and latest.hostname = early.resolved_hostname)
		  where
		    latest.hostname != latest.resolved_hostname
		    and latest.resolved_timestamp > early.resolved_timestamp
	   	`

	err := db.Query(query, nil, func(m sqlutil.RowMap) error {
		invalidHostnames = append(invalidHostnames, m.GetString("hostname"))
		return nil
	})
	if err != nil {
		return err
	}

	for _, invalidHostname := range invalidHostnames {
		_, err = db.ExecSQL(`
			delete
				from ham_hostname_resolve
			where
				hostname = ?`,
			invalidHostname,
		)
		log.Errore(err)
	}
	return err
}

// deleteHostnameResolves compeltely erases the database cache
func DeleteHostnameResolves() error {
	_, err := db.ExecSQL(`
			delete
				from ham_hostname_resolve`,
	)
	return err
}

// writeHostnameIPs stroes an ipv4 and ipv6 associated witha hostname, if available
func WriteHostnameIPs(hostname string, ipv4String string, ipv6String string) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
			insert into
					ham_hostname_ip (hostname, ipv4, ipv6, last_updated_timestamp)
				values
					(?, ?, ?, NOW())
				on duplicate key update
					ipv4 = VALUES(ipv4),
					ipv6 = VALUES(ipv6),
					last_updated_timestamp = VALUES(last_updated_timestamp)
			`,
			hostname,
			ipv4String,
			ipv6String,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// readUnresolvedHostname reverse-reads hostname resolve. It returns a hostname which matches given pattern and resovles to resolvedHostname,
// or, in the event no such hostname is found, the given resolvedHostname, unchanged.
func ReadHostnameIPs(hostname string) (ipv4 string, ipv6 string, err error) {
	query := `
		select
			ipv4, ipv6
		from
			ham_hostname_ip
		where
			hostname = ?
	`
	err = db.Query(query, sqlutil.Args(hostname), func(m sqlutil.RowMap) error {
		ipv4 = m.GetString("ipv4")
		ipv6 = m.GetString("ipv6")
		return nil
	})
	return ipv4, ipv6, log.Errore(err)
}

// UpdateResolvedHostname will store the given resolved hostname in cache
// Returns false when the key already existed with same resolved value (similar
// to AFFECTED_ROWS() in mysql)
func UpdateResolvedHostname(hostname string, resolvedHostname string) bool {
	if resolvedHostname == "" {
		return false
	}
	if existingResolvedHostname, found := cache.GetHostname(hostname, nil); found && (existingResolvedHostname == resolvedHostname) {
		return false
	}
	cache.SetHostname(hostname, resolvedHostname, constant.CacheExpireByCache)
	if !IsHostnameResolveMethodNone() {
		WriteResolvedHostname(hostname, resolvedHostname)
	}
	return true
}

func FlushNontrivialResolveCacheToDatabase() error {
	if IsHostnameResolveMethodNone() {
		return nil
	}
	items := cache.ItemHostname()
	for hostname := range items {
		resolvedHostname, found := cache.GetHostname(hostname, nil)
		if found && (resolvedHostname != hostname) {
			WriteResolvedHostname(hostname, resolvedHostname)
		}
	}
	return nil
}

func ResetHostnameResolveCache() error {
	err := DeleteHostnameResolves()
	cache.FlushHostname()
	hostnameResolvesLightweightCacheLoadedOnceFromDB = false
	return err
}

func UnresolveHostname(instanceKey *dtstruct.InstanceKey, f func(*dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error)) (dtstruct.InstanceKey, bool, error) {
	if *dtstruct.RuntimeCLIFlags.SkipUnresolved {
		return *instanceKey, false, nil
	}
	unresolvedHostname, err := ReadUnresolvedHostname(instanceKey.Hostname)
	if err != nil {
		return *instanceKey, false, log.Errore(err)
	}
	if unresolvedHostname == instanceKey.Hostname {
		// unchanged. Nothing to do
		return *instanceKey, false, nil
	}
	// We unresovled to a different hostname. We will now re-resolve to double-check!
	unresolvedKey := &dtstruct.InstanceKey{Hostname: unresolvedHostname, Port: instanceKey.Port}

	instance, err := f(unresolvedKey)
	if err != nil {
		return *instanceKey, false, log.Errore(err)
	}
	if instance.IsReplicaServer() && config.Config.SkipBinlogServerUnresolveCheck {
		// Do nothing. Everything is assumed to be fine.
	} else if instance.GetInstance().Key.Hostname != instanceKey.Hostname {
		// Resolve(Unresolve(hostname)) != hostname ==> Bad; reject
		if *dtstruct.RuntimeCLIFlags.SkipUnresolvedCheck {
			return *instanceKey, false, nil
		}
		return *instanceKey, false, log.Errorf("Error unresolving; hostname=%s, unresolved=%s, re-resolved=%s; mismatch. Skip/ignore with --skip-unresolve-check", instanceKey.Hostname, unresolvedKey.Hostname, instance.GetInstance().Key.Hostname)
	}
	return *unresolvedKey, true, nil
}

func RegisterHostnameUnresolve(registration *dtstruct.HostnameRegistration) (err error) {
	if registration.Hostname == "" {
		return DeleteHostnameUnresolve(&registration.Key)
	}
	if registration.CreatedAt.Add(time.Duration(config.Config.ExpiryHostnameResolvesMinutes) * time.Minute).Before(time.Now()) {
		// already expired.
		return nil
	}
	return WriteHostnameUnresolve(&registration.Key, registration.Hostname)
}

func ResolveHostnameIPs(hostname string) error {
	ips, err := cache.GetIP(hostname, func() (interface{}, error) {
		return net.LookupIP(hostname)
	})
	if err != nil {
		return err
	}
	ipv4String, ipv6String := extractIP(ips)
	return WriteHostnameIPs(hostname, ipv4String, ipv6String)
}

// ForgetUnseenInstancesDifferentlyResolved will purge instances which are invalid, and whose hostname
// appears on the hostname_resolved table; this means some time in the past their hostname was unresovled, and now
// resovled to a different value; the old hostname is never accessed anymore and the old entry should be removed.
func ForgetUnseenInstancesDifferentlyResolved() error {
	query := `
			select
				ham_database_instance.hostname, ham_database_instance.port
			from
					ham_hostname_resolve
					JOIN ham_database_instance ON (ham_hostname_resolve.hostname = ham_database_instance.hostname)
			where
					ham_hostname_resolve.hostname != ham_hostname_resolve.resolved_hostname
					AND ifnull(last_checked_timestamp <= last_seen_timestamp, 0) = 0
	`
	keys := dtstruct.NewInstanceKeyMap()
	err := db.Query(query, nil, func(m sqlutil.RowMap) error {
		key := dtstruct.InstanceKey{
			Hostname: m.GetString("hostname"),
			Port:     m.GetInt("port"),
		}
		keys.AddKey(key)
		return nil
	})
	var rowsAffected int64 = 0
	for _, key := range keys.GetInstanceKeys() {
		sqlResult, err := db.ExecSQL(`
			delete from
				ham_database_instance
			where
		    hostname = ? and port = ?
			`, key.Hostname, key.Port,
		)
		if err != nil {
			return log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return log.Errore(err)
		}
		rowsAffected = rowsAffected + rows
	}
	AuditOperation("forget-unseen-differently-resolved", nil, "", fmt.Sprintf("Forgotten instances: %d", rowsAffected))
	return err
}
