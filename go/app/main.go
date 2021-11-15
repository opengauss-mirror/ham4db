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

package main

import (
	"flag"
	"fmt"
	_ "gitee.com/opengauss/ham4db/go/adaptor"
	"gitee.com/opengauss/ham4db/go/app/cli"
	"gitee.com/opengauss/ham4db/go/app/http"
	"gitee.com/opengauss/ham4db/go/core/base"
	_ "gitee.com/opengauss/ham4db/go/core/consensus/raft/snapshot"
	initdb "gitee.com/opengauss/ham4db/go/core/db/init"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"github.com/uber/jaeger-client-go"
	"os"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	jconfig "github.com/uber/jaeger-client-go/config"
)

var AppVersion, GitCommit string

// main is the application's entry point. It will either spawn a CLI or HTTP itnerfaces.
func main() {
	configFile := flag.String("config", "", "config file name")
	command := flag.String("c", "", "command, required. See full list of commands via 'ham4db -c help'")
	strict := flag.Bool("strict", false, "strict mode (more checks, slower)")
	databaseType := flag.String("type", "", "database type")
	clusterId := flag.String("clstid", "", "cluster id")
	section := flag.String("section", "", "command section")
	instance := flag.String("i", "", "instance, host_fqdn[:port] (e.g. db.company.com:3306, db.company.com)")
	sibling := flag.String("s", "", "sibling instance, host_fqdn[:port]")
	destination := flag.String("d", "", "destination instance, host_fqdn[:port] (synonym to -s)")
	owner := flag.String("owner", "", "operation owner")
	reason := flag.String("reason", "", "operation reason")
	duration := flag.String("duration", "", "maintenance duration (format: 59s, 59m, 23h, 6d, 4w)")
	pattern := flag.String("pattern", "", "regular expression pattern")
	clusterAlias := flag.String("alias", "", "cluster alias")
	pool := flag.String("pool", "", "Pool logical name (applies for pool-related commands)")
	hostnameFlag := flag.String("hostname", "", "Hostname/fqdn/CNAME/VIP (applies for hostname/resolve related commands)")
	discovery := flag.Bool("discovery", true, "auto discovery mode")
	quiet := flag.Bool("quiet", false, "quiet")
	warning := flag.Bool("warn", false, "warn")
	verbose := flag.Bool("verbose", false, "verbose")
	debug := flag.Bool("debug", false, "debug mode (very verbose)")
	stack := flag.Bool("stack", false, "add stack trace upon error")
	dtstruct.RuntimeCLIFlags.SkipBinlogSearch = flag.Bool("skip-binlog-search", false, "when matching via Pseudo-GTID, only use relay logs. This can save the hassle of searching for a non-existend pseudo-GTID entry, for example in servers with replication filters.")
	dtstruct.RuntimeCLIFlags.SkipUnresolved = flag.Bool("skip-unresolve", false, "Do not unresolve a host name")
	dtstruct.RuntimeCLIFlags.SkipUnresolvedCheck = flag.Bool("skip-unresolve-check", false, "Skip/ignore checking an unresolve mapping (via ham_hostname_unresolved table) resolves back to same hostname")
	dtstruct.RuntimeCLIFlags.Noop = flag.Bool("noop", false, "Dry run; do not perform destructing operations")
	dtstruct.RuntimeCLIFlags.BinlogFile = flag.String("binlog", "", "Binary log file name")
	dtstruct.RuntimeCLIFlags.Statement = flag.String("statement", "", "Statement/hint")
	dtstruct.RuntimeCLIFlags.GrabElection = flag.Bool("grab-election", false, "Grab leadership (only applies to continuous mode)")
	dtstruct.RuntimeCLIFlags.PromotionRule = flag.String("promotion-rule", "prefer", "Promotion rule for register-andidate (prefer|neutral|prefer_not|must_not)")
	dtstruct.RuntimeCLIFlags.Version = flag.Bool("version", false, "Print version and exit")
	dtstruct.RuntimeCLIFlags.SkipContinuousRegistration = flag.Bool("skip-continuous-registration", false, "Skip cli commands performaing continuous registration (to reduce orchestratrator backend db load")
	dtstruct.RuntimeCLIFlags.EnableDatabaseUpdate = flag.Bool("enable-database-update", false, "Enable database update, overrides SkipDatabaseUpdate")
	dtstruct.RuntimeCLIFlags.IgnoreRaftSetup = flag.Bool("ignore-raft-setup", false, "Override RaftEnabled for CLI invocation (CLI by default not allowed for raft setups). NOTE: operations by CLI invocation may not reflect in all raft nodes.")
	dtstruct.RuntimeCLIFlags.Tag = flag.String("tag", "", "tag to add ('tagname' or 'tagname=tagvalue') or to search ('tagname' or 'tagname=tagvalue' or comma separated 'tag0,tag1=val1,tag2' for intersection of all)")
	flag.Parse()

	// show help, TODO refactor this next time
	helpTopic := ""
	if flag.Arg(0) == "help" {
		if flag.Arg(1) != "" {
			helpTopic = flag.Arg(1)
		}
		if helpTopic == "" {
			helpTopic = *command
		}
		// hacky way to make the CLI kick in as if the user typed `ham4db -c help cli`
		if helpTopic == "" {
			*command = "help"
			flag.Args()[0] = "cli"
		}
	}
	if helpTopic != "" {
		cli.HelpCommand(helpTopic)
		return
	}
	// No command, no argument: just prompt
	if len(flag.Args()) == 0 && *command == "" {
		fmt.Println(cli.AppPrompt)
		return
	}

	if *destination != "" && *sibling != "" {
		log.Fatalf("-s and -d are synonyms, yet both were specified. You're probably doing the wrong thing.")
	}
	switch *dtstruct.RuntimeCLIFlags.PromotionRule {
	case "prefer", "neutral", "prefer_not", "must_not":
		{
			// OK
		}
	default:
		{
			log.Fatalf("-promotion-rule only supports prefer|neutral|prefer_not|must_not")
		}
	}
	if *destination == "" {
		*destination = *sibling
	}

	if *verbose {
		log.SetLevel(log.INFO)
	}
	if *debug {
		log.SetLevel(log.DEBUG)
	}
	if *stack {
		log.SetPrintStackTrace(*stack)
	}
	if *dtstruct.RuntimeCLIFlags.Version {
		fmt.Println(AppVersion)
		fmt.Println(GitCommit)
		return
	}

	startText := "starting ham4db"
	if AppVersion != "" {
		startText += ", version: " + AppVersion
	}
	if GitCommit != "" {
		startText += ", git commit: " + GitCommit
	}
	log.Info(startText)

	if len(*configFile) > 0 {
		config.ForceRead(*configFile)
	}
	if *dtstruct.RuntimeCLIFlags.EnableDatabaseUpdate {
		config.Config.SkipDatabaseUpdate = false
	}
	if config.Config.Debug {
		log.SetLevel(log.DEBUG)
	}
	if *warning {
		log.SetLevel(log.WARNING)
	}
	if *quiet {
		// Override!!
		log.SetLevel(log.ERROR)
	}
	if config.Config.EnableSyslog {
		log.EnableSyslogWriter("ham4db")
		log.SetSyslogLevel(log.INFO)
	}
	if config.Config.AuditToSyslog {
		base.EnableSyslog()
	}
	dtstruct.RuntimeCLIFlags.ConfiguredVersion = AppVersion

	jcfg := jconfig.Configuration{Sampler: &jconfig.SamplerConfig{Type: jaeger.SamplerTypeConst, Param: 1}, Reporter: &jconfig.ReporterConfig{LogSpans: true, LocalAgentHostPort: "0.0.0.0:6831"}}
	closer, err := jcfg.InitGlobalTracer("ham4db")
	defer closer.Close()
	if err != nil {
		return
	}

	if *command != "help" {
		if len(*configFile) == 0 {
			log.Fatalf("should specified config file using option --config")
		}
		if err = initdb.BackendDBInit(); err != nil {
			log.Fatale(err)
		}
		config.MarkConfigurationLoaded()
	}

	switch {
	case len(flag.Args()) == 0 || flag.Arg(0) == "cli":
		cli.CliWrapper(*section, *command, *strict, *databaseType, *clusterId, *instance, *destination, *owner, *reason, *duration, *pattern, *clusterAlias, *pool, *hostnameFlag)
	case flag.Arg(0) == "http":
		http.Http(*discovery)
	default:
		fmt.Fprintln(os.Stderr, `Usage:
  ham4db --options... [cli|http]
See complete list of commands:
  ham4db -c help
Full blown documentation:
  ham4db`)
		os.Exit(1)
	}
}
