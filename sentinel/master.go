package sentinel

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type MasterInfo struct {
	Name                  string `mapstructure:"name"`
	IP                    string `mapstructure:"ip"`
	Port                  string `mapstructure:"port"`
	RunID                 string `mapstructure:"runid"`
	Flags                 string `mapstructure:"flags"`
	LinkPendingCommands   string `mapstructure:"link-pending-commands"`
	LinkRefCount          string `mapstructure:"link-refcount"`
	LastPingSent          string `mapstructure:"last-ping-sent"`
	LastOkPingReply       string `mapstructure:"last-ok-ping-reply"`
	LastPingReply         string `mapstructure:"last-ping-reply"`
	DownAfterMilliseconds string `mapstructure:"down-after-milliseconds"`
	InfoRefresh           string `mapstructure:"info-refresh"`
	RoleReported          string `mapstructure:"role-reported"`
	RoleReportedTime      string `mapstructure:"role-reported-time"`
	ConfigEpoch           string `mapstructure:"config-epoch"`
	NumSlaves             string `mapstructure:"num-slaves"`
	NumOtherSentinels     string `mapstructure:"num-other-sentinels"`
	Quorum                string `mapstructure:"quorum"`
	FailoverTimeout       string `mapstructure:"failover-timeout"`
	ParallelSync          string `mapstructure:"parallel-syncs"`
}

func initMasterInfo(s *Sentinel, opts ...Option) MasterInfo {
	o := GetOpts(opts...)
	s.masterInfo = MasterInfo{
		Name:                  o.masterName,
		IP:                    s.master.Host(),
		Port:                  s.master.Port(),
		RunID:                 uuid.New().String(),
		Flags:                 "master",
		LinkPendingCommands:   "0",
		LinkRefCount:          "1",
		LastPingSent:          "0",
		LastOkPingReply:       "0",
		LastPingReply:         "0",
		DownAfterMilliseconds: "5000",
		InfoRefresh:           "6295",
		RoleReported:          "master",
		RoleReportedTime:      fmt.Sprintf("%d", time.Now().Unix()),
		ConfigEpoch:           "1",
		NumSlaves:             fmt.Sprintf("%d", len(s.replicas)),
		NumOtherSentinels:     "0",
		Quorum:                "1",
		FailoverTimeout:       "60000",
		ParallelSync:          "1",
	}
	return s.masterInfo
}
