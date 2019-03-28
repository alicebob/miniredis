// Commands from https://redis.io/commands#server

package miniredis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linuxfreak003/miniredis/server"
)

func commandsServer(m *Miniredis) {
	m.srv.Register("DBSIZE", m.cmdDbsize)
	m.srv.Register("FLUSHALL", m.cmdFlushall)
	m.srv.Register("FLUSHDB", m.cmdFlushdb)
	m.srv.Register("TIME", m.cmdTime)
	m.srv.Register("INFO", m.cmdInfo)
}

// DBSIZE
func (m *Miniredis) cmdDbsize(c *server.Peer, cmd string, args []string) {
	if len(args) > 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		c.WriteInt(len(db.keys))
	})
}

// FLUSHALL
func (m *Miniredis) cmdFlushall(c *server.Peer, cmd string, args []string) {
	if len(args) > 0 && strings.ToLower(args[0]) == "async" {
		args = args[1:]
	}
	if len(args) > 0 {
		setDirty(c)
		c.WriteError(msgSyntaxError)
		return
	}

	if !m.handleAuth(c) {
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		m.flushAll()
		c.WriteOK()
	})
}

// FLUSHDB
func (m *Miniredis) cmdFlushdb(c *server.Peer, cmd string, args []string) {
	if len(args) > 0 && strings.ToLower(args[0]) == "async" {
		args = args[1:]
	}
	if len(args) > 0 {
		setDirty(c)
		c.WriteError(msgSyntaxError)
		return
	}

	if !m.handleAuth(c) {
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		m.db(ctx.selectedDB).flush()
		c.WriteOK()
	})
}

// TIME: time values are returned in string format instead of int
func (m *Miniredis) cmdTime(c *server.Peer, cmd string, args []string) {
	if len(args) > 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		now := m.now
		if now.IsZero() {
			now = time.Now()
		}
		nanos := now.UnixNano()
		seconds := nanos / 1000000000
		microseconds := (nanos / 1000) % 1000000

		c.WriteLen(2)
		c.WriteBulk(strconv.FormatInt(seconds, 10))
		c.WriteBulk(strconv.FormatInt(microseconds, 10))
	})
}

// INFO: returns info
func (m *Miniredis) cmdInfo(c *server.Peer, cmd string, args []string) {
	if len(args) > 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	response := `
# Server
redis_version:999.999.999
redis_git_sha1:3c968ff0
redis_git_dirty:0
redis_build_id:51089de051945df4
redis_mode:standalone
os:Linux 4.8.0-1-amd64 x86_64
arch_bits:64
multiplexing_api:epoll
atomicvar_api:atomic-builtin
gcc_version:6.3.0
process_id:11111
run_id:7e419f47915be43169ab38b3bfac2739f55a9810
tcp_port:6379
uptime_in_seconds:3644962
uptime_in_days:42
hz:10
lru_clock:10294713
executable:/usr/local/bin/redis-server
config_file:

# Clients
connected_clients:1
client_longest_output_list:0
client_biggest_input_buf:0
blocked_clients:0

# Memory
used_memory:454939440
used_memory_human:433.86M
used_memory_rss:468774912
used_memory_rss_human:447.06M
used_memory_peak:454979488
used_memory_peak_human:433.90M
used_memory_peak_perc:99.99%
used_memory_overhead:137730976
used_memory_startup:509680
used_memory_dataset:317208464
used_memory_dataset_perc:69.80%
allocator_allocated:454928680
allocator_active:455200768
allocator_resident:467652608
total_system_memory:1044770816
total_system_memory_human:996.37M
used_memory_lua:37888
used_memory_lua_human:37.00K
maxmemory:0
maxmemory_human:0B
maxmemory_policy:noeviction
allocator_frag_ratio:1.00
allocator_frag_bytes:272088
allocator_rss_ratio:1.03
allocator_rss_bytes:12451840
rss_overhead_ratio:1.00
rss_overhead_bytes:1122304
mem_fragmentation_ratio:1.03
mem_fragmentation_bytes:13918496
mem_allocator:jemalloc-4.0.3
active_defrag_running:0
lazyfree_pending_objects:0

# Persistence
loading:0
rdb_changes_since_last_save:1024830782
rdb_bgsave_in_progress:0
rdb_last_save_time:1550153623
rdb_last_bgsave_status:ok
rdb_last_bgsave_time_sec:-1
rdb_current_bgsave_time_sec:-1
rdb_last_cow_size:0
aof_enabled:0
aof_rewrite_in_progress:0
aof_rewrite_scheduled:0
aof_last_rewrite_time_sec:-1
aof_current_rewrite_time_sec:-1
aof_last_bgrewrite_status:ok
aof_last_write_status:ok
aof_last_cow_size:0

# Stats
total_connections_received:70
total_commands_processed:38582982
instantaneous_ops_per_sec:8
total_net_input_bytes:3210551075
total_net_output_bytes:503657990
instantaneous_input_kbps:0.74
instantaneous_output_kbps:0.10
rejected_connections:0
sync_full:0
sync_partial_ok:0
sync_partial_err:0
expired_keys:40110
expired_stale_perc:0.12
expired_time_cap_reached_count:0
evicted_keys:0
keyspace_hits:9331378
keyspace_misses:4083815
pubsub_channels:0
pubsub_patterns:0
latest_fork_usec:0
migrate_cached_sockets:0
slave_expires_tracked_keys:0
active_defrag_hits:0
active_defrag_misses:0
active_defrag_key_hits:0
active_defrag_key_misses:0

# Replication
role:master
connected_slaves:0
master_replid:0f9f025be8f6497a72318c74954819e0511f1b49
master_replid2:0000000000000000000000000000000000000000
master_repl_offset:0
second_repl_offset:-1
repl_backlog_active:0
repl_backlog_size:1048576
repl_backlog_first_byte_offset:0
repl_backlog_histlen:0

# CPU
used_cpu_sys:3087.21
used_cpu_user:35192.17
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Cluster
cluster_enabled:0

# Keyspace
`
	for _, db := range m.dbs {
		var ttlAvg int64
		if l := len(db.ttl); l > 0 {
			for _, ttl := range db.ttl {
				ttlAvg += ttl.Nanoseconds()
			}
			ttlAvg = ttlAvg / int64(l)
		}

		ks := fmt.Sprintf("db%d:keys=%d,expires=2177,agv_ttl=%d\n", db.id, len(db.keys), ttlAvg)
		response += ks
	}
	c.WriteBulk(response)
}
