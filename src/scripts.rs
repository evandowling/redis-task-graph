//! Lua scripts that make multi-step state transitions atomic.
//!
//! Each script that touches a claimed entry first checks, via `XPENDING ... consumer`, that the
//! calling consumer still owns it. A worker whose lease was reclaimed therefore cannot ack,
//! retry or extend an entry that another worker is now processing.
//!
//! Every script except [`LEGACY_PROMOTE`] receives all the keys it touches, so they also run on
//! Redis Cluster.

use std::sync::LazyLock;

use redis::Script;

/// `lost` returns false if `consumer` owns the entry, 0 if another consumer does, and -1 if the
/// entry is no longer pending. The second `XPENDING` runs only when the lease was lost.
const LOST: &str = r"
local function lost(stream, group, id, consumer)
  if #redis.call('XPENDING', stream, group, id, id, 1, consumer) > 0 then return false end
  if #redis.call('XPENDING', stream, group, id, id, 1) > 0 then return 0 end
  return -1
end
";

fn with_lost(body: &str) -> Script {
    Script::new(&format!("{LOST}\n{body}"))
}

/// KEYS: stream. ARGV: group, id, consumer. Returns 1 if acked; otherwise the lease was lost, and
/// it returns 0 if another consumer owns the entry or -1 if it is no longer pending.
pub(crate) static ACK: LazyLock<Script> = LazyLock::new(|| {
    with_lost(
        r"
local l = lost(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) if l then return l end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
",
    )
});

/// KEYS: stream, next_stream, chain. ARGV: group, id, consumer, next_step, then the next step's
/// fields: header field, header, context field, context.
/// Completes one chain step and enqueues the next one. Returns 1 on success, or 0 or -1 as for
/// [`ACK`] if the lease was lost.
pub(crate) static ADVANCE: LazyLock<Script> = LazyLock::new(|| {
    with_lost(
        r"
local l = lost(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) if l then return l end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('XADD', KEYS[2], '*', ARGV[5], ARGV[6], ARGV[7], ARGV[8])
redis.call('HSET', KEYS[3], 'state', 'running', 'step', ARGV[4])
return 1
",
    )
});

/// KEYS: stream, chain. ARGV: group, id, consumer, output, ttl_secs.
/// Completes a chain's final step and records its output. Returns 1 on success, or 0 or -1 as for
/// [`ACK`] if the lease was lost.
pub(crate) static COMPLETE: LazyLock<Script> = LazyLock::new(|| {
    with_lost(
        r"
local l = lost(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) if l then return l end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('HSET', KEYS[2], 'state', 'completed', 'output', ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[5])
return 1
",
    )
});

/// KEYS: stream, delayed, dead, and for chain steps also the chain key.
/// ARGV: group, id, consumer, mode ('retry' | 'dead'), payload, delay_ms, dead_max_len, field,
/// and for chain steps also step, error, ttl_secs.
/// Removes the entry and either schedules `payload` for retry or appends it to the dead-letter
/// stream, marking the chain failed. Returns 1 on success, or 0 or -1 as for [`ACK`] if the
/// lease was lost.
pub(crate) static RETRY_OR_DEAD: LazyLock<Script> = LazyLock::new(|| {
    with_lost(
        r"
local l = lost(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) if l then return l end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if ARGV[4] == 'retry' then
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  redis.call('ZADD', KEYS[2], now + tonumber(ARGV[6]), ARGV[5])
else
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[7], '*', ARGV[8], ARGV[5])
  if KEYS[4] then
    redis.call('HSET', KEYS[4], 'state', 'failed', 'step', ARGV[9], 'error', ARGV[10])
    redis.call('EXPIRE', KEYS[4], ARGV[11])
  end
end
return 1
",
    )
});

/// KEYS: stream. ARGV: group, consumer. Removes the consumer from the group unless it still
/// has pending entries, which deleting it would orphan. Returns 1 if removed, 0 if not.
pub(crate) static REMOVE_CONSUMER: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if #redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]) > 0 then return 0 end
redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
return 1
",
    )
});

/// KEYS: the stream of each entry. ARGV: group, consumer, then the entry ids, in KEYS order.
/// Resets each entry's idle time if this consumer still owns it. Returns one status per entry:
/// 1 if the lease was extended, 0 if another consumer now owns the entry, and -1 if the entry
/// is no longer pending (it was already acked).
pub(crate) static HEARTBEAT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local group, consumer = ARGV[1], ARGV[2]
local statuses = {}
for i, stream in ipairs(KEYS) do
  local id = ARGV[i + 2]
  local pending = redis.call('XPENDING', stream, group, id, id, 1)
  if #pending == 0 then
    statuses[i] = -1
  elseif pending[1][2] ~= consumer then
    statuses[i] = 0
  else
    redis.call('XCLAIM', stream, group, consumer, 0, id, 'JUSTID')
    statuses[i] = 1
  end
end
return statuses
",
    )
});

/// KEYS: one processor type's delayed set, that type's stream. ARGV: limit, field.
/// Moves due retries back onto the stream. Returns the number moved.
pub(crate) static PROMOTE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, payload in ipairs(due) do
  redis.call('XADD', KEYS[2], '*', ARGV[2], payload)
  redis.call('ZREM', KEYS[1], payload)
end
return #due
",
    )
});

/// KEYS: the 0.1 shared delayed set, dead. ARGV: 0.1 stream_prefix, limit, field, dead_max_len.
/// Moves due 0.1 retries back onto their processor type's 0.1 stream. Returns the number moved.
/// It builds stream keys from task data, which is why 0.2 replaced it; it runs only while a
/// namespace still has 0.1 data, which can only exist on a standalone Redis.
pub(crate) static LEGACY_PROMOTE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
for _, payload in ipairs(due) do
  local ok, env = pcall(cjson.decode, payload)
  if ok and type(env) == 'table' and type(env.processor_type) == 'string' then
    redis.call('XADD', ARGV[1] .. env.processor_type, '*', ARGV[3], payload)
  else
    redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[4], '*', ARGV[3], payload)
  end
  redis.call('ZREM', KEYS[1], payload)
end
return #due
",
    )
});

#[cfg(test)]
mod tests {
    use super::*;

    /// Finalize scripts tell a lease held by another consumer (0) from an entry that is no
    /// longer pending (-1). Skipped when Redis isn't reachable.
    #[test]
    fn ack_reports_why_the_lease_was_lost() {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
        let Ok(mut conn) = redis::Client::open(url.as_str())
            .and_then(|c| c.get_connection_with_timeout(std::time::Duration::from_secs(2)))
        else {
            eprintln!("skipping: Redis not reachable at {url}");
            return;
        };
        let stream = format!("{{rtg-test-{}}}:stream:t", uuid::Uuid::new_v4());
        let _: () = redis::cmd("XGROUP")
            .arg("CREATE").arg(&stream).arg("g").arg("$").arg("MKSTREAM")
            .query(&mut conn)
            .unwrap();
        let id: String = redis::cmd("XADD").arg(&stream).arg("*").arg("f").arg("v").query(&mut conn).unwrap();
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP").arg("g").arg("a").arg("STREAMS").arg(&stream).arg(">")
            .query(&mut conn)
            .unwrap();
        let ack = |conn: &mut redis::Connection, consumer: &str| -> i64 {
            ACK.key(&stream).arg("g").arg(&id).arg(consumer).invoke(conn).unwrap()
        };

        assert_eq!(ack(&mut conn, "b"), 0, "owned by another consumer");
        assert_eq!(ack(&mut conn, "a"), 1, "acked by its owner");
        assert_eq!(ack(&mut conn, "a"), -1, "no longer pending");
        let _: () = redis::cmd("DEL").arg(&stream).query(&mut conn).unwrap();
    }
}
