//! Lua scripts that make multi-step state transitions atomic.
//!
//! Each script that touches a claimed entry first checks, via `XPENDING ... consumer`, that the
//! calling consumer still owns it. A worker whose lease was reclaimed therefore cannot ack,
//! retry or extend an entry that another worker is now processing.
//!
//! The promote script derives stream keys from the payload, so these scripts assume a
//! non-cluster Redis deployment.

use std::sync::LazyLock;

use redis::Script;

const OWNED: &str = r"
local function owned(stream, group, id, consumer)
  return #redis.call('XPENDING', stream, group, id, id, 1, consumer) > 0
end
";

fn with_owned(body: &str) -> Script {
    Script::new(&format!("{OWNED}\n{body}"))
}

/// KEYS: stream. ARGV: group, id, consumer. Returns 1 if acked, 0 if the lease was lost.
pub(crate) static ACK: LazyLock<Script> = LazyLock::new(|| {
    with_owned(
        r"
if not owned(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) then return 0 end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
",
    )
});

/// KEYS: stream, delayed, dead.
/// ARGV: group, id, consumer, mode ('retry' | 'dead'), payload, delay_ms, dead_max_len, field.
/// Removes the entry and either schedules `payload` for retry or appends it to the dead-letter
/// stream. Returns 1 on success, 0 if the lease was lost.
pub(crate) static RETRY_OR_DEAD: LazyLock<Script> = LazyLock::new(|| {
    with_owned(
        r"
if not owned(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) then return 0 end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if ARGV[4] == 'retry' then
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  redis.call('ZADD', KEYS[2], now + tonumber(ARGV[6]), ARGV[5])
else
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[7], '*', ARGV[8], ARGV[5])
end
return 1
",
    )
});

/// KEYS: stream. ARGV: group, id, consumer. Resets the entry's idle time if still owned.
/// Returns 1 if the lease was extended, 0 if it was lost.
pub(crate) static HEARTBEAT: LazyLock<Script> = LazyLock::new(|| {
    with_owned(
        r"
if not owned(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) then return 0 end
redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], 'JUSTID')
return 1
",
    )
});

/// KEYS: delayed, dead. ARGV: stream_prefix, limit, field, dead_max_len.
/// Moves due retries back onto their processor type's stream. Returns the number moved.
pub(crate) static PROMOTE: LazyLock<Script> = LazyLock::new(|| {
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
