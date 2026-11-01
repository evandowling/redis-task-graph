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

/// KEYS: stream, next_stream, chain. ARGV: group, id, consumer, next_payload, field, next_step.
/// Completes one chain step and enqueues the next one. Returns 1 on success, 0 if the lease was
/// lost.
pub(crate) static ADVANCE: LazyLock<Script> = LazyLock::new(|| {
    with_owned(
        r"
if not owned(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) then return 0 end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('XADD', KEYS[2], '*', ARGV[5], ARGV[4])
redis.call('HSET', KEYS[3], 'state', 'running', 'step', ARGV[6])
return 1
",
    )
});

/// KEYS: stream, chain. ARGV: group, id, consumer, output, ttl_secs.
/// Completes a chain's final step and records its output. Returns 1 on success, 0 if the lease
/// was lost.
pub(crate) static COMPLETE: LazyLock<Script> = LazyLock::new(|| {
    with_owned(
        r"
if not owned(KEYS[1], ARGV[1], ARGV[2], ARGV[3]) then return 0 end
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
/// stream, marking the chain failed. Returns 1 on success, 0 if the lease was lost.
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
  if KEYS[4] then
    redis.call('HSET', KEYS[4], 'state', 'failed', 'step', ARGV[9], 'error', ARGV[10])
    redis.call('EXPIRE', KEYS[4], ARGV[11])
  end
end
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
