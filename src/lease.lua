#!/usr/local/bin/tarantool

fiber = require('fiber')
math = require('math')
pickle = require('pickle')
digest = require('digest')

slices = 10

local conn_pool
local neighbor
local max_lease

local leases = {}
local reminders = {}
local announces = {}

function bootstrapdb()
  box.schema.func.create('announce_lease', {if_not_exists = true})
  box.schema.user.grant('guest', 'execute', 'function', 'announce_lease',
    {if_not_exists = true})
end

function init_lease(p_conn_pool, p_neighbor, p_max_lease)
  bootstrapdb()
  conn_pool = p_conn_pool
  neighbor = p_neighbor
  max_lease = p_max_lease
end

function get_max_lease(key)
  return max_lease
end

function get_key(user, domain, op)
  return user .. '::' .. domain .. '::' .. op
end

--clear outdated history
function clear_history(usage, stime)
  local history = usage['history']
  for i = usage['stime'] + 1, math.min(stime, usage['stime'] + slices) do
    history[1 + math.mod(i, slices)] = 0
  end
  usage['stime'] = stime
end

function get_usage(key, slice_time)
  local usage = leases[key]
  if usage == nil then
    usage = {history = {}, stime = slice_time}
    for i = 1, slices do
      usage['history'][i] = 0
    end
    leases[key] = usage
  end
  return usage
end

function gossip_lease(key, srv, stime, used)
  local servers = conn_pool:all()
  local g_size = #servers
  if conn_pool.self_server ~= nil then
    g_size = g_size - 1
  end
  local left = math.min(neighbor, g_size)
  local processed = 0
  for i = 1, #servers do
    if servers[i] ~= conn_pool.self_server then
      if math.mod(pickle.unpack('i', digest.urandom(4)),
                 (g_size - processed)) < left then
        servers[i].conn:call('announce_lease', key, srv, stime, used)
        left = left - 1
      end
      processed = processed + 1
    end
    if left == 0 then
      break
    end
  end
end

function announce_lease(key, srv, stime, used)
  if srv == box.info.server.uuid then
    return
  end
  if announces[srv] == nil then
    announces[srv] = {[key] = {used = 0, stime = stime}}
  elseif announces[srv][key] == nil or
    announces[srv][key]['stime'] < stime then
    announces[srv][key] = {used = 0, stime = stime}
  end
  if announces[srv][key]['stime'] > stime or
    announces[srv][key]['used'] >= used then
    return
  end

  announces[srv][key]['used'] = used
  local usage = get_usage(key, stime)
  clear_history(usage, stime)
  usage['history'][1 + math.mod(stime, slices)] =
    usage['history'][1 + math.mod(stime, slices)] +
    used - announces[srv][key]['used']
  fiber.create(gossip_lease, key, srv, stime, used)
end

function request_lease(key, quota, time)
  local slice_time = math.floor(time * slices)
  local usage = get_usage(key, slice_time)

  clear_history(usage, slice_time)

  --sum last sec resource usage
  local history = usage['history']
  local quota_usage = 0
  for i = slice_time - slices + 1, usage['stime'] do
    quota_usage = quota_usage + history[1 + math.mod(i, slices)]
  end
  local grant = math.min(get_max_lease(key), quota - quota_usage)
  grant = math.max(grant, 0)
  history[1 + math.mod(slice_time, slices)] =
    history[1 + math.mod(slice_time, slices)] + grant
  if grant > 0 then
    fiber.create(gossip_lease, key,
      box.info.server.uuid, slice_time, quota_usage + grant)
  end
  return {lease = grant, exp = (slice_time + 1) / slices}
end

function get_lease(user, domain, op, quota)
  local key = get_key(user, domain, op)
  local reminder = reminders[key]
  local time = fiber.time()
  if reminder == nil or reminder['exp'] <= time or reminder['lease'] == 0 then
    reminder = request_lease(key, quota, time)
    reminders[key] = reminder
  end
  if reminder['lease'] > 0 then
    reminder['lease'] = reminder['lease'] - 1
    return true
  end
  return false
end

return {
  init = init_lease,
  get_lease = get_lease
}
