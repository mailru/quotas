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

local gossip_queue
local gossip_fiber

function bootstrapdb()
  box.schema.func.create('announce_lease', {if_not_exists = true})
  box.schema.user.grant('guest', 'execute', 'function', 'announce_lease',
    {if_not_exists = true})
end

function get_max_lease(key)
  return max_lease
end

function get_key(user, domain, op)
  return user .. '::' .. domain .. '::' .. op
end

function get_usage(key, slice_time)
    local usage = leases[key]
    if usage == nil then
        usage = {history = {}, slice_time = slice_time}
        for i = 1, slices do
            usage['history'][i] = 0
        end
        leases[key] = usage
    end
    history = usage['history']
    for i = usage['slice_time'] + 1, 
        slice_time do
        --math.min(slice_time - 1, usage['slice_time'] - 1 + slices) do
        history[1 + math.mod(i, slices)] = 0
    end
    usage['slice_time'] = slice_time
    return usage
end

function get_announce(srv, key, slice_time)
    if announces[srv] == nil then
        announces[srv] = {[key] = {slice_time = slice_time, used = 0}}
    end
    if announces[srv][key] == nil or announces[srv][key]['slice_time'] < slice_time then
       announces[srv][key] = {slice_time = slice_time, used = 0}
    end
    return announces[srv][key]
end

function send_lease(servers, srv, key, slice_time, used)
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
		if servers[i].conn:call('pop_lease', srv, key, slice_time, used) then
                    left = left - 1
                end
                processed = processed + 1
            end
	end
        if left == 0 then
            break
        end
    end
end

function gossip_fiber(queue)
    local servers = conn_pool:all()
    while true do
        local task = queue:get()
        if task.mode == 'stop' then
	    break
	end
	if task.mode == 'push' then
	    local announce = 
	        get_announce(box.info.server.uuid, task.key, task.slice_time)
	    announce.used = announce.used + task.grant
	    send_lease(servers,
	        box.info.server.uuid, task.key, task.slice_time, announce.used)
        end
	if task.mode == 'pop' then
            local announce = get_announce(task.srv, task.key, task.slice_time)
            if announce.used < task.used and announce.slice_time == task.slice_time then
	        local usage = get_usage(task.key, task.slice_time)
		usage.history[1 + math.mod(task.slice_time, slices)] = 
		    usage.history[1 + math.mod(task.slice_time, slices)] +
		    task.used - announce.used
	        announce.used = task.used
	        send_lease(servers,
	            task.srv, task.key, task.slice_time, announce.used)
	    end
	end
    end
end

function pop_lease(srv, key, slice_time, used)
  if srv == box.info.server.uuid then
      return false
  end
  return gossip_queue:put({
      mode = 'pop', 
      srv = srv, key = key, 
      slice_time = slice_time, 
      used = used})
end

function request_lease(key, quota, time)
    local slice_time = math.floor(time * slices)
    local usage = get_usage(key, slice_time)

    --sum last sec resource usage
    local history = usage['history']
    local quota_usage = 0
    for i = slice_time - slices + 1, usage['slice_time'] do
        quota_usage = quota_usage + history[1 + math.mod(i, slices)]
    end
    local grant = math.min(get_max_lease(key), quota - quota_usage)
    grant = math.max(grant, 0)
    history[1 + math.mod(slice_time, slices)] =
        history[1 + math.mod(slice_time, slices)] + grant
    if grant > 0 then
        gossip_queue:put({
	    mode = 'push',
	    key = key,
	    slice_time = slice_time,
	    grant = grant})
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

function init_lease(p_conn_pool, p_neighbor, p_max_lease)
  bootstrapdb()
  conn_pool = p_conn_pool
  neighbor = p_neighbor
  max_lease = p_max_lease
  gossip_queue = fiber.channel(10000)
  fiber.create(gossip_fiber, gossip_queue)
end


return {
  init = init_lease,
  get_lease = get_lease
}
