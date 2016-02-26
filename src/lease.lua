#!/usr/local/bin/tarantool

local fiber = require('fiber')
local math = require('math')
local pickle = require('pickle')
local digest = require('digest')

local slices = 10
local task_pack_size = 10

local conn_pool
local neighbor
local max_lease

local leases = {}
local reminders = {}
local announces = {}

local gossip_queue
local gossip_fiber

local function get_max_lease(key)
    return max_lease
end

local function get_key(user, domain, op)
    return user .. '::' .. domain .. '::' .. op
end

local function get_usage(key, stime)
    local usage = leases[key]
    if usage == nil then
        usage = {history = {}, stime = stime}
        for i = 1, slices do
            usage.history[i] = 0
        end
        leases[key] = usage
    end
    history = usage.history
    for i = usage.stime + 1,
        math.min(stime, usage.stime + slices) do
        history[1 + math.mod(i, slices)] = 0
    end
    if usage.stime < stime then
        usage.stime = stime
    end
    if usage.stime - slices > stime then
        return nil
    end
    return usage
end

local function get_announce(srv, key, stime)
    if announces[srv] == nil then
        announces[srv] = {[key] = {stime = stime, history = {}}}
        for i = 1, slices do
            announces[srv][key].history[i] = 0
        end
    end
    if announces[srv][key] == nil then
        announces[srv][key] = {stime = stime, history = {}}
        for i = 1, slices do
            announces[srv][key].history[i] = 0
        end
    end
    announce = announces[srv][key]
    for i = announce.stime + 1,
        math.min(stime, announce.stime + slices) do
        announce.history[1 + math.mod(i, slices)] = 0
    end
    if announce.stime < stime then
        announce.stime = stime
    end
    if announce.stime - slices > stime then
        return nil
    end
    return announce
end

local function send_lease(servers, task_pack)
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
                servers[i].conn:call('pop_lease', task_pack)
                left = left - 1
            end
            processed = processed + 1
        end
        if left == 0 then
            break
        end
    end
end

local function lease_task(srv, key, stime, used)
    return {
        mode = 'pop',
        srv = srv,
        key = key,
        stime = stime,
        used = used}
end

local function gossip_fiber(queue)
    while true do
        local task = queue:get()
        local servers = conn_pool:all()
        local task_pack = {}
        local task_count = 0
        repeat
            local index = 1 + math.mod(task.stime, slices)
            local announce = get_announce(task.srv, task.key, task.stime)
            if announce ~= nil then
                if task.mode == 'push' then
                    announce.history[index] = announce.history[index] + task.grant
                else
                    local usage = get_usage(task.key, task.stime)
                    if announce.history[index] < task.used and usage ~= nil then
                        usage.history[index] = usage.history[index] +
                            task.used - announce.history[index]
                        announce.history[index] = task.used
                    else
                        task = nil
                    end
                end
                if task ~= nil then
                    task_pack[task.srv .. '::' .. task.key] =
                        lease_task(task.srv, task.key, task.stime, announce.history[index])
                    task_count = task_count + 1
                end
            end
            if task_count >= task_pack_size then
                break
            end 
            task = queue:get(0)
        until task == nil
        if task_count > 0 then
            send_lease(servers, task_pack)
        end
    end
end

function pop_lease(task_pack)
    for i, task in pairs(task_pack) do
        if task.srv ~= box.info.server.uuid then
            gossip_queue:put(task)
        end
    end
end

local function request_lease(key, quota, time)
    local stime = math.floor(time * slices)
    local usage = get_usage(key, stime)
    local history = usage.history
    local quota_usage = 0
    for i = stime - slices + 1, usage.stime do
        quota_usage = quota_usage + history[1 + math.mod(i, slices)]
    end
    local grant = math.min(get_max_lease(key), quota - quota_usage)
    grant = math.max(grant, 0)
    history[1 + math.mod(stime, slices)] =
        history[1 + math.mod(stime, slices)] + grant
    if grant > 0 then
        gossip_queue:put({
            mode = 'push',
            srv = box.info.server.uuid,
            key = key,
            stime = stime,
            grant = grant})
    end
    return {lease = grant, exp = (stime + 1) / slices}
end

local function get_lease(user, domain, op, quota)
    local key = get_key(user, domain, op)
    local reminder = reminders[key]
    local time = fiber.time()
    if reminder == nil or reminder.exp <= time or reminder.lease == 0 then
        reminder = request_lease(key, quota, time)
        reminders[key] = reminder
    end
    if reminder.lease > 0 then
        reminder.lease = reminder.lease - 1
        return true
    end
    return false
end

local function init_lease(p_conn_pool, p_neighbor, p_max_lease)
    conn_pool = p_conn_pool
    neighbor = p_neighbor
    max_lease = p_max_lease
    gossip_queue = fiber.channel(1000)
    fiber.create(gossip_fiber, gossip_queue)
end


return {
    init = init_lease,
    get_lease = get_lease,
}
