#!/usr/local/bin/tarantool

-- Distributed rps quota library using gossip protocol
-- Main params - connection pool with connections to gossip members,
-- count of neighbours to distrubute updates on each gossip round
-- Resource identifies by user, resource domain and operation
-- (e.g. user1, subscriptions, write)
-- Each second sliced to some count of intervals, for each of them we 
-- count amount of used resource. For time we use scaled time abstraction, so
-- scaled_time = [time * count_of_slices_in_second].

local fiber = require('fiber')
local math = require('math')

-- cache lua functions
local mmin = math.min
local mmax = math.max
local mmod = math.mod
local mrandom = math.random
local mfloor = math.floor


-- Quotas constants
-- count of SLICES in one second
local SLICES = 10
-- max count of lease anounces grouped in one rpc call
local PACK_SIZE = 10

-- Quotas params
-- max amount for granting
local max_lease

-- Gossip params
-- cluster connection pool
local conn_pool
-- count of cluster members to send lease announces on each round
local neighbour_count
-- local server id
local server_id

-- Quotas cluster state
local grants = {}

-- Local grant remainders
local remainders = {}

local gossip_queue
local gossip_fiber

-- build resource key from user, domain and operation
local function build_resource_key(user, domain, op)
    return user .. '::' .. domain .. '::' .. op
end

-- build key for resource and server
local function build_server_resource_key(server, resource_key)
    return server .. '::' .. resource_key
end

-- check if scaled_time is outdated, renew grant info (clear outdated history)
local function renew_grant(grant, scaled_time)
    if grant.update_time and scaled_time <= grant.update_time - SLICES then
        return false
    end
    if grant.update_time and scaled_time <= grant.update_time then
        return true
    end
    if grant.update_time == nil then
        grant.update_time = scaled_time
        for i = 0, SLICES - 1 do
            grant[i] = 0
        end
        return true
    end
    
    for i = 1, mmin(SLICES, scaled_time - grant.update_time) do
        grant[mmod(grant.update_time + i, SLICES)] = 0
    end
    grant.update_time = scaled_time
    return true
end

-- return grant info by key (server or cluster and resource)
local function get_grant(key)
    local grant = grants[key]
    if grant == nil then
        grant = {}
        grants[key] = grant
    end
    return grant
end

-- get resource grants for cluster
local function get_cluster_grant(resource_key)
    return get_grant(resource_key)
end

-- get resource grant for server
local function get_server_grant(server, resource_key)
    return get_grant(build_server_resource_key(server, resource_key))
end

-- select random count items from servers excluding local
local function select_neighbours(servers, count)
    local neighbours = {}
    for _, server in pairs(servers) do
        if server ~= conn_pool.self_server then
            neighbours[#neighbours + 1] = server
        end
    end
    count = mmin(#neighbours, count)
    local selected = {}
    for i = 1, count do
        local index = mrandom(#neighbours - i)
        selected[i] = neighbours[index]
        neighbours[index] = neighbours[#neighbours]
        neighbours[#neighbours] = selected[i]
    end
    return selected
end

-- forward grant info to next neigbours
local function gossip_fiber(queue)
    while true do
        local task = queue:get()
        local servers = conn_pool:all()
        local task_pack = {}
        local task_count = 0
        repeat
            local index = mmod(task.scaled_time, SLICES)
            local server_grant = get_server_grant(task.server, task.key)
	    -- check if grant info is not outdated
            if renew_grant(server_grant, task.scaled_time)
                and (server_grant[index] < task.grant or task.server == server_id) then
                local server_key = build_server_resource_key(task.server, task.key)
                if task_pack[server_key] then
		    -- update exisiting task in pack
                    task_pack[server_key].grant = server_grant[index]
                else
                    task.grant = server_grant[index]
                    task_pack[server_key] = task
                    task_count = task_count + 1
                end
            end
            if task_count >= PACK_SIZE then
                break
            end 
            task = queue:get(0)
        until task == nil
        if task_count > 0 then
            -- send task pack
            for _, neighbour in pairs(select_neighbours(servers, neighbour_count)) do
                neighbour.conn:call('pop_lease', task_pack)
            end
        end
    end
end

-- apply grant info update to cluster state
local function update_grant(task)
    local index = mmod(task.scaled_time, SLICES)
    local server_grant = get_server_grant(task.server, task.key)
    if not renew_grant(server_grant, task.scaled_time) then
        return false
    end
    local delta = task.grant - server_grant[index]
    if delta <= 0 then
        return false
    end
    server_grant[index] = server_grant[index] + delta
    local cluster_grant = get_cluster_grant(task.key)
    if not renew_grant(cluster_grant, task.scaled_time) then
        return false
    end
    cluster_grant[index] = cluster_grant[index] + delta
    return true
end

-- accept grant info pack, apply it to cluster state and forward
function pop_lease(task_pack)
    for i, task in pairs(task_pack) do
        if task.server ~= server_id and update_grant(task) then
            gossip_queue:put(task)
        end
    end
end

-- request resource grant
local function request_lease(key, quota, time)
    -- get "scaled" time
    local scaled_time = mfloor(time * SLICES)
    -- calculate resource usage for last 1 second
    local cluster_grant = get_cluster_grant(key)
    if not renew_grant(cluster_grant, scaled_time) then
        return {}
    end

    local quota_used = 0
    for i = 0, SLICES - 1 do
        quota_used = quota_used + cluster_grant[i]
    end

    -- detect granted resource amount
    local lease = mmax(mmin(max_lease, quota - quota_used), 0)
    if lease == 0 then
        return {}
    end

    local index = mmod(scaled_time, SLICES)
    cluster_grant[index] = cluster_grant[index] + lease

    local server_grant = get_server_grant(server_id, key)
    renew_grant(server_grant, scaled_time)
    server_grant[index] = server_grant[index] + lease
    if lease > 0 then
        gossip_queue:put({
            server = server_id,
            key = key,
            scaled_time = scaled_time,
            grant = server_grant[index]})
    end
    return {value = lease, expiration = time + 1 / SLICES}
end

-- check if we have available remainder, or request lease if not
local function get_lease(user, domain, op, quota)
    local key = build_resource_key(user, domain, op)
    local time = fiber.time()
    local remainder = remainders[key]
    if not (remainder and remainder.expiration and remainder.expiration > time
        and remainder.value and remainder.value > 0) then
        remainder = request_lease(key, quota, time)
        remainders[key] = remainder
    end
    if remainder.value and remainder.value > 0 then
        remainder.value = remainder.value - 1
        return true
    end
    return false
end

local function init_leases(p_conn_pool, p_neighbour_count, p_max_lease)
    conn_pool = p_conn_pool
    neighbour_count = p_neighbour_count
    max_lease = p_max_lease
    server_id = box.info.server.uuid
    gossip_queue = fiber.channel(100)
    fiber.create(gossip_fiber, gossip_queue)
end


return {
    init = init_leases,
    get_lease = get_lease,
}
