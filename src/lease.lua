#!/usr/local/bin/tarantool

f = require('fiber')
m = require('math')
n = require('net.box')

slices = 10

local master_addr
local max_lease

function bootstrapdb()
  s = box.schema.space.create('leases', {if_not_exists = true})
  box.schema.user.grant('guest', 'read,write', 'space', 'leases', {if_not_exists = true})
  i = s:create_index('primary', { if_not_exists = true, type = 'hash', unique = true,
    parts = {1, 'str', 2, 'str', 3, 'num'}})
  s:truncate()
  box.schema.func.create('request_lease', {if_not_exists = true})
  box.schema.user.grant('guest', 'execute', 'function', 'request_lease', {if_not_exists = true})
end

function init_lease(p_master_addr, p_max_lease)
  bootstrapdb()
  master_addr = p_master_addr
  max_lease = p_max_lease
end

function get_master(user, domain)
  return n:new(master_addr[1], master_addr[2])
end

function is_master(user, domain)
  return master_addr == nil
end

function get_max_lease(user, domain, op)
  return max_lease
end

function request_lease(user, domain, op, quota)
  local time = f.time()
  local slice_time = m.floor(time * slices)
  local s = box.space['leases']
  local lease = s:select({user, domain, op})

  if is_master(user, domain) then
    if #lease == 0 then
      usage = {}
      for i = 1, slices do
        usage[i] = m.floor(quota / slices)
      end
      local slice = 1 + m.mod(slice_time, slices)
      usage[slice] = quota - (slices - 1) * m.floor(quota / slices)
      s:insert({user, domain, op, quota, 
        {usage = usage, utime = slice_time}})
      return {0, (slice_time + 1) / slices}
    else
      lease = lease[1]
      local usage = lease[5]['usage']

      local quota_usage = 0
      for i = slice_time - slices + 1, lease[5]['utime'] do
        quota_usage = quota_usage + usage[1 + m.mod(i, slices)]
      end

      for i = lease[5]['utime'] + 1, 
        m.min(slice_time, lease[5]['utime'] + slices) do
        usage[1 + m.mod(i, slices)] = 0
      end

      local grant = math.min(get_max_lease(user, domain, op), quota - quota_usage)
      usage[1 + m.mod(slice_time, slices)] = 
        usage[1 + m.mod(slice_time, slices)] + grant
      s:update({user, domain, op}, {{'=', 4, quota}, 
        {'=', 5, {usage = usage, utime = slice_time}}})
      return {grant, (slice_time + 1) / slices}
    end
  end

  if #lease > 0 then
    lease = lease[1]
    local rem = lease[5]['lease'] + m.min(quota - lease[4], 0)
    if lease[5]['expire'] < time then
      rem = 0
    end
    grant = m.min(rem, get_max_lease(user, domain))
    if grant > 0 then
      s:update({user, domain, op}, {{'=', 4, quota}, 
        {'=', 5, {lease = rem - grant, expire = lease[5]['expire']}}})
      return {grant, lease[5]['expire']}
    end
  end
  master = get_master(u, d)
  master_lease = master:call('request_lease', user, domain, op, quota)[1]
  master:close()
  grant = m.min(master_lease[1], get_max_lease(user, domain))
  if #lease > 0 then
    s:update({user, domain, op}, {{'=', 4, quota}, 
        {'=', 5, {lease = grant, expire = master_lease[2]}}})
  else
    s:insert({user, domain, op, quota, 
      {lease = master_lease[1] - grant,
        expire = master_lease[2]}})
  end
  return {grant, master_lease[2]}
end

function get_lease(user, domain, op, quota)
  return request_lease(user, domain, op, quota)[1] > 0
end

return {
  init = init_lease,
  get_lease = get_lease
}
