local protocol = require 'lredis.protocol'
local response = require 'lredis.response'
local util = require 'lredis.util'
local cqueues = require'cqueues'
cqueues.socket = require'cqueues.socket'
cqueues.condition = require'cqueues.condition'
cqueues.errno = require'cqueues.errno'

local M = {}

-- find the appropriate error handler and call it, returning the result.
local function handle_error(client, error_handler, err_type, err_msg)
  error_handler = error_handler or (client or {}).error_handler or M.error_handler
  if error_handler then
    return error_handler(err_type, err_msg)
  end

  return nil, err_type, err_msg
end

local function close_client(client)
  client.socket:close()
  client.socket = false
end

-- if a whitelist is given, only accept commands in the whitelist.
-- if a blacklist is given, reject commands that are in it.
local function validate_command(options, command)
  if options.whitelist then
    for _,v in ipairs(options.whitelist) do
      if tostring(v):upper() == command then return true end
    end
    return false
  end

  for _,v in ipairs(options.blacklist or {}) do
    if tostring(v):upper() == command then return false end
  end

  return true
end

-- call the redis function and return the response.
-- return nil, err_type, err_msg on error.
local function redis_pcall(client, ...)
  local options, args = util.transform_variadic_args_to_tables(...)

  if #args == 0 then
    return nil, 'USAGE', 'no arguments given'
  end

  args[1] = tostring(args[1]):upper()
  if not validate_command(options, args[1]) then
    return nil, 'USAGE', ('command "%s" not permitted at this time'):format(args[1])
  end

  if client.socket == false then
    return nil, 'USAGE', 'socket already closed'
  elseif not client.socket then
    return nil, 'USAGE', 'invalid socket'
  end

  local cond = cqueues.condition.new()
  local ok, err_type, err_msg = protocol.send_command(client.socket, args)
  if not ok then
    return nil, err_type, err_msg
  end
  table.insert(client.fifo, cond)
  if client.fifo[1] ~= cond then
    cond:wait()
  end
  -- read the response, signal the next command in the queue and then deal with errors.
  local resp, err_type, err_msg = protocol.read_response(client.socket)
  table.remove(client.fifo, 1)
  if client.fifo[1] then
    client.fifo[1]:signal()
  end

  if resp then
    local renderer = options.response_renderer or client.response_renderer or M.response_renderer or response.new
    local cmd = table.remove(args, 1)

    return renderer(cmd, options, args, resp.type, resp.data)
  end

  return nil, err_type, err_msg
end

-- call the redis function and return the response.
-- call the registered error handler on error and return the results.
local function redis_call(client, ...)
  local options, args = util.transform_variadic_args_to_tables(...)

  local resp, err_type, err_msg = client:pcall(options, args)
  if not resp then
    return handle_error(client, options.error_handler, err_type, err_msg)
  end

  return resp
end

--[[ Static module functions --]]

-- override the default socket handler so that it returns all errors rather than
-- throwing them.
local function socket_error_handler(socket, method, code, level)
  return 'SOCKET', ('%s %s'):format(cqueues.errno[code] or '', cqueues.errno.strerror(code)), level
end

local function new(socket, error_handler)
  socket:onerror(socket_error_handler)
  socket:setmode('b', 'b')
  socket:setvbuf('full', math.huge) -- 'infinite' buffering; no write locks needed
  return {
    socket = socket,
    error_handler = error_handler,
    fifo = {},
    close = close_client,
    pcall = redis_pcall,
    call = redis_call
  }
end

local function connect_tcp(host, port, error_handler)
  -- override the global CQueues error handler briefly, until we have a socket.
  local old_error_handler = cqueues.socket.onerror(socket_error_handler)
  local socket, err_type, err_msg = cqueues.socket.connect({
    host = host or '127.0.0.1',
    port = port or '6379',
    nodelay = true,
  })
  cqueues.socket.onerror(old_error_handler)

  if not socket then return handle_error(nil, error_handler, err_type, err_msg) end

  socket:onerror(socket_error_handler)
  local ok, err_type, err_msg = socket:connect()
  if not ok then return handle_error(nil, error_handler, err_type, err_msg) end

  return new(socket, error_handler)
end

M.new = new
M.connect = connect_tcp
M.error_handler = function(err_type, err_msg)
  error(('%s %s'):format(tostring(err_type), tostring(err_msg)), 2)
end

-- need locking around sending subscribe, as you won't know
function start_subscription_modet(arg)
  if self.in_transaction then -- in a transaction
    -- read off "QUEUED"
    local resp = self:pcallt(arg)
  else
    local ok, err_code = protocol.send_command(self.socket, arg)
    if not ok then
      return M.error_handler(('send_command error: %s'):format(tostring(err_code)))
    end
  end
  self.subscribes_pending = self.subscribes_pending + 1
end

function start_subscription_mode(...)
  return self:start_subscription_modet(pack(...))
end

return M
