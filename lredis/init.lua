-- Redis command handler
local cqueues = require'cqueues'
local cond = require'cqueues.condition'
local redis = require'lredis.redis'
local util = require'lredis.util'
local protocol = require'lredis.protocol'

--[[ Base command handler --]]

-- return a handler object with an index function which transforms handler.foo into
-- handler.call('foo', ...), caching all created functions.
local function new_handler(redis_client, handler_attrs)
  local cache = {}

  -- default some handler properties
  handler_attrs = handler_attrs or {}
  handler_attrs.blacklist = handler_attrs.blacklist or {}
  handler_attrs.precall = handler_attrs.precall or {}
  handler_attrs.postcall = handler_attrs.postcall or {}

  local handler = {
    attrs = {
      redis_client = redis_client,
    },
    close = function(handler)
      if handler.attrs.redis_client then
        handler.attrs.redis_client:close()
      end
    end,
    call = function(handler, ...)
      if not handler.attrs.redis_client then
        return nil, 'USAGE', 'invalid redis client.  Was it closed?'
      end

      local options, args = util.transform_variadic_args_to_tables(...)

      -- duplicate the options table so we can add things without upsetting the caller
      -- then inject the handler white and black lists.
      local new_options = util.deep_copy(options)
      new_options.blacklist = new_options.blacklist or {}

      -- call the pre-call hook if present.
      local cmd = tostring(args[1] or ''):upper()
      local resp, err_type, err_msg = true, nil, nil
      if handler_attrs.precall['*'] then
        resp, err_type, err_msg = handler_attrs.precall['*'](handler, new_options, args)
      end
      if resp and handler_attrs.precall[cmd] then
        resp, err_type, err_msg = handler_attrs.precall[cmd](handler, new_options, args)
      end
      if not resp then
        return nil, err_type, err_msg
      end

      -- apply the white and blacklists.
      if handler_attrs.whitelist then
        new_options.whitelist = new_options.whitelist or {}
        for _, cmd in ipairs(handler_attrs.whitelist) do
          table.insert(new_options.whitelist, cmd)
        end
      end
      for _, cmd in ipairs(handler_attrs.blacklist) do
        table.insert(new_options.blacklist, cmd)
      end

      -- call the post-call hook with the results of the actual call
      local resp, err_type, err_msg = handler.attrs.redis_client:pcall(new_options, args)
      if handler_attrs.postcall[cmd] then
        resp, err_type, err_msg = handler_attrs.postcall[cmd](handler, new_options, args, resp, err_type, err_msg)
      end
      if handler_attrs.postcall['*'] then
        resp, err_type, err_msg = handler_attrs.postcall['*'](handler, new_options, args, resp, err_type, err_msg)
      end

      -- handle any errors and return.
      if not resp then
        local error_handler = new_options.error_handler or handler.attrs.redis_client.error_handler
        return error_handler(err_type, err_msg)
      end

      return resp
    end
  }

  return setmetatable(handler, {
    __index = function(t, k)
      -- ignore non-string keys, otherwise ipairs will go loopy.
      if type(k) ~= 'string' then
        return nil
      end

      if not cache[k] then
        cache[k] = function(handler, ...)
          local options, args = util.transform_variadic_args_to_tables(...)
          table.insert(args, 1, k)
          return handler:call(options, args)
        end
      end
      return cache[k]
    end
  })
end

-- set up a transaction handler.
local function cancel_transaction(xact)
  if xact.attrs.parent.attrs.transaction_lock then
    xact.attrs.parent.attrs.transaction_lock:signal()
    xact.attrs.parent.attrs.transaction_lock = nil
  end
  xact.attrs.in_transaction = nil
end

local transaction_handler_attrs = {
  blacklist = {
    'MULTI'
  },
  precall = {
    ['*'] = function(xact, options, args)
      if not xact.attrs.in_transaction then
        return nil, 'USAGE', 'transaction no longer valid'
      end
      return true
    end
  },
  postcall = {
    -- cancel the transaction on any error.
    ['*'] = function(xact, options, args, resp, err_type, err_msg)
      if not resp or resp.type == protocol.ERROR then
        cancel_transaction(xact)
      end
      return resp, err_type, err_msg
    end,
    -- cancel the transaction after DISCARD, EXEC and RESET
    DISCARD = function(xact, options, args, resp, err_type, err_msg)
      cancel_transaction(xact)
      return resp, err_type, err_msg
    end,
    EXEC = function(xact, options, args, resp, err_type, err_msg)
      cancel_transaction(xact)
      return resp, err_type, err_msg
    end,
    RESET = function(xact, options, args, resp, err_type, err_msg)
      cancel_transaction(xact)
      return resp, err_type, err_msg
    end,
  },
}

-- set up the basic operations of a command handler.
local command_handler_attrs = {
  blacklist = {
    'EXEC',
    'DISCARD'
  },
  precall = {
    ['*'] = function(handler, options, args)
      if handler.attrs.transaction_lock and not cqueues.running() then
        return nil, 'USAGE', 'Transaction in progress and cannot wait -- not in a coroutine'
      end
      while handler.attrs.transaction_lock do
        handler.attrs.transaction_lock:wait()
      end
      return true
    end,
    -- create a transaction lock now to prevent other coroutines from pipelining
    -- commands while MULTI is pending.
    MULTI = function(handler, options, args)
      if handler.attrs.transaction_lock then
        return nil, 'USAGE', 'Unexpected transaction state'
      end
      handler.attrs.transaction_lock = cond.new()
      return true
    end,
  },
  postcall = {
    -- if the MULTI command finished successfully create and return a transaction
    -- handler.  Otherwise, remove the transaction lock and return an error.
    MULTI = function(handler, options, args, resp, err_type, err_msg)
      if not resp or resp.type == protocol.ERROR then
        cancel_transaction{
          attrs = {
            parent = handler
          }
        }
      end

      if not resp then
        return nil, err_type, err_msg
      elseif resp.type == protocol.ERROR then
        return nil, 'USAGE', resp.data
      end

      local transaction = new_handler(handler.attrs.redis_client, transaction_handler_attrs)
      transaction.attrs.parent = handler
      transaction.attrs.in_transaction = true
      return transaction
    end
  },
}

return {
  new = function(redis_client)
    return new_handler(redis_client, command_handler_attrs)
  end,
  connect = function(host, port, error_handler)
    local redis_client, err_type, err_msg = redis.connect_tcp(host, port, error_handler)
    if not redis_client then
      return nil, err_type, err_msg
    end
    return new_handler(redis_client, command_handler_attrs)
  end,
}
