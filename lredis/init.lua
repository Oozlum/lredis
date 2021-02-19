-- Redis command handler
local cqueues = require'cqueues'
cqueues.condition = require'cqueues.condition'
local redis = require'lredis.redis'
local util = require'lredis.util'
local response = require'lredis.response'

--[[ Base command handler --]]

-- return a handler object with an index function which transforms handler.foo into
-- handler.call('foo', ...), caching all created functions.
local function new_handler(redis_client, handler_spec)
  local cache = {}

  -- default some handler properties
  handler_spec = handler_spec or {}
  handler_spec.blacklist = handler_spec.blacklist or {}
  handler_spec.precall = handler_spec.precall or {}
  handler_spec.postcall = handler_spec.postcall or {}

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

      -- create a state object for use by the hooks.
      local state = {}

      -- call the pre-call hook if present.
      local cmd = tostring(args[1] or ''):upper()
      local resp, err_type, err_msg = true, nil, nil
      if handler_spec.precall['*'] then
        resp, err_type, err_msg = handler_spec.precall['*'](handler, state, new_options, args)
      end
      if resp and handler_spec.precall[cmd] then
        resp, err_type, err_msg = handler_spec.precall[cmd](handler, state, new_options, args)
      end
      if not resp then
        return nil, err_type, err_msg
      end

      -- apply the white and blacklists.
      if handler_spec.whitelist then
        new_options.whitelist = new_options.whitelist or {}
        for _, cmd in ipairs(handler_spec.whitelist) do
          table.insert(new_options.whitelist, cmd)
        end
      end
      for _, cmd in ipairs(handler_spec.blacklist) do
        table.insert(new_options.blacklist, cmd)
      end

      -- call the post-call hook with the results of the actual call
      local resp, err_type, err_msg = handler.attrs.redis_client:pcall(new_options, args)
      if handler_spec.postcall[cmd] then
        resp, err_type, err_msg = handler_spec.postcall[cmd](handler, state, new_options, args, resp, err_type, err_msg)
      end
      if handler_spec.postcall['*'] then
        resp, err_type, err_msg = handler_spec.postcall['*'](handler, state, new_options, args, resp, err_type, err_msg)
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
    __index = function(t, cmd)
      -- ignore non-string keys, otherwise ipairs will go loopy.
      if type(cmd) ~= 'string' then
        return nil
      end

      if not cache[cmd] then
        cache[cmd] = function(handler, ...)
          local options, args = util.transform_variadic_args_to_tables(cmd, ...)
          return handler:call(options, args)
        end
      end
      return cache[cmd]
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

local transaction_handler_spec = {
  blacklist = {
    'MULTI'
  },
  precall = {
    ['*'] = function(xact, state, options, args)
      if not xact.attrs.in_transaction then
        return nil, 'USAGE', 'transaction no longer valid'
      end
      -- override the response creator so we can reliably detect errors.
      local orig_response_creator = options.response_creator or
        xact.attrs.redis_client.response_creator or
        response.new or
        function (resp) return resp end

      options.response_creator = function(type, data)
        if state.error == nil then
          state.error = (type == response.ERROR)
        end
        return orig_response_creator(type, data)
      end
      return true
    end
  },
  postcall = {
    -- cancel the transaction after DISCARD, EXEC and RESET
    DISCARD = function(xact, state, options, args, resp, err_type, err_msg)
      cancel_transaction(xact)
      return resp, err_type, err_msg
    end,
    EXEC = function(xact, state, options, args, resp, err_type, err_msg)
      cancel_transaction(xact)
      return resp, err_type, err_msg
    end,
    RESET = function(xact, state, options, args, resp, err_type, err_msg)
      cancel_transaction(xact)
      return resp, err_type, err_msg
    end,
    -- cancel the transaction on any error.
    ['*'] = function(xact, state, options, args, resp, err_type, err_msg)
      if not resp or state.error then
        cancel_transaction(xact)
      end
      return resp, err_type, err_msg
    end,
  },
}

-- set up the basic operations of a command handler.
local command_handler_spec = {
  blacklist = {
    'EXEC',
    'DISCARD'
  },
  precall = {
    ['*'] = function(handler, state, options, args)
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
    MULTI = function(handler, state, options, args)
      if handler.attrs.transaction_lock then
        return nil, 'USAGE', 'Unexpected transaction state'
      end
      handler.attrs.transaction_lock = cqueues.condition.new()
      return true
    end,
  },
  postcall = {
    -- if the MULTI command finished successfully create and return a transaction
    -- handler.  Otherwise, remove the transaction lock and return an error.
    MULTI = function(handler, state, options, args, resp, err_type, err_msg)
      if not resp or resp.type == response.ERROR then
        cancel_transaction{
          attrs = {
            parent = handler
          }
        }
      end

      if not resp then
        return nil, err_type, err_msg
      elseif resp.type == response.ERROR then
        return nil, 'USAGE', resp.data
      end

      local transaction = new_handler(handler.attrs.redis_client, transaction_handler_spec)
      transaction.attrs.parent = handler
      transaction.attrs.in_transaction = true
      return transaction
    end
  },
}

return {
  new = function(redis_client)
    return new_handler(redis_client, command_handler_spec)
  end,
  connect = function(host, port, error_handler)
    local redis_client, err_type, err_msg = redis.connect_tcp(host, port, error_handler)
    if not redis_client then
      return nil, err_type, err_msg
    end
    return new_handler(redis_client, command_handler_spec)
  end,
}
