-- Redis command handler
local util = require'lredis.util'

--[[ Base command handler --]]

local function null_postcall_hook(client, new_options, args, ...)
  return ...
end

-- the base handler creates an index function which transforms client.foo into
-- client.call('foo', ...), caching all created functions.
local function new_base_handler(client, handler)
  local cache = {}

  -- default some handler properties
  handler.blacklist = handler.blacklist or {}
  handler.precall = handler.precall or {}
  handler.postcall = handler.postcall or {}

  -- override the client call() function to call the handler's pre- and post-call
  -- hooks and to inject the handler-specific white- and blacklists.
  local pcall = client.pcall
  client.pcall = function(client, ...)
    local options, args = util.transform_variadic_args_to_tables(...)

    -- duplicate the options table so we can add things without upsetting the caller
    -- then inject the handler white and black lists.
    local new_options = {}
    for k,v in pairs(options) do
      new_options[k] = v
    end
    new_options.blacklist = new_options.blacklist or {}

    -- call the pre-call hook if present.
    local cmd = args[1]
    if handler.precall[cmd] then
      local ok, err_type, err_msg = handler.precall[cmd](client, new_options, args)
      if not ok then
        return nil, err_type, err_msg
      end
    end

    -- apply the white and blacklists.
    if handler.whitelist then
      new_options.whitelist = new_options.whitelist or {}
      for _, cmd in ipairs(handler.whitelist) do
        table.insert(new_options.whitelist, cmd)
      end
    end
    for _, cmd in ipairs(handler.blacklist) do
      table.insert(new_options.blacklist, cmd)
    end

    -- call the post-call hook with the results of the actual call
    handler.postcall[cmd] = handler.postcall[cmd] or null_postcall_hook
    return handler.postcall[cmd](client, new_options, args, pcall(client, new_options, args))
  end

  return setmetatable(client, {
    __index = function(t, k)
      -- ignore non-string keys, otherwise ipairs will go loopy.
      if type(k) ~= 'string' then
        return nil
      end

      if not cache[k] then
        cache[k] = function(client, ...)
          local options, args = util.transform_variadic_args_to_tables(...)
          table.insert(args, 1, k)
          return client:call(options, args)
        end
      end
      return cache[k]
    end
  })
end

local function new_handler(client)
  return new_base_handler(client, {})
end

return {
  new_handler = new_handler,
}
