-- general utility functions.

-- transform a variadic argument list into a two-table argument list as follows:
-- () => ({},{})
-- ({a}) => ({a},{})
-- (x,y,z,...) => ({},{x,y,z,...})
-- ({a},x,y,z,...) => ({a},{x,y,z,...})
-- ({a},{b},...) => ({a},{b})
local function transform_variadic_args_to_tables(...)
  local options
  local args = {...}
  if type(args[1]) == 'table' then
    options = table.remove(args, 1)
  end
  if type(args[1]) == 'table' then
    args = args[1]
  end
  if #args == 0 then
    args = options
    options = nil
  end
  options = options or {}
  args = args or {}

  return options, args
end

-- simple non-recursive deep-copy function
local function deep_copy(t)
  local cache = { [t] = {} }
  local copy = { { src = t, dst = cache[t] } }

  local function duplicate(v)
    if type(v) == 'table' then
      if not cache[v] then
        cache[v] = {}
        table.insert(copy, { src = v, dst = cache[v] })
      end
      return cache[v]
    end
    return v
  end

  while copy[1] do
    local op = table.remove(copy, 1)
    for k,v in pairs(op.src) do
      op.dst[duplicate(k)] = duplicate(v)
    end
  end

  return cache[t]
end

return {
  transform_variadic_args_to_tables = transform_variadic_args_to_tables,
  deep_copy = deep_copy,
}
