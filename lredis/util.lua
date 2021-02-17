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

return {
  transform_variadic_args_to_tables = transform_variadic_args_to_tables,
}
