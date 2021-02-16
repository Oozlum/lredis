-- Documentation on the redis protocol found at http://redis.io/topics/protocol
local data_types = {
  STATUS = 'STATUS',
  ERROR = 'ERROR',
  INT = 'INT',
  STRING = 'STRING',
  ARRAY = 'ARRAY',
}

-- Encode and send a redis command.
local function send_command(file, arg)
  local n = arg.n or #arg
  if n == 0 then
    return nil, "need at least one argument"
  end

  -- convert the argument array into a RESP array of bulk strings.
  local str_arg = {
    [0] = string.format("*%d\r\n", n);
  }
  for i=1, n do
    str_arg[i] = string.format("$%d\r\n%s\r\n", #arg[i], arg[i])
  end
  str_arg = table.concat(str_arg, nil, 0, n)

  -- send the string to the server.
  local ok, err_code = file:write(str_arg)
  if not ok then
    return nil, err_code
  end
  ok, err_code = file:flush()
  if not ok then
    return nil, err_code
  end

  return true
end

-- Parse a redis response
local function read_response(file)
  local line, err_code = file:read("*L")
  if not line then
    return nil, err_code or "EOF reached"
  end

  -- split the string into its component parts and validate.
  local data_type, data, ending = line:sub(1, 1), line:sub(2, -2), line:sub(-2)
  local int_data = tonumber(data, 10)

  if ending ~= "\r\n" then
    return nil, "invalid line ending"
  end

  if data_type == "+" then
    return { type = data_types.STATUS, data = data }
  elseif data_type == "-" then
    return { type = data_types.ERROR, data = data }
  elseif data_type == ":" and int_data then
    return { type = data_types.INT, data = int_data }
  elseif data_type == "$" and int_data == -1 then
    return { type = data_types.STRING }
  elseif data_type == "$" and int_data >= 0 and int_data <= 512*1024*1024 then
    line, err_code = file:read(int_data + 2)
    if not line then
      return nil, err_code
    end
    data, ending = line:sub(1, -2), line:sub(-2)
    if ending ~= "\r\n" then
      return nil, err_code or "invalid bulk reply"
    end
    return { type = data_types.STRING, data = data }
  elseif data_type == "*" and int_data == -1 then
    return { type = data_types.ARRAY }
  elseif data_type == "*" and int_data >= 0 then
    local array = { type = data_types.ARRAY, data = {} }
    for i = 1, int_data do
      array.data[i], err_code = read_response(file)
      if not array.data[i] then
        return nil, err_code
      end
    end
    return array
  end

  return nil, "protocol error"
end

return setmetatable({
  send_command = send_command,
  read_response = read_response,
},
{
  __index = data_types
})
