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
  if #arg == 0 then
    return nil, 'USAGE', 'No arguments given'
  end

  -- convert the argument array into a RESP array of bulk strings.
  local str_arg = {
    ('*%d\r\n'):format(#arg)
  }
  for _,v in ipairs(arg) do
    v = tostring(v)
    table.insert(str_arg, ('$%d\r\n%s\r\n'):format(#v, v))
  end
  str_arg = table.concat(str_arg)

  -- send the string to the server.
  local ok, err_type, err_msg = file:write(str_arg)
  if not ok then
    return nil, err_type, err_msg
  end
  ok, err_type, err_msg = file:flush()
  if not ok then
    return nil, err_type, err_msg
  end

  return true
end

-- Parse a redis response
local function read_response(file, response_formatter)
  response_formatter = response_formatter or function(resp)
    return resp
  end

  local line, err_type, err_msg = file:read('*L')
  if not line then
    return nil, err_type or 'SOCKET', err_msg or 'EOF'
  end

  -- split the string into its component parts and validate.
  local data_type, data, ending = line:sub(1, 1), line:sub(2, -2), line:sub(-2)
  local int_data = tonumber(data, 10)

  if ending ~= '\r\n' then
    return nil, 'PROTOCOL', 'invalid line ending'
  end

  if data_type == '+' then
    return response_formatter{ type = data_types.STATUS, data = data }
  elseif data_type == '-' then
    return response_formatter{ type = data_types.ERROR, data = data }
  elseif data_type == ':' and int_data then
    return response_formatter{ type = data_types.INT, data = int_data }
  elseif data_type == '$' and int_data == -1 then
    return response_formatter{ type = data_types.STRING }
  elseif data_type == '$' and int_data >= 0 and int_data <= 512*1024*1024 then
    line, err_type, err_msg = file:read(int_data + 2)
    if not line then
      return nil, err_type, err_msg
    end
    data, ending = line:sub(1, -2), line:sub(-2)
    if ending ~= '\r\n' then
      return nil, 'PROTOCOL', 'invalid line ending'
    end
    return response_formatter{ type = data_types.STRING, data = data }
  elseif data_type == '*' and int_data == -1 then
    return response_formatter{ type = data_types.ARRAY }
  elseif data_type == '*' and int_data >= 0 then
    local array = { type = data_types.ARRAY, data = {} }
    for i = 1, int_data do
      array.data[i], err_type, err_msg = read_response(file, response_formatter)
      if not array.data[i] then
        return nil, err_type, err_msg
      end
    end
    return array
  end

  return nil, 'PROTOCOL', 'invalid response'
end

return setmetatable({
  send_command = send_command,
  read_response = read_response,
},
{
  __index = data_types
})
