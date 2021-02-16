-- Documentation on the redis protocol found at http://redis.io/topics/protocol

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
local function read_response(file, new_status, new_error, string_null, array_null)
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
		return new_status(data)
	elseif data_type == "-" then
		return new_error(data)
	elseif data_type == ":" and int_data then
		return int_data
	elseif data_type == "$" and int_data then
		if int_data == -1 then
			return string_null
		elseif int_data > 512*1024*1024 then -- max 512 MB
			return nil, "bulk string too large"
		else
			local str, err_code = file:read(int_data)
			if not str then
				return str, err_code
			end
			-- should be followed by CRLF
			local crlf, err_code = file:read(2)
			if not crlf or crlf ~= "\r\n" then
				return nil, err_code or "invalid bulk reply"
			end
			return str
		end
	elseif data_type == "*" and int_data then
		if int_data == -1 then
			return array_null
		else
			local arr, null = {}, {}
			for i=1, int_data do
				local resp, err_code = read_response(file, new_status, new_error, null, array_null)
				if not resp then
					return nil, err_code
				end
				arr[i] = (resp ~= null) and resp or string_null
			end
			return arr
		end
	else
		return nil, "protocol error"
	end
end

-- The way lua embedded into redis encodes things:
local function error_reply(message)
	return {err = message}
end
local function status_reply(message)
	return {ok = message}
end
local function default_read_response(file)
	return read_response(file, status_reply, error_reply, false, false)
end

return {
	send_command = send_command;
	read_response = read_response;

	error_reply = error_reply;
	status_reply = status_reply;
	string_null = false;
	array_null = false;
	default_read_response = default_read_response;
}
