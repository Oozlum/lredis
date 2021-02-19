-- Construction of a redis response object.

return {
  STATUS = 'STATUS',
  ERROR = 'ERROR',
  INT = 'INT',
  STRING = 'STRING',
  ARRAY = 'ARRAY',

  new = function(type, data)
    return {
      type = type,
      data = data
    }
  end
}
