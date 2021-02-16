package = "lredis"
version = "scm-0"

description = {
  summary = "Redis library for Lua";
  homepage = "https://github.com/Oozlum/lredis";
  license = "MIT/X11";
}

source = {
  url = "git+https://github.com/Oozlum/lredis.git";
}

dependencies = {
  "lua >= 5.1";
  "cqueues >= 20150907";
}

build = {
  type = "builtin";
  modules = {
    ["lredis.commands"] = "lredis/commands.lua";
    ["lredis.cqueues"] = "lredis/cqueues.lua";
    ["lredis.protocol"] = "lredis/protocol.lua";
  };
}
