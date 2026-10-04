-- Renders the configuration Kong runs: kong.yml, plus the one consumer whose
-- credential is Auth's RS256 public key.
--
-- WHY A RENDER STEP. The jwt plugin finds the key to verify a token with by matching
-- the token's `iss` to a consumer credential, and that credential has to hold the key
-- itself: its rsa_public_key field takes no vault reference (measured, Kong 3.9). The
-- keypair does not exist until compose's setup container has generated it into a
-- volume, so the key cannot be written into kong.yml. This runs as Kong starts, reads
-- the key from the volume every service verifies with, and writes the whole document
-- to /tmp, which is what KONG_DECLARATIVE_CONFIG names.
--
-- It refuses rather than guesses: no key, no issuer, a file that is not a PEM public
-- key, or a kong.yml that already declares consumers all stop Kong from starting.
-- A rotated key reaches Kong when Kong is restarted.
--
-- Run with `resty`, the OpenResty CLI the Kong image ships.

local function fail(msg)
  io.stderr:write("render-consumer: ", msg, "\n")
  os.exit(1)
end

local function read(path)
  local f, err = io.open(path, "r")
  if not f then fail("cannot read " .. path .. ": " .. tostring(err)) end
  local text = f:read("*a")
  f:close()
  return text
end

local template_path = os.getenv("KONG_TEMPLATE") or "/kong/declarative/kong.yml"
local key_path = os.getenv("AUTH_PUBLIC_KEY_FILE") or fail("AUTH_PUBLIC_KEY_FILE is not set")
local issuer = os.getenv("JWT_ISSUER") or fail("JWT_ISSUER is not set")
local out_path = os.getenv("KONG_DECLARATIVE_CONFIG") or fail("KONG_DECLARATIVE_CONFIG is not set")
if out_path == template_path then fail("KONG_DECLARATIVE_CONFIG must not be the template it is rendered from") end

local template = read(template_path)
if template:find("\nconsumers:", 1, true) or template:find("^consumers:") then
  fail(template_path .. " already declares consumers; this step owns them")
end
if not issuer:match("^https?://[%w%.%-:/]+$") then fail("JWT_ISSUER is not a URL: " .. issuer) end

local pem = read(key_path):gsub("\r", "")
if not pem:match("^%-%-%-%-%-BEGIN PUBLIC KEY%-%-%-%-%-\n") or not pem:find("\n%-%-%-%-%-END PUBLIC KEY%-%-%-%-%-") then
  fail(key_path .. " is not a PEM public key")
end
local indented = pem:gsub("\n+$", ""):gsub("\n", "\n            ")

local rendered = template:gsub("\n*$", "\n") .. table.concat({
  "",
  "# Rendered at start by render-consumer.lua from " .. key_path .. ".",
  "consumers:",
  "  - username: eventhub-auth",
  "    jwt_secrets:",
  '      - key: "' .. issuer .. '"',
  "        algorithm: RS256",
  "        rsa_public_key: |",
  "            " .. indented,
  "",
}, "\n")

local f, err = io.open(out_path, "w")
if not f then fail("cannot write " .. out_path .. ": " .. tostring(err)) end
f:write(rendered)
f:close()
io.stdout:write("render-consumer: wrote ", out_path, " with the eventhub-auth consumer for ", issuer, "\n")
