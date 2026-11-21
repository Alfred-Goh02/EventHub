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
-- THE DEPLOYMENT'S OWN DOMAIN, the other thing a file cannot know. kong.yml serves two:
-- eventhub.localhost, which every laptop resolves to itself, and eventhub.com, which
-- stands for the deployment's own. Given APP_URL_TEMPLATE -- the address Auth and
-- Notification build their links on, https://{tenant}.example.org -- this puts that
-- scheme, domain and port in eventhub.com's four places: the tenant hosts, the api host,
-- the CORS origin and the tenant rule. Unset, or naming eventhub.localhost, the file is
-- rendered as it is: compose sets neither, and infra/eks/public.mjs sets it on EKS.
--
-- Each place is an exact string that must occur exactly once, so a kong.yml that has moved
-- on stops Kong starting instead of leaving a place on a domain nobody owns; and after the
-- replacement, no line that is not a comment may still name eventhub.com. The domain is
-- held to lower-case DNS labels before it is written anywhere: it lands in a YAML string,
-- a regular expression and a Lua pattern.
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

-- The one occurrence of `old` replaced by `new`, plainly: no pattern, so nothing in either
-- is read as one. Refuses when `old` is missing or there more than once.
local function replace_once(text, old, new, what)
  local s, e = text:find(old, 1, true)
  if not s then fail("kong.yml has no " .. what .. " to put the domain in: expected " .. old) end
  if text:find(old, e + 1, true) then fail("kong.yml has more than one " .. what .. ": " .. old) end
  return text:sub(1, s - 1) .. new .. text:sub(e + 1)
end

local DEV_DOMAIN = "eventhub.localhost"
local domain_note = "eventhub.com, as written"
local app_url = os.getenv("APP_URL_TEMPLATE")
if app_url and app_url ~= "" then
  local scheme, domain, port = app_url:match("^(https?)://{tenant}%.([a-z0-9%.%-]+):?([0-9]*)/?$")
  if not scheme then fail("APP_URL_TEMPLATE is not http(s)://{tenant}.<domain>[:port]: " .. app_url) end
  local labels = 0
  for label in (domain .. "."):gmatch("([^.]*)%.") do
    if #label == 0 or #label > 63 or not (label:match("^[a-z0-9]$") or label:match("^[a-z0-9][a-z0-9%-]*[a-z0-9]$")) then
      fail("APP_URL_TEMPLATE's domain is not lower-case DNS labels: " .. domain)
    end
    labels = labels + 1
  end
  if labels < 2 then fail("APP_URL_TEMPLATE's domain has one label; a tenant needs a subdomain of two: " .. domain) end
  if port ~= "" and (port:sub(1, 1) == "0" or tonumber(port) > 65535) then fail("APP_URL_TEMPLATE's port is not a port: " .. port) end

  if domain ~= DEV_DOMAIN then
    local as_regex = (domain:gsub("%.", "\\."))
    local as_lua = (domain:gsub("[%.%-]", "%%%0"))
    local origin_port = port ~= "" and (":" .. port) or ""
    template = replace_once(template, '"*.eventhub.com"', '"*.' .. domain .. '"', "tenant host")
    template = replace_once(template, '"api.eventhub.com"', '"api.' .. domain .. '"', "api host")
    template = replace_once(template, "'https://[a-z0-9][a-z0-9-]*\\.eventhub\\.com'",
      "'" .. scheme .. "://[a-z0-9][a-z0-9-]*\\." .. as_regex .. origin_port .. "'", "CORS origin")
    template = replace_once(template, 'host:match("^([a-z0-9][a-z0-9%-]*)%.eventhub%.com$")',
      'host:match("^([a-z0-9][a-z0-9%-]*)%.' .. as_lua .. '$")', "tenant rule")
    for line in template:gmatch("[^\n]+") do
      if not line:match("^%s*#") and (line:find("eventhub.com", 1, true) or line:find("eventhub\\.com", 1, true) or line:find("eventhub%.com", 1, true)) then
        fail("kong.yml still names eventhub.com after the domain was put in: " .. line)
      end
    end
    domain_note = domain .. " over " .. scheme .. origin_port
  else
    domain_note = "eventhub.com, as written: APP_URL_TEMPLATE names " .. DEV_DOMAIN .. ", which kong.yml always serves"
  end
end

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
io.stdout:write("render-consumer: wrote ", out_path, " with the eventhub-auth consumer for ", issuer, ", serving ", domain_note, "\n")
