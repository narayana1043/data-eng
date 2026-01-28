import json, subprocess, sys, os

p = subprocess.Popen([sys.executable, "server.py"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def send(m):
    p.stdin.write(json.dumps(m,separators=(",",":"))+"\n")
    p.stdin.flush()

def recv():
    while True:
        line = p.stdout.readline()
        if line == "": raise SystemExit("EOF from server")
        line=line.strip()
        if line: return json.loads(line)

send({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"cli","version":"0.1"}}})
print("init:", recv())

send({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_tables","arguments":{"schema":"public"}}})
print("tables:", recv())