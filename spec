now I would like you to create a new project called "clutchpedal" or "clutch-pedal" or
  "clutch_pedal" (whatever the idiomatic way is to name this kind of thing is in golang)

This project will be a lightweight CLI utility around the clutch library we just made that automatically converts any ws endpoint containing an id field into an http server. Something like:


clutch localhost 8000 --upstream ws://localhost:1080 --path /ws

Where:
1. argument #1 is host to bind an http server to 
2. argument #2 is port to bind server to (both 1 & 2 are identical to netcat syntax)
3. --upstream is the uri of the websocket server that serves as the "engine"
4. --path is path on host to mount ws server (optional field, may not even be necessary at all)

This then allows for:

echo "<some json with id field>" \
| curl --json @- http://localhost:8000/ws \
| jq '.<some json field>'

Alternatively, if one does not specify --upstream, clutch pedal should read json messages (newline as rec. sep.) from stdio (this should be trivial to implement given our new interface). For example (zsh):

coproc { websocat --listen ws://localhost:8000 }

clutch localhost 8000 <&p >&p

Overall thoughts?
