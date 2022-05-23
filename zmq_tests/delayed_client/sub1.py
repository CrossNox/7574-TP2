import zmq

context = zmq.Context.instance()

socket_sub = context.socket(zmq.SUB)
socket_sub.connect("tcp://localhost:11000")
socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")

socket_req = context.socket(zmq.REQ)
socket_req.connect("tcp://localhost:11001")

socket_result = context.socket(zmq.PUB)
socket_result.bind("tcp://*:11002")

socket_resultsync = context.socket(zmq.REP)
socket_resultsync.bind("tcp://*:11003")

socket_req.send(b"")
socket_req.recv()

suma = 0
while True:
    s = socket_sub.recv()
    if s == b"":
        break
    suma += int(s.decode())

print(f"suma @ sub1: {suma}")

socket_resultsync.recv()
socket_resultsync.send(b"")

socket_result.send_string(f"{suma}")
