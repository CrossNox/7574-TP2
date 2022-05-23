import zmq

context = zmq.Context.instance()

socket_sub = context.socket(zmq.SUB)
socket_sub.connect("tcp://localhost:11000")
socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")

socket_req = context.socket(zmq.REQ)
socket_req.connect("tcp://localhost:11001")

socket_sub_res = context.socket(zmq.SUB)
socket_sub_res.connect("tcp://localhost:11002")
socket_sub_res.setsockopt_string(zmq.SUBSCRIBE, "")

socket_req_res = context.socket(zmq.REQ)
socket_req_res.connect("tcp://localhost:11003")


socket_req.send(b"")
socket_req.recv()

socket_req_res.send(b"")
socket_req_res.recv()

suma_sub1 = int(socket_sub_res.recv_string())


suma = 0
while True:
    s = socket_sub.recv()
    if s == b"":
        break
    suma += int(s.decode())

print(f"results match: {suma == suma_sub1}")
