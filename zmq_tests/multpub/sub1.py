import json

import zmq

context = zmq.Context.instance()

socket_sub = context.socket(zmq.SUB)
socket_sub.connect("tcp://localhost:11000")
socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")
socket_sub.connect("tcp://localhost:12000")
socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")


socket_req1 = context.socket(zmq.REQ)
socket_req1.connect("tcp://localhost:11001")
socket_req2 = context.socket(zmq.REQ)
socket_req2.connect("tcp://localhost:12001")

socket_req1.send(b"")
socket_req1.recv()
socket_req2.send(b"")
socket_req2.recv()


pills = 0

joins = dict()


def merge_dicts(a, b):
    z = dict(a)
    z.update(b)
    return z


while pills < 2:
    s = socket_sub.recv()
    if s == b"":
        pills += 1
    else:
        msg = json.loads(s.decode())
        if msg["val"] in joins:
            dict_old = joins[msg["val"]]
            dict_merge = merge_dicts(dict_old, msg)
            print(dict_merge)
        else:
            joins[msg["val"]] = msg
