import json
import time

import zmq

context = zmq.Context.instance()

socket_pub = context.socket(zmq.PUB)
socket_pub.bind("tcp://*:12000")

socket_rep = context.socket(zmq.REP)
socket_rep.bind("tcp://*:12001")


socket_rep.recv()
socket_rep.send(b"")


for i in range(50, 150):
    msg = {"pub_2": i, "val": i}
    socket_pub.send_string(json.dumps(msg))

    time.sleep(0.01)

socket_pub.send(b"")
