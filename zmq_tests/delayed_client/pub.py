import time

import zmq

context = zmq.Context.instance()

socket_pub = context.socket(zmq.PUB)
socket_pub.bind("tcp://*:11000")

socket_rep = context.socket(zmq.REP)
socket_rep.bind("tcp://*:11001")

subs = 0
while subs < 2:
    socket_rep.recv()
    socket_rep.send(b"")
    subs += 1


for i in range(100):
    msg = f"{i}"
    socket_pub.send_string(msg)
    time.sleep(0.001)

socket_pub.send(b"")
