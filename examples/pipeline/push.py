
import zmq
import time

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect("tcp://127.0.0.1:5557")

while True:
    socket.send("Work!")
    time.sleep (1)
