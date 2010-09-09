
import zmq
import time

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://127.0.0.1:5555")

while True:
    socket.send("Hello?")
    time.sleep (1)
    socket.send("World???")
    time.sleep (1)
