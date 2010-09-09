
import zmq
import time

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://127.0.0.1:5555")

while True:
    socket.send("Hello, world???")
    time.sleep (1)

