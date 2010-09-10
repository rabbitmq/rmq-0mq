
import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://127.0.0.1:5560")

while True:
    message = socket.recv()
    print "processing request"
    socket.send("World!")
