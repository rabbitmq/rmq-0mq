
import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:5559")

print "sending Hello"
socket.send("Hello")
rep = socket.recv()
print "received", rep
