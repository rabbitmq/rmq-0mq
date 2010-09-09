
import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:5559")

socket.send("Hello!")
rep = socket.recv()
print rep
