
import zmq

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.bind("tcp://127.0.0.1:5555")
socket.setsockopt (zmq.SUBSCRIBE, "")

while True:
    print socket.recv()
