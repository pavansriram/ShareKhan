import socket
import pickle

def RPC(func_name, destIpAddr, destPort, *args):
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.connect((destIpAddr, destPort))

    msgList = (func_name, args)     # message payload
    msgSend = pickle.dumps(msgList) # wrap call

    clientSocket.send(msgSend)      # send request to server
    msgRecv = clientSocket.recv(1024)
    retVal = pickle.loads(msgRecv)

    clientSocket.close()
    return retVal