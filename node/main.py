import socket

class Node:
    def __init_(self, ipAddr):
        self.ipAddr = ipAddr
        self.key = self.ComputeKey(ipAddr)
        self.fingerTable = self.ComputeFingerTable()
        self.myResources = []

    def ComputeKey(ipAddr):
        pass

    # Request for a resource
    def ReqResource(self, filename):
        pass

    # Send the requested resource as response
    def ResResource():
        pass

    def Lookup(self, filename):
        (successorIp, key) = self.FindSuccessor(filename)
        
        # send request till we find the resource containing machine
        while True:
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            clientSocket.connect((successorIp, 80))
            # .....
            pass
    def ComputeFingerTable():
        pass

    # return the key
    def FindSuccessor(filename):
        pass