import socket

class Node:
    def __init_(self, ipAddr):
        self.ipAddr = ipAddr
        self.predecessor = None
        self.successor = None
        self.key = self.ComputeKey(ipAddr)
        self.fingerTable = self.ComputeFingerTable()
        self.myResources = []

    def ComputeKey(ipAddr):
        return 0
        pass

    # Request for a resource
    def ReqResource(self, filename):
        pass

    # Send the requested resource as response
    def ResResource():
        pass


    def Lookup(self, filename):
        filekey = self.keyOfResource(filename)
        if self.predecessor < filekey and filekey <= self.key:
            return self.key
        
        for l in reversed(self.fingerTable):
            if filekey <= l.key:
                return RPC('Lookup', l.key, filekey)

        return RPC('Lookup', self.fingerTable[0].key, filekey)

        '''
        (successorIp, key) = self.FindSuccessor(filename)
        
        # send request till we find the resource containing machine
        while True:
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            clientSocket.connect((successorIp, 80))
            # .....
            pass
        '''
        
    def ComputeFingerTable():
        pass

    # return the key
    def FindSuccessor(filename):
        pass