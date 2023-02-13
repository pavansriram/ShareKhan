import socket
from hash import keyOfResource
import pickle
import os

class Node:
    def __init_(self, ipAddr):
        self.ipAddr = ipAddr
        self.predecessor = None
        self.successor = None
        self.key = self.ComputeKey(ipAddr)
        self.fingerTable = self.ComputeFingerTable()
        self.myResources = []
        self.m = 3
        self.totalNodes = 8
        self.maxFileSize = 4096

    def ComputeKey(ipAddr):
        return 0
        pass

    # Request for a resource
    def ReqResource(self, filename):
        destIpAddr, destPort = self.Lookup(filename)

        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((destIpAddr, destPort))

        clientSocket.send(pickle.dumps('ResourceRequest', filename))
        data = pickle.loads(clientSocket.recv(self.maxFileSize))
        pass

    # Send the requested resource as response
    def ResResource(connection):
        connection.send(str.encode('Server is working:'))
        while True:
            data = connection.recv(2048)
            response = 'Server message: ' + data.decode('utf-8')
            if not data:
                break
            connection.sendall(str.encode(response))
        connection.close()
        pass


    def Lookup(self, filename):
        filekey = keyOfResource(filename)
        if self.predecessor < filekey and filekey <= self.key:
            return (self.ipAddr, self.port)
        
        for l in reversed(self.fingerTable):
            if (filekey - self.key + self.totalNodes)%self.totalNodes >= (l.key - self.key + self.totalNodes)%self.totalNodes:
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