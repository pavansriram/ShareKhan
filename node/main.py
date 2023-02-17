from __future__ import annotations
import socket
from hash import keyOfResource
import pickle
import os

class Node:
    def __init_(self, ipAddr):
        self.ipAddr = ipAddr
        self.predecessor = None
        self.successor = None
        self.id = self.ComputeKey(ipAddr)
        self.fingerTable = self.ComputeFingerTable()
        self.myResources = []
        self.m = 3
        self.totalNodes = 8
        self.maxFileSize = 4096

    def ComputeKey(ipAddr):
        return 0

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
        fileId = keyOfResource(filename)
        if self.predecessor < fileId and fileId <= self.id:
            return (self.ipAddr, self.port)
        
        for l in reversed(self.fingerTable):
            if (fileId - self.id + self.totalNodes)%self.totalNodes >= (l.id - self.id + self.totalNodes)%self.totalNodes:
                return RPC('Lookup', l.id, fileId)

        return RPC('Lookup', self.fingerTable[0].id, fileId)

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
    def FindSuccessor(self, id):
        node : Node = self.FindPredecessor(id)
        return node.successor

    def FindPredecessor(self, id):
        node : Node = self
        #RPC to closest Preceeding Finger
        while(not((id - node.id + node.totalNodes)%node.totalNodes <= (node.successor - node.id + node.totalNodes)%node.totalNodes)):
            node = RPC('ClosestPreceedingFinger', node.id, id)
        return node
    '''
        while(not(node.id < id and id < node.successor)):
            node = node.ClosestPreceedingFinger(id)
        return node
    '''
    

    def ClosestPreceedingFinger(self, id):

        for l in reversed(self.fingerTable):
            if (id - self.id + self.totalNodes)%self.totalNodes > (l.id - self.id + self.totalNodes)%self.totalNodes:
                return l
        return self

    def Join(self, node : Node):
        if(node):
            self.InitFingerTable(node)
            self.UpdateOthers()
        else:
            for i in self.m:
                self.fingerTable[i] = (self.id, self.ipAddr)
            self.predecessor = self.id
        # return success message

    def InitFingerTable(self, node: Node):
        tempNode = RPC('FindSuccessor', node, self.Start(0))
        self.fingerTable[0] = (tempNode.id, tempNode.ipAddr)
        self.predecessor = node.predecessor
        RPC('SetPredecessor', node, self.id)
        # return success msg

    def UpdateOthers(self):
        for i in self.m:
            node = self.FindPredecessor((self.id - 2**i + self.totalNodes)%self.totalNodes)
            RPC('UpdateFingerTable', node, i)
        # return success msg

    def UpdateFingerTable(self, node: Node, i):
        if((node.id - self.id + self.totalNodes) % self.totalNodes <= (self.fingerTable[i].id - self.id + self.totalNodes) % self.totalNodes):
            self.fingerTable[i] = (node.id, node.ipAddr)
            RPC('UpdateFingerTable', self.predecessor, node, i)
        # return success msg

    def Start(self, k):
        return (self.id + 2**(k))%(self.totalNodes)