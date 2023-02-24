from __future__ import annotations
import random
import socket
from hash import keyOfResource
from rpc import RPC
import pickle
import os
import math

class Node:
    def __init__(self, ipAddr):
        self.ipAddr = ipAddr
        self.predecessor = None         # a node
        self.successor = self           # the successor node itself
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

    def ServerStub(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(self.ipAddr, 12345)
        sock.listen(10)

        while(True):
            (conn, addr) = sock.accept()    # accept incoming call
            data = conn.recv(1024)          # fetch data from client
            request = pickle.loads(data)    # unwrap the request

            if request[0] == 'Lookup':
                filename = request[1][0]    
                result = self.Lookup(filename)
            
            elif request[0] == 'ClosestPreceedingFinger':
                id = request[1][0]
                result = self.ClosestPreceedingFinger(id)

            elif request[0] == 'FindSuccessor':
                id = request[1][0]
                result = self.FindSuccessor(id)

            elif request[0] == 'UpdateFingerTable':
                node = request[1][0]
                i = request[1][1]
                result = self.UpdateFingerTable(node, i)

            else:
                result = -1
            
            retVal = pickle.dumps(result)
            conn.send(retVal)
            conn.close()

    def Lookup(self, filename):
        fileId = keyOfResource(filename)
        if self.predecessor.id < fileId and fileId <= self.id:
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

    # return the successor for the given key
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
        self.successor = RPC('FindSuccessor', node, self.id)
        if(node):
            self.InitFingerTable(node)
            self.UpdateOthers()
        else:
            for i in self.m:
                self.fingerTable[i] = (self.id, self.ipAddr)
            self.predecessor = self
        
        return {f'Node {self.id}, {self.ipAddr} joined successfully'}
        # return success message

    def InitFingerTable(self, node: Node):
        tempNode : Node = RPC('FindSuccessor', node, self.Start(0))
        self.fingerTable[0] = (tempNode.id, tempNode.ipAddr)
        self.predecessor = tempNode.predecessor
        RPC('SetPredecessor', tempNode, self)
        for i in range(self.m):
            if((self.Start(i+1) - self.id + self.totalNodes)%self.totalNodes < (self.Start(i) - self.id + self.totalNodes)%self.totalNodes):
                self.fingerTable[i+1] = self.fingerTable[i]
            else:
                ithEntry : Node = RPC('FindSuccessor', node, self.Start(i+1))
                self.fingerTable[i+1] = (ithEntry.id, ithEntry.ipAddr)
        
        return {f'Updated the finger table for node {self.id}, {self.ipAddr}'}
        # return success msg

    def UpdateOthers(self):
        for i in self.m:
            node = self.FindPredecessor((self.id - 2**i + self.totalNodes)%self.totalNodes)
            RPC('UpdateFingerTable', node, i)
        
        return 'Update All Nodes Successfully'
        # return success msg

    def UpdateFingerTable(self, node: Node, i):
        if((node.id - self.id + self.totalNodes) % self.totalNodes <= (self.fingerTable[i].id - self.id + self.totalNodes) % self.totalNodes):
            self.fingerTable[i] = (node.id, node.ipAddr)
            RPC('UpdateFingerTable', self.predecessor, node, i)
        
        return {f'Updated {i}th FingerTable Successfully'}
        # return success msg

    def Start(self, k):
        return (self.id + 2**(k))%(self.totalNodes)
    
    def SetPredecessor(self, node: Node):
        self.predecessor = node
        return 

    def Stabilize(self):
        tempNode : Node = self.successor.predecessor
        if((tempNode.id - self.id + self.totalNodes)%self.totalNodes < (self.successor.id - self.id + self.totalNodes)%self.totalNodes):
            self.successor = tempNode
        RPC('Notify', self.successor, self)

    def Notify(self, node : Node):
        if(self.predecessor == None or ((node.id - self.predecessor.id + self.totalNodes)%self.totalNodes <= (self.id - self.predecessor.id + self.totalNodes)%self.totalNodes)):
            self.predecessor = node
        return
    
    def FixFingers(self):
        i = math.floor(random.random()*10)
        self.fingerTable[i] = self.FindSuccessor(self.Start(i))
        return 
    