
import random
import socket
from rpc import RPC
import pickle
import fcntl, os
import math
import time
import sys
import errno
import threading
import ipaddress

SEPARATOR = '$'
DataReqPort = 5001
RPCReqPort = 5002

class Node:
    def __init__(self, ipAddr):
        self.ipAddr = ipAddr
        self.predecessor = None         # (key, ipAddr)
        self.id = self.ComputeKey(ipAddr)
        self.successor = (self.id, ipAddr)         # (key, ipAddr)
        self.myResources = []
        self.m = 3
        self.totalNodes = 3
        self.maxFileSize = 4096
        self.fingerTable = None

    def ComputeKey(self, ipAddr):
        return (int(ipaddress.ip_address(ipAddr))%(2**self.m))

    def ComputeFileKey(filename):
        return filename.split('.')[0]

    # Request for a resource
    def ReqResource(self, filename):
        destIpAddr, destPort = self.Lookup(filename)

        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((destIpAddr, destPort))
        fcntl.fcntl(clientSocket, fcntl.F_SETFL, os.O_NONBLOCK)

        clientSocket.send(pickle.dumps(('ResourceRequest', filename)))
        
        try:
            while True:
                bytes_read = clientSocket.recv(1024)
                if not bytes_read:
                    break
                path = 'localData/' + filename
                with open(path, 'wr') as reqfile:
                    reqfile.write(bytes_read)
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                # time.sleep(1)
                print('No data available')
            else:
                # a "real" error occurred
                print(e)
                sys.exit(1)

        clientSocket.close()

    # Send the requested resource as response
    def SendResource(self):
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            serverSocket.bind((self.ipAddr, DataReqPort))
        except socket.error as e:
            print(str(e))
        print('Socket is listening...')
        serverSocket.listen(5)
        
        currentThreads = []
        while(True):
            client, ipAddress = serverSocket.accept()
            print('Connected to: ' + ipAddress[0] + ':' + str(ipAddress[1]))
            thread = threading.Thread(target=self.helperSend, args=(client, ))
            currentThreads.append[thread]
            thread.start()

    def helperSend(client):
        try:
            data = client.recv(1024)
            callType, filename = pickle.loads(data)

            if(callType == 'ResourceRequest'):
                path = 'Data/' + filename
                with open(path, 'r') as sendFile:
                    while True:
                        # read the bytes from the file
                        bytes_read = sendFile.read(1024)
                        if not bytes_read:
                            # file transmitting is done
                            break
                        # we use sendall to assure transimission in 
                        # busy networks
                        client.sendall(bytes_read)
                        
                print(f'File {filename} Sent Successfully')
                
            elif(callType == 'MoveResource'):
                path = 'Data/' + filename
                with open(path, 'r') as sendFile:
                    while True:
                        # read the bytes from the file
                        bytes_read = sendFile.read(1024)
                        if not bytes_read:
                            # file transmitting is done
                            break
                        # we use sendall to assure transimission in 
                        # busy networks
                        client.sendall(bytes_read)
            
                print(f'File {filename} Move Successfull')
                
                # Removing the file
                if os.path.exists(f'Data/{filename}'):
                    os.remove(f'Data/{filename}')
                else:
                    print("The file does not exist")
            else:
                pass
            
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                # time.sleep(1)
                print('No data available')
            else:
                # a "real" error occurred
                print(e)
                sys.exit(1)
        
        client.close()

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
                
            elif request[0] == 'GetResourceKeyList':
                predecessor = request[1][0]
                result = self.GetResourceKeyList(predecessor)

            elif request[0] == 'MoveResource':
                filename = request[1][0]
                result = self.MoveResource(filename)

            else:
                result = -1
            
            retVal = pickle.dumps(result)
            conn.send(retVal)
            conn.close()

    def AddResource(self, filename):
        filekey = self.computeFileKey(filename)
        id, successorIp = self.FindSuccessor(filekey)
        os.rename(f'localData/{filename}', f'Data/{filename}')
        RPC('MoveResource', successorIp, DataReqPort, filename)
        

    def Lookup(self, filename):
        fileId = keyOfResource(filename)
        if self.predecessor[0] < fileId and fileId <= self.id:
            return (self.ipAddr, DataReqPort)
        
        for l in reversed(self.fingerTable):
            if (fileId - self.id + (2**self.m))%(2**self.m) >= (l.id - self.id + (2**self.m))%(2**self.m):
                return RPC('Lookup', l[1], RPCReqPort, fileId)

        return RPC('Lookup', self.fingerTable[1], RPCReqPort, fileId)
    
    # return the successor for the given key
    def FindSuccessor(self, id):
        node : Node = self.FindPredecessor(id)
        return node.successor

    def FindPredecessor(self, id):
        node : Node = self
        #RPC to closest Preceeding Finger
        while(not((id - node.id + node.totalNodes)%node.totalNodes <= (node.successor[0] - node.id + node.totalNodes)%node.totalNodes)):
            node = RPC('ClosestPreceedingFinger', node.ipAddr, RPCReqPort, id)
        return node


    def ClosestPreceedingFinger(self, id):
        for l in reversed(self.fingerTable):
            if (id - self.id + (2**self.m))%(2**self.m) > (l[0] - self.id + (2**self.m))%(2**self.m):
                return RPC('GetNode', l[1], RPCReqPort)
        return self

    def Join(self, node : Node):
        if(node):
            self.successor = RPC('FindSuccessor', node.ipAddr, RPCReqPort, self.id)
            self.InitFingerTable(node)
            self.UpdateOthers()
            self.GetResources()
        else:
            for i in self.m:
                self.fingerTable[i] = (self.id, self.ipAddr)
            self.predecessor = (self.id, self.ipAddr)
            self.successor = (self.id, self.ipAddr)
        
        return {f'Node {self.id}, {self.ipAddr} joined successfully'}

    def InitFingerTable(self, node: Node):
        tempNode : Node = RPC('FindSuccessor', node.ipAddr, RPCReqPort, self.Start(0))
        self.fingerTable[0] = (tempNode.id, tempNode.ipAddr)
        self.predecessor = tempNode.predecessor
        RPC('SetPredecessor', tempNode.ipAddr, RPCReqPort, self.id, self.ipAddr)
        for i in range(self.m):
            if((self.Start(i+1) - self.id + (2**self.m))%(2**self.m) < (self.Start(i) - self.id + (2**self.m))%(2**self.m)):
                self.fingerTable[i+1] = self.fingerTable[i]
            else:
                ithEntry : Node = RPC('FindSuccessor', node.ipAddr, RPCReqPort, self.Start(i+1))
                self.fingerTable[i+1] = (ithEntry.id, ithEntry.ipAddr)
        
        return {f'Updated the finger table for node {self.id}, {self.ipAddr}'}
        # return success msg

    def UpdateOthers(self):
        for i in self.m:
            node = self.FindPredecessor((self.id - 2**i + (2**self.m))%(2**self.m))
            RPC('UpdateFingerTable', node.ipAddr, RPCReqPort, i)
        
        return 'Update All Nodes Successfully'
        # return success msg
    
    def GetResources(self):
        rlist = RPC('GetResouceKeyList', self.fingerTable[0][1], RPCReqPort, self.id)
        for file in rlist:
            self.MoveResource(file[0])

    def MoveResource(self, filename):
        destKey, destIpAddr = self.fingerTable[0]

        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((destIpAddr, DataReqPort))
        fcntl.fcntl(clientSocket, fcntl.F_SETFL, os.O_NONBLOCK)

        clientSocket.send(pickle.dumps(('MoveResource', filename)))
        
        try:
            while True:
                bytes_read = clientSocket.recv(1024)
                if not bytes_read:
                    break
                with open(f'localData/{filename}', 'wr') as reqfile:
                    reqfile.write(bytes_read)
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                time.sleep(1)
                print('No data available')
            else:
                # a "real" error occurred
                print(e)
                sys.exit(1)

            
    def GetResourceKeyList(self, predecessor):
        return list(filter(lambda x: x[1] <= predecessor, self.myResources))
    
    def UpdateFingerTable(self, node: Node, i):
        if((node.id - self.id + (2**self.m)) % (2**self.m) <= (self.fingerTable[i].id - self.id + (2**self.m)) % (2**self.m)):
            self.fingerTable[i] = (node.id, node.ipAddr)
            RPC('UpdateFingerTable', self.predecessor[1], RPCReqPort, node, i)
        
        return {f'Updated {i}th FingerTable Successfully'}
        # return success msg

    def Start(self, k):
        return (self.id + 2**(k))%((2**self.m))
    
    def SetPredecessor(self, id, ipAddr):
        self.predecessor = (id, ipAddr)
        return 

    def GetPredecessor(self):
        node : Node = RPC('GetNode', self.predecessor[1], RPCReqPort)
        return node

    def GetNode(self):
        return self

    def Stabilize(self):
        tempNode : Node = RPC('GetPredecessor', self.successor[1], RPCReqPort)
        if((tempNode.id - self.id + (2**self.m))%(2**self.m) < (self.successor[0] - self.id + (2**self.m))%(2**self.m)):
            self.successor = (tempNode.id, tempNode.ipAddr)
        RPC('Notify', self.successor[1], RPCReqPort, self)

    def Notify(self, node : Node):
        if(self.predecessor == None or ((node.id - self.predecessor[0] + (2**self.m))%(2**self.m) <= (self.id - self.predecessor[0] + (2**self.m))%(2**self.m))):
            self.predecessor = (node.id, node.ipAddr)
        return
    
    def FixFingers(self):
        i = math.floor(random.random()*10)
        self.fingerTable[i] = self.FindSuccessor(self.Start(i))
        return 
    
    