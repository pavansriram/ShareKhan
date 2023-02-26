from __future__ import annotations
import random
import socket
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
globalSockets = []

class Node:
    def __init__(self, ipAddr):
        self.ipAddr = ipAddr
        self.predecessor = None         # (key, ipAddr)
        self.m = 3
        self.id = self.ComputeKey(ipAddr)
        self.successor = (self.id, ipAddr)         # (key, ipAddr)
        self.myResources = []
        self.totalNodes = 3
        self.maxFileSize = 4096
        self.fingerTable = []
        for i in range(self.m):
            self.fingerTable.append((self.id, self.ipAddr))

    def ComputeKey(self, ipAddr):
        return (int(ipaddress.ip_address(ipAddr))%(2**self.m))

    def ComputeFileKey(self, filename):
        return int(filename.split('.')[0].split('_')[0])

    # Request for a resource
    def ReqResource(self, filename):

        destIpAddr, destPort = self.Lookup(filename)
        print('Request is called for file', filename, 'to', destIpAddr)

        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        globalSockets.append(clientSocket)
        clientSocket.connect((destIpAddr, destPort))
        # fcntl.fcntl(clientSocket, fcntl.F_SETFL, os.O_NONBLOCK)

        clientSocket.send(pickle.dumps(('ResourceRequest', filename)))
        
        try:
            while True:
                bytes_read = clientSocket.recv(1024)
                if not bytes_read:
                    break
                path = os.getcwd() +  '/localData/' + filename
                with open(path, 'w') as reqfile:
                    reqfile.write(pickle.loads(bytes_read))
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                # time.sleep(1)
                print('No data available')
            else:
                print(e)
                sys.exit(1)

        print('File request completed')
        clientSocket.close()

    # Send the requested resource as response
    def SendResource(self):
        print('SendResouce Thread started...')
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        globalSockets.append(serverSocket)

        try:
            serverSocket.bind((self.ipAddr, DataReqPort))
        except socket.error as e:
            print('ERROR: Send Resource socket', str(e))

        serverSocket.listen(10)
        
        currentThreads = []
        while(True):
            client, ipAddress = serverSocket.accept()
            thread = threading.Thread(target=self.helperSend, args=(client, ))
            # currentThreads.append[thread]
            thread.start()

    def helperSend(self, client):
        try:
            data = client.recv(1024)
            callType, filename = pickle.loads(data)
            
            filename = str(self.ComputeFileKey(filename)) + '_' + str(self.id) + '.txt'
            if(callType == 'ResourceRequest'):
                path = os.getcwd() + '/Data/' + filename
                with open(path, 'r') as sendFile:
                    while True:
                        # read the bytes from the file
                        bytes_read = sendFile.read(1024)
                        if not bytes_read:
                            break
                        client.sendall(pickle.dumps(bytes_read))
                        
                print(f'File {filename} Sent Successfully')
                
            elif(callType == 'MoveResource'):
                path = os.getcwd() + '/Data/' + filename
                with open(path, 'r') as sendFile:
                    while True:
                        # read the bytes from the file
                        bytes_read = sendFile.read(1024)
                        if not bytes_read:
                            break
                        client.sendall(pickle.dumps(bytes_read))
            
                print(f'File {filename} Move Successfull')
                
                # Removing the file
                if os.path.exists(path):
                    os.remove(path)
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

    def RPC(self, func_name, destIpAddr, destPort, *args):
        print('RPC call', func_name)
        if(self.ipAddr == destIpAddr):
            if func_name == 'Lookup':
                filename = args[0]    
                result = self.Lookup(filename)
            
            elif func_name == 'ClosestPreceedingFinger':
                id = args[0]
                result = self.ClosestPreceedingFinger(id)

            elif func_name == 'FindSuccessor':
                id = args[0]
                result = self.FindSuccessor(id)

            elif func_name == 'UpdateFingerTable':
                nodeId = args[0]
                nodeIp = args[1]
                i = args[2]
                result = self.UpdateFingerTable(nodeId, nodeIp, i)
                
            elif func_name == 'GetResourceKeyList':
                predecessor = args[0]
                result = self.GetResourceKeyList(predecessor)

            elif func_name == 'MoveResource':
                filename = args[0]
                destIpAddr = args[1]
                result = self.MoveResource(filename, destIpAddr)

            elif func_name == 'GetNode':
                result = self.GetNode()

            elif func_name == 'GetSuccessor':
                result = self.GetSuccessor()

            elif func_name == 'GetPredecessor':
                result = self.GetPredecessor()

            elif func_name == 'SetPredecessor':
                result = self.SetPredecessor(args[0], args[1])

            elif func_name == 'SetSuccessor':
                result = self.SetSuccessor(args[0], args[1])

            else:
                result = -1
            print('RPC completed', result)
            return result
            
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((destIpAddr, destPort))
        
        msgList = (func_name, args)     # message payload
        msgSend = pickle.dumps(msgList) # wrap call
        
        clientSocket.send(msgSend)      # send request to server
        msgRecv = clientSocket.recv(1024)
        retVal = pickle.loads(msgRecv)
        
        clientSocket.close()

        print('RPC completed', retVal)
        return retVal

    def ServerStub(self):
        print('ServerStub Thread Started...')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        globalSockets.append(sock)
        
        try:
            sock.bind((self.ipAddr, RPCReqPort))
        except socket.error as e:
            print('ERROR: ServerStub socket', e)
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
                nodeId = request[1][0]
                nodeIp = request[1][1]
                i = request[1][2]
                result = self.UpdateFingerTable(nodeId, nodeIp, i)
                
            elif request[0] == 'GetResourceKeyList':
                predecessor = request[1][0]
                result = self.GetResourceKeyList(predecessor)

            elif request[0] == 'MoveResource':
                filename = request[1][0]
                destIpAddr = request[1][1]
                result = self.MoveResource(filename, destIpAddr)
            
            elif request[0] == 'GetNode':
                result = self.GetNode()

            elif request[0] == 'GetSuccessor':
                result = self.GetSuccessor()

            elif request[0] == 'GetPredecessor':
                result = self.GetPredecessor()
            
            elif request[0] == 'SetPredecessor':
                predId = request[1][0]
                predIp = request[1][1]
                result = self.SetPredecessor(predId, predIp)

            elif request[0] == 'SetSuccessor':
                succId = request[1][0]
                succIp = request[1][1]
                result = self.SetSuccessor(succId, succIp)
            
            else:
                result = -1
            
            retVal = pickle.dumps(result)
            conn.send(retVal)
            conn.close()


    def AddResource(self, filename):
        filekey = self.ComputeFileKey(filename)
        id, successorIp = self.FindSuccessor(filekey)
        succpred = self.RPC('GetPredecessor', successorIp, RPCReqPort)
        
        if succpred[0] == filekey:
            id, successorIp = succpred[0], succpred[1]
        os.rename(os.getcwd() + '/localData/' + filename, os.getcwd() + '/Data/' + filename)
        self.RPC('MoveResource', successorIp, RPCReqPort, filename, self.ipAddr)
        

    def Lookup(self, filename):
        
        fileId = self.ComputeFileKey(filename)

        op1 = (fileId - self.predecessor[0] + 2**self.m)%(2**self.m)
        op2 = (self.id - self.predecessor[0] + 2**self.m)%(2**self.m)

        if(op1>0 and op1 <= op2):
            return (self.ipAddr, DataReqPort)
        
        for l in reversed(self.fingerTable):
            if (fileId - self.id + (2**self.m))%(2**self.m) >= (l[0] - self.id + (2**self.m))%(2**self.m):
                return self.RPC('Lookup', l[1], RPCReqPort, filename)

        return self.RPC('Lookup', self.fingerTable[0][1], RPCReqPort, filename)
    
    # return the successor for the given key
    def FindSuccessor(self, id):
        predId, predIpAddr, predSuccessor = self.FindPredecessor(id)
        
        return self.RPC('GetSuccessor', predIpAddr, RPCReqPort) 

    def GetSuccessor(self):
        return self.successor

    def FindPredecessor(self, id):
        node = (self.id, self.ipAddr, self.successor)

        if self.id ==  self.successor[0]:
            return node

        relid = (id - node[0] + (2**self.m))%(2**self.m)
        relsucnode = (node[2][0] - node[0] + (2**self.m))%(2**self.m)

        #RPC to closest Preceeding Finger
        while(not(relid <= relsucnode and relid  > 0)):
            node = self.RPC('ClosestPreceedingFinger', node[1], RPCReqPort, id)

            relid = (id - node[0] + (2**self.m))%(2**self.m)
            relsucnode = (node[2][0] - node[0] + (2**self.m))%(2**self.m)

        return node


    def ClosestPreceedingFinger(self, id):
        if self.id == id:
            return (self.predecessor[0], self.predecessor[1], (self.id, self.ipAddr))
        for l in reversed(self.fingerTable):
            relid = (id - self.id + (2**self.m))%(2**self.m)
            relentry = (l[0] - self.id + (2**self.m))%(2**self.m)
            if relid > relentry and relentry > 0:
                return self.RPC('GetNode', l[1], RPCReqPort)
        return (self.id, self.ipAddr, self.successor)

    def Join(self, ipAddr):
        if(ipAddr):
            self.InitFingerTable(ipAddr)
            self.UpdateOthers()
            self.GetResources()
        else:
            for i in range(self.m):
                self.fingerTable[i] = (self.id, self.ipAddr)
            self.predecessor = (self.id, self.ipAddr)
            self.successor = (self.id, self.ipAddr)
        
        return {f'Node {self.id}, {self.ipAddr} joined successfully'}

    def InitFingerTable(self, ipAddr):
        tempId, tempIpAddr = self.RPC('FindSuccessor', ipAddr, RPCReqPort, self.Start(0))
        self.fingerTable[0] = (tempId, tempIpAddr)
        self.successor = self.fingerTable[0]
        
        # self.predecessor = tempNode.predecessor
        self.predecessor = self.RPC('GetPredecessor', tempIpAddr, RPCReqPort)
        self.RPC('SetPredecessor', tempIpAddr, RPCReqPort, self.id, self.ipAddr)
        self.RPC('SetSuccessor', self.predecessor[1], RPCReqPort, self.id, self.ipAddr)
        

        for i in range(self.m - 1):
            if((self.Start(i+1) - self.id + (2**self.m))%(2**self.m) < (self.fingerTable[i][0] - self.id + (2**self.m))%(2**self.m)):
                self.fingerTable[i+1] = self.fingerTable[i]
            else:
                self.fingerTable[i+1] = self.RPC('FindSuccessor', ipAddr, RPCReqPort, self.Start(i+1))
        
        return {f'Updated the finger table for node {self.id}, {self.ipAddr}'}
        # return success msg

    def UpdateOthers(self):
        for i in range(self.m):
            node = self.FindPredecessor((self.id - 2**i + 1 + (2**self.m))%(2**self.m))
            if node[0] != self.id:
                self.RPC('UpdateFingerTable', node[1], RPCReqPort, self.id, self.ipAddr, i)
        
        return 'Update All Nodes Successfully'
    
    def GetResources(self):
        rlist = self.RPC('GetResourceKeyList', self.fingerTable[0][1], RPCReqPort, self.id)
        for f in rlist:
            self.MoveResource(f[0], self.fingerTable[0][1])

    def MoveResource(self, filename, destIpAddr):

        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        globalSockets.append(clientSocket)

        clientSocket.connect((destIpAddr, DataReqPort))
        # fcntl.fcntl(clientSocket, fcntl.F_SETFL, os.O_NONBLOCK)

        clientSocket.send(pickle.dumps(('MoveResource', filename)))
        filename = str(self.ComputeFileKey(filename)) + '_' + str(self.id) + '.txt'
        try:
            while True:
                bytes_read = clientSocket.recv(1024)
                if not bytes_read:
                    break
                path = os.getcwd() + '/Data/' + filename
                
                with open(path, 'w') as reqfile:
                    reqfile.write(pickle.loads(bytes_read))

            self.myResources.append((filename, self.ComputeFileKey(filename)))
        except socket.error as e:
            print("ERROR", e)
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                time.sleep(1)
                print('No data available')
            else:
                # a "real" error occurred
                print(e)
                sys.exit(1)

            clientSocket.close()

    def GetResourceKeyList(self, predecessor):
        retVal = list(filter(lambda x: (x[1] != self.id) and (((x[1] - self.id + 2**self.m)%2**self.m) <= ((predecessor - self.id + 2**self.m)%2**self.m)), self.myResources))
        self.myResources = list(filter(lambda x: (x[1] == self.id) or (((x[1] - self.id + 2**self.m)%2**self.m) > ((predecessor - self.id + 2**self.m)%2**self.m)), self.myResources))

        return retVal
    
    def UpdateFingerTable(self, sid, sipAddr, i):
        if self.id == self.fingerTable[i][0]:
            self.fingerTable[i] = (sid, sipAddr)
        if((sid - self.id + (2**self.m)) % (2**self.m) < (self.fingerTable[i][0] - self.id + (2**self.m)) % (2**self.m)):
            self.fingerTable[i] = (sid, sipAddr)
            self.RPC('UpdateFingerTable', self.predecessor[1], RPCReqPort, sid, sipAddr, i)
        
        return {f'Updated {i}th FingerTable Successfully'}

    def Start(self, k):
        return (self.id + 2**(k))%((2**self.m))
    
    def SetPredecessor(self, nid, ipAddr):
        self.predecessor = (nid, ipAddr)
        return 
    
    def SetSuccessor(self, nid, ipAddr):
        self.fingerTable[0] = (nid, ipAddr)
        self.successor = (nid, ipAddr)
        return 

    def GetPredecessor(self):
        return self.RPC('GetNode', self.predecessor[1], RPCReqPort)[:2]

    def GetNode(self):
        return (self.id, self.ipAddr, self.successor)

    def Stabilize(self):
        tempNode : Node = self.RPC('GetPredecessor', self.successor[1], RPCReqPort)
        if((tempNode.id - self.id + (2**self.m))%(2**self.m) < (self.successor[0] - self.id + (2**self.m))%(2**self.m)):
            self.successor = (tempNode.id, tempNode.ipAddr)
        self.RPC('Notify', self.successor[1], RPCReqPort, self)

    def Notify(self, node : Node):
        if(self.predecessor == None or ((node.id - self.predecessor[0] + (2**self.m))%(2**self.m) <= (self.id - self.predecessor[0] + (2**self.m))%(2**self.m))):
            self.predecessor = (node.id, node.ipAddr)
        return
    
    def FixFingers(self):
        i = math.floor(random.random()*10)
        self.fingerTable[i] = self.FindSuccessor(self.Start(i))
        return 
    
    