import socket
from ..node.main import ResResource
# import threads

class Dispatcher:
    def __init__(self):
        self.activeThreads = 0
        self.ServerSideSocket = socket.socket()
        self.host = '127.0.0.1'
        self.port = 5001
        self.ThreadCount = 0
        try:
            self.ServerSideSocket.bind((self.host, self.port))
        except socket.error as e:
            print(str(e))
        print('Socket is listening..')
        self.ServerSideSocket.listen(5)
        

    def ListenForever(self):
        while True:
            Client, address = self.ServerSideSocket.accept()
            print('Connected to: ' + address[0] + ':' + str(address[1]))
            start_new_thread(ResResource, (Client, ))
            ThreadCount += 1
            print('Thread Number: ' + str(ThreadCount))
        ServerSideSocket.close()

        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serverSocket.bind((socket.gethostname(), 80))
        serverSocket.listen(5)

        while True:
            (clientSocket, address) = serverSocket.accept()
            self.CreateThread(clientSocket, address)

    def CreateThread(clientSocket, address):
        pass