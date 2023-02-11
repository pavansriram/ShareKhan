import socket

class Dispatcher:
    def __init__(self):
        self.activeThreads = 0
        

    def ListenForever(self):
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serverSocket.bind((socket.gethostname(), 80))
        serverSocket.listen(5)

        while True:
            (clientSocket, address) = serverSocket.accept()
            self.CreateThread(clientSocket, address)

    def CreateThread(clientSocket, address):
        pass