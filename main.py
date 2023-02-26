from node import Node, globalSockets
import sys
import threading
# from rpc import RPC

DataReqPort = 5001
RPCReqPort = 5002


if __name__ == "__main__":

    node = Node(sys.argv[1])

    # Thread to handle incoming data requests
    thread = threading.Thread(target=node.SendResource)
    thread.start()

    # Thread to handle RPC requests
    thread = threading.Thread(target=node.ServerStub)
    thread.start()
    
    beacon = None
    try:
        beacon = sys.argv[2]
    except:
        print('This is the first node')
        
    print(node.Join(beacon))

    while(True):
        print('1 - Request a resource')
        print('2 - Add a resource')
        print('3 - Print Resources')
        print('4 - Print FingerTable')
        print('5 - Print successor and predecessor')

        option = int(input())

        if(option == 1):
            filename = input('Enter the filename: ')
            print(filename)
            thread = threading.Thread(target=node.ReqResource, args=(filename,))
            thread.start()
        elif(option == 2):
            filename = input('Enter the filename: ')
            print(filename)
            node.AddResource(filename)
        elif(option == 3):
            print(node.myResources)
        elif(option == 4):
            print('No   NodeId\tIP')
            for i in range(node.m):
                print(i, '  ', node.fingerTable[i][0], '  ', node.fingerTable[i][1])
        elif(option == 5):
            print('successor of ipAddr ', node.ipAddr, ' is ', node.successor)
            print('predecessor of ipAddr ', node.ipAddr, ' is ', node.predecessor)
        else:
            print('Please enter a number between 1 and 5')
