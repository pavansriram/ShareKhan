from node import Node
import sys
import threading
from rpc import RPC

DataReqPort = 5001
RPCReqPort = 5002

if __name__ == "__main__":


    node = Node(sys.args[0])
    
    beacon = None
    try:
        beacon = sys.argv[1]
    except:
        print('This is the first node')
        
    node.Join(beacon)

    # Thread to handle incoming data requests
    thread = threading.Thread(target=node.SendResource)
    thread.start()

    # Thread to handle RPC requests
    thread = threading.Thread(target=node.ServerStub)
    thread.start()

    while(True):
        print('1 - Request a resource')
        print('2 - Add a resource')
        print('3 - Leave the system')

        option = int(input())

        if(option == 1):
            filename = input('Enter the filename')
            thread = threading.Thread(target=node.ReqResource, args=(filename,))
            thread.start()
        elif(option == 2):
            filename = input('Enter the filename')
            node.AddResource(filename)
        elif(option == 3):
            RPC('SetPredecessor', node.successor[1], RPCReqPort, node.predecessor[0], node.predecessor[1])
            RPC('SetSuccessor', node.predecessor[1], RPCReqPort, node.successor[0], node.successor[1])

            for resource in node.myResources:
                RPC('MoveResource', node.successor[1], RPCReqPort, resource[0])

            pass
        else:
            print('Please read the above intructions carefully')
