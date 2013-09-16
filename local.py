import socket
import threading
import time
import string

port = 40016

def client():
    HOST = '127.0.0.1'

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print "Trying to connect to {0}".format((HOST,port))
    s.connect((HOST,port))

    myfile = s.makefile()
    for i in range(1,11):
        mess = 'Node1\n {0} time \n3 \nhello(andrei)\nhello(manlio)\nhello(man)\n4\nyo\ndude\nhows\nit\n1\nsent\n'.format(i)
        print "Sending {0}".format(mess)
        s.send(mess)
 

        print "Waiting for cmd"
        cmd = s.recv(10)
        print "Client: CMD is {0}".format(cmd)
        if cmd == 'RESUME':
            continue
        else:
            print "cmd unknown"
        

def communicate(myfile,sock):
    for i in xrange(1,5):
        print 'Waiting for Header'
        nodeid = myfile.readline()
        print nodeid
        tmp = myfile.readline()
        tmp = string.split(tmp)
        
        state_count = tmp[0]
        timestamp = tmp[1]
        print state_count
        print timestamp
        
        received_count = int(myfile.readline())
        print "Received Count is: {0}".format(received_count)
        received = []
        for i in xrange(0,received_count):
            received.append(myfile.readline())



        state_count = int(myfile.readline())
        print "State Count: {0}".format(state_count)
        state = []
        for i in xrange(0,state_count):
            state.append(myfile.readline())

   
        sent_count = int(myfile.readline())
        print "Sent Count: {0}".format(sent_count)   
        sent = []
        for i in xrange(0,sent_count):
            sent_mess = myfile.readline()
            print sent_mess
            sent.append(sent_mess)

        print 'Node:{0} State:{1} Time:{2}\n Received:{3} \n State:{4} \n Sent:{5}'\
              .format(nodeid,state_count,timestamp,received,state,sent)

        print "Sending RESUME"
        sock.send('RESUME')

def server():
   
    HOST = ''
    PORT = 40009
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print "port = {0}".format(port)
    s.bind((HOST,port))

    s.listen(5)
    print "Server Created"
    while True:
        sock,addr = s.accept()
        myfile = sock.makefile()
        print sock, addr
        print "Connected to {0}".format(addr)
        communicate(myfile,sock)

    s.close()


if __name__ == "__main__":
    print "in main"
    server = threading.Thread(target=server)
    client1 = threading.Thread(target=client)
    client2 = threading.Thread(target=client)

    server.start()
    time.sleep(1)
    client1.start()
    time.sleep(2)
    print "Started the second Client\n================================"
    client2.start()

    server.join()
    client1.join()
    client2.join()
