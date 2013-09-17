import threading
import socket

class LocalController(threading.Thread):
    def __init__(self,host,port,node_id):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.state = \
'''
5
peers(Self,dute,10020);
peers(no,way,123123);
add_neighbour(jeff,12312);
add_neighbour(me,12312);
add_link(two,2,12312);
10
neighbour(jeff,12321);
neighbour(me,12312);
neighbour(joanne,12321);
candidate(A,AB,3,12321);
candidate(G,G,3,12321);
candidate(A,F,3,12321);
cost(A,F,45,12312);
cost(B,G,56,12321);
cost(G,H,100,12312);
last(straw,20,123123);
4
advertise(n003,n002,C,1002393);
advertise(n003,n004,C,1992393);
construct(n004,n003,D,1929392);
construct(n004,n003,D,1293740);
'''
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print "Trying to connect to {0}".format((host,port))
        self.s.connect((host,port))

        
    def run(self):
        for i in range(1,4):
            mess = '{0}\n{1} time {2}'.format(self.node_id,i,self.state)
            print "Sending {0}".format(mess)
            self.s.sendall(mess)
 

            print "Waiting for cmd"
            cmd = self.s.recv(10)
            print "Client: CMD is {0}".format(cmd)
            if cmd == 'RESUME':
                continue
            else:
                print "cmd unknown"


if __name__ == "__main__":
    loc = LocalController(2,3)
    loc.start()
