import threading
import socket

class LocalController(threading.Thread):
    def __init__(self,host,port,node_id):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.state = \
'''
3
add_neighbour(jeff);
add_neighbour(me);
add_link(two,2,3.4);
10
neighbour(jeff);
neighbour(me);
neighbour(joanne);
candidate(A,AB,3);
candidate(G,G,3);
candidate(A,F,3);
cost(A,F,45);
cost(B,G,56);
cost(G,H,100);
last(straw,20);
4
advertise(A,B,C);
advertise(C,B,A);
construct(yoyoyo);
construct(ohmygod);
'''
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print "Trying to connect to {0}".format((host,port))
        self.s.connect((host,port))

        
    def run(self):
        for i in range(1,11):
            mess = 'Node{2}\n{0} time{1}'.format(i,self.state,self.node_id)
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
