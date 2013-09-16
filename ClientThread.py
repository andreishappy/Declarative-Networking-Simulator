import socket
import threading
import string
from tools import andreadline

class ClientThread(threading.Thread):
    def __init__(self,sock,myfile,node_id):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.state_list = []
        self.sock = sock
        self.stopped = False
        self.myfile = myfile

    def stop(self):
        self.stopped = True
        return self.node_id, self.state_list

    def run(self):
        first = True
        while not self.stopped:
            if not first:
                nodeid = andreadline(self.myfile)

            tmp = andreadline(self.myfile)
            tmp = string.split(tmp)
        
            state_count = tmp[0]
            timestamp = tmp[1]
        
            received_count = int(andreadline(self.myfile))
            raw_received = []
            for i in xrange(0,received_count):
                raw_received.append(andreadline(self.myfile))
            
            received = cook_messages(raw_received)
    
            state_count = int(andreadline(self.myfile))
            raw_state = []
            for i in xrange(0,state_count):
                raw_state.append(andreadline(self.myfile))
                
            state = cook_state(raw_state)

            sent_count = int(andreadline(self.myfile))
            raw_sent = []
            for i in xrange(0,sent_count):
                sent_mess = andreadline(self.myfile)
            raw_sent.append(sent_mess)
            
            sent = cook_messages(raw_sent)
        
            to_log = 'Node:{0} State:{1} Time:{2}\n Received:{3} \n State:{4} \n Sent:{5}'\
                .format(self.node_id,state_count,timestamp,received,state,sent)

            self.state_list.append(to_log)
            print "Just Logged {0}".format(to_log)
            
            print "Sending RESUME"
            self.sock.send('RESUME')

            first = False
