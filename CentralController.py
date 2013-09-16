import socket
import threading
from ClientThread import ClientThread
from tools import andreadline
class CentralController(threading.Thread):
    def __init__(self,port):
        threading.Thread.__init__(self)

        #Set up the server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)        
        self.server_socket.bind(('',port))
        self.server_socket.listen(5)

        #Initialize the history map
        self.history = {}

        #Bool which stops the thread
        self.stopped = False

        self.client_threads = []

    def stop(self):
        for thread in self.client_threads:
            node_id, state_list = thread.stop()
            self.history[node_id] = state_list

        self.stopped = True
        return self.history

    def run(self):
        print "Server Started Listening"
        while not self.stopped:
            sock,addr = self.server_socket.accept()
            myfile = sock.makefile(mode='r')
            node_id = andreadline(myfile)
            print "Connected to node {0}".format(node_id)
            client_thread = ClientThread(sock,myfile, node_id)
            client_thread.start()
            self.client_threads.append(client_thread)        
