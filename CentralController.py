import socket, string
import threading, time
from ClientThread import ClientThread
from tools import andreadline
from Inserter import Inserter
import datetime
#Following 2 classes used to encapsulate Dynamic Changes
class DynamicChangeTypeException(Exception):
    pass

class DynamicChange:
    def __init__(self,typ,target,tup,delay):
        self.type = typ
        self.target = target
        first_paren = string.find(tup,'(')
        self.table_name = tup[:first_paren]
        self.delay = delay

        tup = tup[first_paren+1:]
        tup = tup.replace(')','').replace(';','')
        
        self.tuple = [x.strip() for x in tup.split(',')]

    def __eq__(self,other):
        return self.type == other.type and \
               self.target == other.target and \
               self.table_name == other.table_name and \
               self.delay == other.delay

    def __str__(self):
        return "Dyn type-{0} table_name-{4} target-{1} tuple-{2} delay-{3}"\
               .format(self.type,self.target,self.tuple,self.delay,self.table_name)

#Helper Functions for dynamic changes
def there_exist(dic):
    for node in dic:
        if dic[node]:
            return True

    return False

def matches(short,longer):
    for i in xrange(0,len(short)):
        if short[i] != longer[i]:
            return False
    return True

def longstamp(date_string):
    timestamp = datetime.datetime.strptime(date_string,'%Y-%m-%d %H:%M:%S.%f')
    timestamp = int(float(timestamp.strftime('%s.%f'))*1000)
    return str(timestamp)


class CentralController(threading.Thread):
    def __init__(self,port,monitor,node_to_class,post_inputs):
        threading.Thread.__init__(self)
        #Monitor
        self.monitor = monitor
        self.to_accept = len(node_to_class)
        self.node_to_class = node_to_class

        #Set up the server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)        
        self.server_socket.bind(('',port))
        self.server_socket.listen(5)
        self.node_to_socket = {}

        #Initialize the history map
        self.history = {}

        #Bool which stops the thread
        self.stopped = False

        self.client_threads = []

        #Dynamic Changes
        self.dynamic_changes = {}
        for node in self.node_to_class:
            self.dynamic_changes[node] = []
            
        for inp in post_inputs:
            typ = inp["type"]
            if typ == "sever_link" or typ == "create_link":
                node_list = inp["link"].split(';')
                node1 = node_list[0]
                node2 = node_list[1]
                if typ == "sever_link":
                    dynamic_type = "delete"
                else:
                    dynamic_type = "add"
                    
                node1_change = DynamicChange(dynamic_type,node1,"neighbour({0});"\
                                             .format(node2),int(inp["delay"]))
                self.dynamic_changes[node1].append(node1_change) 

                node2_change = DynamicChange(dynamic_type,node2,"neighbour({0});"\
                                             .format(node1),int(inp["delay"]))
                self.dynamic_changes[node2].append(node2_change) 

            elif typ == "add" or typ == "delete":
                to_add = DynamicChange(typ,inp["node"],inp["tuple"],int(inp["delay"]))
                self.dynamic_changes[inp["node"]].append(to_add)

            else:
                raise DynamicChangeTypeException


        print "DYNAMIC CHANGES\n"
        for node in self.dynamic_changes:
            print "  {0}".format(node)
            for change in self.dynamic_changes[node]:
                print "     {0}".format(change)
        


    def stop(self):

        for thread in self.client_threads:
            node_id, state_list = thread.stop()
            self.history[node_id] = state_list

        self.stopped = True
        self.server_socket.close()
        return self.history

    def run(self):    
        self.accept_sockets()
        
        self.do_dynamic_changes()
        

    def accept_sockets(self):
        print "Server Started Listening"
        accepted = 0
        while not accepted == self.to_accept:
            sock,addr = self.server_socket.accept()
            myfile = sock.makefile(mode='r')
            node_id = andreadline(myfile).split('/')[1]
            print "Central Controller connected to node {0}".format(node_id)
            client_thread = ClientThread(sock,myfile, node_id,self.monitor,self.node_to_class[node_id])
            client_thread.daemon = True
            client_thread.start()
            self.client_threads.append(client_thread) 
            self.node_to_socket[node_id] = sock
            accepted += 1
        print "CentralController connected to all engines"

    def do_dynamic_changes(self):
        while not self.monitor.start_tuples_sent:
            time.sleep(.2)

        print "CC: Starting to do Dynamic Changes"
        while there_exist(self.dynamic_changes):
            time.sleep(.3)
            to_do = {}
            current_delay = time.time() - self.monitor.start_time
            for node in self.dynamic_changes:
                dyn_list = self.dynamic_changes[node]
                index = 0
                while index < len(dyn_list):
                    dyn = dyn_list[index]
                    # Colllect all the dynamic changes that should
                    # have happened by now in a dict (to_do)
                    if dyn.delay < current_delay:
                        if node in to_do:
                            to_do[node].append(dyn)
                        else:
                            to_do[node] = [dyn]
                        #Remove from the list so that it's not found
                        # the next time
                        del dyn_list[index]
                    index += 1

            messages = {}
            for node in to_do:
                print "CC: suspended node {0}".format(node)
                self.monitor.suspend(node)
            for node in to_do:
                #In case an engine is in limbo after a resume
                self.insert_dummy(node)
                mess = self.make_mess(to_do[node],node)
                messages[node] = mess
                print "DYNAMIC CHANGE message\n {0}".format(mess)

            for node in to_do:
                while not self.monitor.ready_for_change(node):
                    time.sleep(.2)

            #Now all the engines are waiting for CHANGESTATE
            for node in messages:
                sock = self.node_to_socket[node]
                sock.sendall(messages[node])

            for node in messages:
                self.monitor.unsuspend(node)

    def insert_dummy(self,node_id):
        cmd = 'tuple insert andrei {0} topology_change dummy=1'.format(node_id)
        ins = Inserter(cmd)
        ins.start()
        ins.join()

    def make_mess(self,dyn_list,node_id):
        to_send = 'CHANGESTATE\n'
        to_delete = []
        to_add = []

        for dyn in dyn_list:
            if dyn.type == 'add':
                to_add.append(dyn)
            elif dyn.type == 'delete':
                to_delete.append(dyn)
                
        delete_count = len(to_delete)
        add_count = len(to_add)

        to_send += '{0}\n'.format(delete_count)
        for dyn in to_delete:
            table_name = dyn.table_name
            row_list = dyn.tuple
            
            #Now find the timestamp from the previous state if it exists 
            relevant_table = self.monitor.last_state[node_id][table_name]
            to_append = None
            for state_row in relevant_table:
                if matches(row_list,state_row):
                    to_append = [longstamp(state_row[-1])]

                else:
                    continue

            if not to_append:
                to_append = ['100']
            
            new_row_list = row_list + to_append
            to_send += '{0}({1});\n'.format(table_name,','.join(new_row_list))

        to_send += '{0}\n'.format(add_count)
        for dyn in to_add:
            table_name = dyn.table_name
            row_list = dyn.tuple
            new_row_list = row_list + ['100']
            to_send += '{0}({1});\n'.format(table_name,','.join(new_row_list))

        return to_send
