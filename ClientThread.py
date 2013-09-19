import socket, time
import threading
import string
from tools import andreadline,ClosedSocketException
from HistoryObjects import Message,State



#TOOLS used by Client Thread
#==================================================


#INPUT: string of the form: "neighbour(x,3);"
#OUTPUT: tuple of the form: (<table>,[entries])
def tuple_to_list(input_string):
    first_paren = string.find(input_string,"(")
    table_name = input_string[:first_paren]
    
    rest = input_string[first_paren+1:-2]
    entries = [x.strip() for x in rest.split(',')]
    return (table_name,entries)

#INPUT: list of tuple strings: ["neighbour(x,3);","yo(r)"]
#OUTPUT: list of tuples: [(name,[onerow])
def tuples_to_list(input_list):
    result = []
    for tup in input_list:
        result.append(tuple_to_list(tup))
    
    return result

def analyse_messages(raw_list, node_id, clas):
    received = []
    sent = []
    
    cooked_list = tuples_to_list(raw_list)
    for mess in cooked_list:
        table_name = '{0}.{1}'.format(clas,mess[0])
        content_list = mess[1]
        src = content_list[1]
        dst = content_list[0]
        timestamp = content_list[-1]
        content = content_list[2:-1]

        mess_object = Message(table_name,src,dst,timestamp,content)
        if src == node_id:
            sent.append(mess_object)
        elif dst == node_id:
            received.append(mess_object)
        else:
            print "This message should in {3}'s space: from {0} to {1} content {2}"\
                  .format(src,dst,content,node_id)

    return received, sent

#INPUT: list of tuples as output by tuple_to_list
#       [(name,[entries]),(name,[other_entries])]
#OUTPUT:dict {table_name: ['[row]','[row],...]}
def make_tables(input_list,clas):
    input_list = tuples_to_list(input_list)
    tables = {}
    for entry in input_list:
        if entry[0] == 'neighbour':
            table_name = entry[0]
        else:
            table_name = '{0}.{1}'.format(clas,entry[0])

        row = entry[1]
        #We take out the timestamp
        if table_name in tables:
            tables[table_name].append(row)
        else:
            tables[table_name] = [row]

    return tables

def take_out_timestamp(dirty_state):
    result = {}
    for table in dirty_state:
        result[table] = []
        rows = dirty_state[table]
        for row in rows:
            result[table].append(row[:-1])
    return result

#INPUT: list of tuple_strings ["neighbour(a);", "add_neighbour(a);"]
#OUTPUT: list of input tuples [(table_name,[onerow]),...]
def find_input_tuples(input_list):
    result = []
    input_list = tuples_to_list(input_list)
    for inp in input_list:
        table_name = inp[0]
        #Don't count he peers table as an input
        if table_name == 'peers':
            continue
        
        row = inp[1]
        result.append((table_name,row))

    return result

#ACTUAL Client Thread Code
#=====================================================
class ClientThread(threading.Thread):
    def __init__(self,sock,myfile,node_id,monitor,clas):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.state_list = []
        self.sock = sock
        self.stopped = False
        self.myfile = myfile
        self.monitor = monitor
        self.clas = clas

    def stop(self):
        self.stopped = True
        self.sock.close()
        return self.node_id, self.state_list

 
    def run(self):
        prev_state = None
        first = True
        try:

            while not self.stopped:
                if not first:
                    nodeid = andreadline(self.myfile)


                tmp = andreadline(self.myfile)

                tmp = string.split(tmp)
                state_nr = int(tmp[0])
                timestamp = tmp[1]
                
                received_count = int(andreadline(self.myfile))
                raw_received = []
                for i in xrange(0,received_count):
                    raw_received.append(andreadline(self.myfile))

                input_tuples = find_input_tuples(raw_received)
    
                state_count = int(andreadline(self.myfile))
                raw_state = []
                for i in xrange(0,state_count):
                    raw_state.append(andreadline(self.myfile))
            
            #Get the state content dict
                state_tables = make_tables(raw_state,self.clas)

                sent_count = int(andreadline(self.myfile))
                raw_messages = []
                for i in xrange(0,sent_count):
                    mess = andreadline(self.myfile)
                    raw_messages.append(mess)
            
                (received,sent) = analyse_messages(raw_messages,self.node_id,self.clas)

            #Turn input tuples into message objects
                for inp in input_tuples:
                    table_name = inp[0]
                    content = inp[1]
                    src = 'admin'
                    dst = self.node_id
                    timestamp = content[-1]
                    content = content[:-1]
                    mess_obj = Message('{0}.{1}'.format(self.clas,table_name),src,dst,timestamp,content)
                    received.append(mess_obj)

            #Create State Object
                prev_state = state_tables
                clean_state_tables = take_out_timestamp(state_tables)
                state_obj = State(self.node_id,state_nr,sent,received,clean_state_tables)
                self.state_list.append(state_obj)
            
                if first:
                    while not self.monitor.started():
                        time.sleep(.2)


                self.monitor.signal_evaluation()

                while self.monitor.hit_limit():
                    time.sleep(10)

                time.sleep(1)
                self.monitor.send_changes(prev_state,state_nr)
                print "Sending RESUME to node {0}".format(self.node_id)
                
                self.sock.sendall("RESUME\n")
            
                if first:
                    self.monitor.resume_sent()

                first = False
        except ClosedSocketException:
            pass
            
    
if __name__ == "__main__":
    tuples = []
    tuples.append("neighbour(a, 3 ,4 , three);")
    tuples.append("fuck(me, side, who, dude , na);")
    tuples.append("neighbour(a, 3 ,4 , three);")
    tuples.append("fuck(me, side, who, dude , na);")
    tuples.append("peers(Neighbour, Reachable);")
    tuples.append("peers(Neighbour, Reachable);")
    print "Make Tables:", make_tables(tuples)
    
    print "Input_tuples:", find_input_tuples(tuples)


    print "Testing Messages"

    messages = []
    messages.append('advertise(n001,n002,blo,blu,yo,12994856);')
    messages.append('advertise(n001,n003,blo,blu,yo,12994856);')
    messages.append('advertise(n001,n004,blo,blu,yo,12994856);')
    messages.append('advertise(n004,n001,blo,blu,yo,12994856);')
    messages.append('advertise(n003,n001,blo,blu,yo,12994856);')

    received, sent = analyse_messages(messages,'n001')

    print "Received"
    for rec in received:
        print rec

    print "Sent"
    for sen in sent:
        print sen
