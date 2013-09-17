from threading import Lock
import time, datetime
import string
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
        return "Dyn type-{0} target-{1} tuple-{2} delay-{3}"\
               .format(self.type,self.target,self.tuple,self.delay)

#HELPER FUNCTIONS FOR SIMULATION MONITOR
def matches(short,longer):
    for i in xrange(0,len(short)):
        if short[i] != longer[i]:
            return False
    return True

def longstamp(date_string):
    timestamp = datetime.datetime.strptime(date_string,'%Y-%m-%d %H:%M:%S.%f')
    timestamp = int(float(timestamp.strftime('%s.%f'))*1000)
    return str(timestamp)

class SimulationMonitor:

    def __init__(self,limit,post_inputs,nodes):
        self.limit = limit
        self.evaluations = 0
        self.evaluation_lock = Lock()
        self.last_evaluation = None
        
        self.nodes = [x[0] for x in nodes]
        self.started = 0
        self.to_start = len(self.nodes)
        self.starting_lock = Lock()
        
        self.dynamic_changes_lock = Lock()
        self.dynamic_changes = {}
        for node in self.nodes:
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
        


    # Synchronous function which does not return 
    # until both sides of a link have been deleted
    def send_changes(self,node_id,sock,prev_state):
        with self.dynamic_changes_lock:
            dynamic_changes_list = self.dynamic_changes[node_id]

        to_delete = []
        to_add = []
        wait_for = []

        #current_delay = time.time() - self.start(time)

        to_send = 'CHANGESTATE\n'
        
        for dynamic_change in dynamic_changes_list:
            #if dynamic_change.delay > current_delay:
            #    continue

            if dynamic_change.type == 'add':
                to_add.append(dynamic_change)
            elif dynamic_change.type == 'delete':
                to_delete.append(dynamic_change)

            #CHANGE THIS TO PEERS LATER
            if dynamic_change.table_name == "neighbour":

                other_node = dynamic_change.tuple[0]
                dyn = DynamicChange(dynamic_change.type,\
                                    other_node,\
                                    'neighbour({0});'.format(node_id),\
                                    dynamic_change.delay)

                wait_for.append(dyn)
                print "Added to wait_for: {0}".format(dyn)

        delete_count = 0
        to_write = []
        for change in to_delete:
            table_name = change.table_name
            row_list = change.tuple
            
            #Now find the timestamp from the previous state if it exists 
            relevant_table = prev_state[table_name]
            for state_row in relevant_table:
                if matches(row_list,state_row):
                    new_row_list = row_list + [longstamp(state_row[-1])]
                    delete_count += 1
                    line = "{0}({1});".format(table_name,','.join(row_list))
                    to_write.append(line)

                else:
                    continue

        to_send += '{0}\n'.format(delete_count)
        for write in to_write:
            to_send += '{0}\n'.format(write)

        add_count = len(to_add)
        to_send += '{0}\n'.format(add_count)
        for change in to_add:
            table_name= change.table_name
            row_list = change.tuple
            new_row_list = row_list + ['100']
            to_send+='{0}({1});\n'.format(table_name,','.join(new_row_list))
        
        print to_send
        #sock.sendall(to_send)
        self.remove_from_changes(to_add,to_delete)
        self.wait_for_others(wait_for)
       
    #IMPLEMENTATION THAT DELETES THE DYNAMIC CHANGES AFTER THEY HAVE BEEN DONE
    def remove_from_changes(self,to_add,to_delete):
        return

    #Implement
    def wait_for_others(self,wait_for):
        for change in wait_for:
            print "IN WAIT FOR\n {0}".format(change)

            #<><><><><><>                      NEXT              <><><><><><><>>#
            #CHANGE THIS TO A WHILE AND RELOAD THE LIST AT A CERTAIN INTERVAL   #
            #                                                                   #
            #with self.dynamic_changes_lock:                                    #
            #   other_node_list = self.dynamic_changes[change.target]           #
            #while change in other_node_list:                                   #
            #   time.sleep(.2)                                                  #
            #   with self.dynamic_changes_lock:                                 #
            #        other_node_list = self.dynamic_changes[change.target]      #
            #                                                                   #
            #CHECK FOR THREAD SAFETY AND IF THE LIST CHANGES COZ IT'S A POINTER #
            #####################################################################

            with self.dynamic_changes_lock:
                other_node_list = self.dynamic_changes[change.target]

                for elem in other_node_list:
                    print "In other node list {0}".format(elem)
                    if change == elem:
                        print "FOUND"
                if change in other_node_list:
                    print "Found {0}".format(change)

    def signal_start(self):
        with self.starting_lock:
            self.started += 1

    def all_started(self):
        with self.starting_lock:
            result = started == self.to_start

        if result:
            self.start_time = time.time()

        return result

    def signal_evaluation(self):
        evaluation_time = time.time()
        with self.evaluation_lock:
            self.evaluations += 1
            self.last_evaluation = evaluation_time

    def convergence_reached(self):
        return time.time() - self.last_evaluation >= 15
        
    def hit_limit(self):
        return self.evaluations >= self.limit
    
   

if __name__ == "__main__":
    #Test for send_changes message content
    post_inputs = [{'type':'sever_link', 'link':'n001;n002', 'delay':'2'},\
                   {'type':'create_link', 'link':'n001;n002', 'delay':'2'},\
                   {'type':'sever_link', 'link':'n001;n003', 'delay':'2'},\
                   {'type':'add', 'node':'n003', 'tuple':'policy(A,B,3)', 'delay':'2'},\
                   {'type':'delete', 'node':'n003', 'tuple':'policy(A,B,3)', 'delay':'2'},\
                   {'type':'delete', 'node':'n002', 'tuple':'policy(A,B,3)', 'delay':'2'}]
    node_list = [('n001','bla'),('n002','bla'),('n003','bla')]

    
    sim = SimulationMonitor(100,post_inputs,node_list)
    
    prev_state = {'neighbour':[['n002','2013-09-17 16:10:25.646'],['n003','2013-09-17 16:10:25.646']],\
                  'yo':[['my','god','100'],['what','sup','100']]}

    print "Fir n001"
    sim.send_changes('n001','socket',prev_state)

    prev_state = {'policy':[['A','B','4','2013-09-17 16:10:25.646'],\
                            ['A','B','3','2013-09-17 16:10:25.646']],
                  'neighbour':[['n001','2013-09-17 16:10:25.646'],\
                               ['n002','2013-09-17 16:10:25.646']]}

    print "For n003"
    sim.send_changes('n003','socket',prev_state)
                         

    din1 = DynamicChange('add','n001','neighbour(n002)','2')
    ls = []
    ls1 = [din1,'3']
    print "Should be false: {0}".format(bool(din1 in ls))
    print "Should be true: {0}".format(bool(din1 in ls1))
