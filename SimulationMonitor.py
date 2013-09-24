from threading import Lock
import threading
import time, datetime
import sys

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

    def resume_sent(self):
        with self.resume_lock:
            self.resumes_sent += 1
        if self.resumes_sent == self.to_send:
            self.start_time = time.time()

    def first_resume_sent(self):
        with self.resume_lock:
            result = self.resumes_sent == self.to_send
        return result


    def __init__(self,limit,post_inputs,nodes):
        self.limit = limit
        self.evaluations = 0
        self.evaluation_lock = Lock()
        self.last_evaluation = None
        
        self.start_tuples_sent = False

        self.nodes = [x[0] for x in nodes]
        self.all_started = False
        self.starting_lock = Lock()

        self.resumes_sent = 0
        self.to_send = len(nodes)
        self.resume_lock = Lock()

        self.suspended_flags = {}
        self.suspended_lock = Lock()
        self.last_state_lock = Lock()
        self.last_state = {}
        self.waiting = {}
        self.waiting_lock = Lock()
        for node in self.nodes:
            self.suspended_flags[node] = False
            self.last_state[node] = None
            self.waiting[node] = False
        

        self.pending_lock = Lock()
        self.pending_dict = {}
        
    def suspend(self,node_id):
        with self.suspended_lock:
            self.suspended_flags[node_id] = True
            
    def unsuspend(self,node_id):
        with self.suspended_lock:
            self.suspended_flags[node_id] = False

    def is_suspended(self,node_id):
        with self.suspended_lock:
            return self.suspended_flags[node_id]

    # Synchronous function which does not return 
    # until both sides of a link have been deleted
    def send_changes(self,prev_state,state_nr):
        thread = threading.current_thread()
        node_id = thread.node_id
        if not prev_state:
            sys.stderr.write('Dynamic change attempted too soon')
            exit(-1)

        with self.last_state_lock:
            self.last_state[node_id] = prev_state

        suspended = self.suspended_flags[node_id]
        self.suspended_lock.release()
        self.waiting_lock.acquire()
        if suspended:

            self.waiting[thread.node_id] = True

        while suspended:
            self.waiting_lock.release()
            time.sleep(.2)
            self.waiting_lock.acquire()
            with self.suspended_lock:
                suspended = self.suspended_flags[node_id]

        self.waiting[node_id] = False 
        self.waiting_lock.release()

        print "++++Free {0}".format(node_id)
       
    def signal_start(self):
        with self.starting_lock:
            self.started += 1

    def all_started_signal(self):
        with self.starting_lock:
            self.all_started = True


    def started(self):
        with self.starting_lock:
            res = self.all_started
        return res

#WORKING HERE===================================
    #Keeping a dict of the form:
    #   {<hashed_mess>, (bool(sent),bool(received))}
    def signal_evaluation(self,received,sent):
        for mess in received:
            hashed = hash(mess)
            with self.pending_lock:
                if hashed in self.pending_dict:
                    self.pending_dict[hashed] = (True,True)
                else:
                    self.pending_dict[hashed] = (False,True)

        for mess in sent:
            hashed = hash(mess)
            with self.pending_lock:
                if hashed in self.pending_dict:
                    self.pending_dict[hashed] = (True,True)
                else:
                    self.pending_dict[hashed] = (True,False)

        evaluation_time = time.time()
        thread = threading.current_thread()
        node_id = thread.node_id

        with self.evaluation_lock:
            self.evaluations += 1
            self.last_evaluation = evaluation_time

    def convergence_reached(self):
        for mess in self.pending_dict:
            (sent,received) = self.pending_dict[mess]
            if not (sent and received):
                return False
        return True


        
        #Time check
        #return time.time() - self.last_evaluation >= 10
        '''If the messages are correct use the following:
        with self.pending_lock:
            return self.pending == 0
        '''
    def hit_limit(self):
        return self.evaluations >= self.limit
    
    def ready_for_change(self,node_id):
        with self.waiting_lock:
            result = self.waiting[node_id]
        return result

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
