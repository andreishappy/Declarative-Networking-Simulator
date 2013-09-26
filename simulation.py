from optparse import OptionParser
from ConfigReader import MyXMLParser
from SimulationMonitor import SimulationMonitor
from CentralController import CentralController, DynamicChangeTypeException
from Inserter import Inserter
from lamport_transformation import lamport_transformation
import os, time, subprocess
import tools, socket
from lxml.etree import *
from result_to_xml import do_declarations,append_messages_lost,do_states
from HistoryObjects import Message, State, Table, GlobalState
import pickle
from cycle_detection import detect_cycle
#HELPER:Removes all messages received that were not from neighbours
def clean_state(state):
    neighbours = []
    try:
        n_list_of_lists = state.state_tables['neighbour']
        for one_neighbour_list in n_list_of_lists:
            neighbours.append(one_neighbour_list[0])
    except KeyError:
        pass
    
    
    received = state.received
    index = 0
    while index < len(received):
        mess = received[index]
        if mess.src not in neighbours and mess.src != 'admin':
            print "Removing Message {0}".format(received[index])
            del received[index]
        index += 1


class Simulator:

    def __init__(self,config_file, output):
        #To redirect ouput of engines away 
        self.engine_output = open(os.devnull,'w')
        self.process_dict = {}

        self.output = output
        
        #PORT for communication
        self.port = None

        self.config = MyXMLParser(config_file)
        #List of all the instances to start
        self.nodes = self.config.nodes
        self.limit = self.config.limit

        #self.do_table_dicts()
 
        self.topology = self.config.topology
        self.pre_inputs = self.config.pre_inputs
        self.post_inputs = self.config.post_inputs

        print "Pre Inputs\n {0}".format(self.pre_inputs)
        print "Post Inputs\n {0}".format(self.post_inputs)
        print "Topology\n {0}".format(self.topology)

        print "Nodes \n {0}".format(self.nodes)

        self.node_to_class = self.config.node_to_class

        #Get the simulation monitor ready
        try:
            self.monitor = SimulationMonitor(self.limit,self.post_inputs,self.nodes)
        except DynamicChangeTypeException:
            sys.stderr.write("Wrong type for post_input")
            exit(-1)


        self.table_templates = self.config.table_templates
        self.message_templates = self.config.message_templates

    def start_engines(self):
        #Make init files for initial topology and any other pre inputs
        #We have a list of tuples
        topology_map = {}
        for link_tuple in self.topology:
            if link_tuple[0] in topology_map:
                topology_map[link_tuple[0]].append(link_tuple[1])
            else:
                topology_map[link_tuple[0]] = [link_tuple[1]]
        
        '''Pre_input format
        {'instance': 'n001', 'rows': ['n004,n002n004,10', 'n004,n002n003n005n004,20'], 
         'table_name': 'add_policy'}'''
        pre_input_map = {}
        for inp in self.pre_inputs:
            instance = inp['instance']
            if instance in pre_input_map:
                pre_input_map[instance].append(inp)
            else:
                pre_input_map[instance] = [inp]
        
        for node in self.nodes:
            node_id = node[0]
            rule = node[1]
            #make the file
            filename = '/homes/ap3012/individual_project/home/NewYork/init{0}.dsmi'.format(node_id)
            f = open(filename,'w')
            for neighbour in topology_map[node_id]:
                f.write('neighbour({0});\n'.format(neighbour))

            try:
                for inp in pre_input_map[node_id]:
                    print "Inp {0}".format(inp)
                    table_name = inp['table_name']
                    for row in inp['rows']:
                        to_write = '{0}({1});\n'.format(table_name,row)
                        f.write(to_write)
            except KeyError:
                pass

            f.close()

            #Launch the subprocess
            cmd = 'icdsmengine -namespace andrei -instance {0} -controllerPort {1} -init {2} {3}'\
                  .format(node_id,self.port,filename,rule)

            f = open('engine-{0}'.format(node_id),'w')
            self.process_dict[node_id] = subprocess.Popen(cmd, shell=True,\
                                              stdout=f,\
                                              stderr=subprocess.STDOUT,\
                                              executable = '/bin/bash')
            print "Launched node {0}".format(node_id)

             
        self.monitor.all_started_signal()
        print "All engines started"

        while not self.monitor.first_resume_sent():
            time.sleep(.5)

        #Insert the start tuples
        inserters = []
        for node in [x[0] for x in self.nodes]:
            cmd = 'tuple insert andrei {0} start dummy=1'\
                .format(node)
            ins = Inserter(cmd)
            ins.start()
            inserters.append(ins)

        for ins in inserters:
            ins.join()
        print "All start tuples sent"
        self.monitor.start_tuples_sent = True

    def kill_engines(self):
        cmd = '$PROJECT_HOME/clear_controllable.sh'
        subprocess.call(cmd,shell=True, executable='/bin/bash')
        print "Engines Killed"

    def start_central_controller(self):
        self.port = 55000
        started = False
        while not started:
            try:
                self.central_controller = CentralController(self.port,self.monitor,self.node_to_class,self.post_inputs)
                print "Started the CentralController on port {0}".format(self.port)
                started = True
            except socket.error:
                self.port += 1


        #This may be a mistake
        self.central_controller.daemon = True
        self.central_controller.start()
        time.sleep(.5)
        

    #|================================================|
    #|<><> MAIN FUNCTION CONTROLS THE SIMULATION ><><>|
    #|================================================|
    def run_simulation(self):
        start_time = time.time()
        self.start_central_controller()

        self.start_engines()
       
        time.sleep(5)
        while not (self.monitor.hit_limit() or self.monitor.convergence_reached()):
            time.sleep(1)

        if self.monitor.hit_limit():
            print 'HIT LIMIT'
            self.outcome = "Hit Limit".format(self.monitor.evaluations)
            
        else:
            print "CONVERGENCE REACHED"
            self.outcome = "Converged"


        self.history = self.central_controller.stop()            
        self.kill_engines()
        #print "Before Lamport"
        #self.print_history()

        #Put in all the tables that don't already exist in the history 
        for node in self.history:
            clas = self.node_to_class[node]
            tables = self.table_templates[clas]
            table_names = []
            for table in tables:
                table_name = table['name']
                if table_name != 'neighbour':
                    table_names.append('{0}.{1}'.format(clas,table['name']))
                table_names.append('neighbour')

            state_list = self.history[node]
            for state in state_list:
                for table_name in table_names:
                    if table_name not in state.state_tables:
                        if table_name not in ['cadd_neighbour',\
                                              'cdel_neighbour',\
                                              'cadd_policy',\
                                              'cdel_policy']:
                            state.state_tables[table_name] = []

        #Remove all the messages received from nodes that weren't neighbours
        for node in self.history:
            state_list = self.history[node]
            for state in state_list:
                clean_state(state)
    
        (self.lost_list, self.nr_received, self.nr_sent), self.evaluations = lamport_transformation(self.history)
        try:
            self.percentage_lost = float((self.nr_sent - self.nr_received))/self.nr_sent
        except ZeroDivisionError:
            self.percentage_lost = 0
            
        self.percentage_lost = round(self.percentage_lost * 100,1)
            #Get this from the monitor
        self.simulation_time = time.time() - start_time
        

        #print "After Lamport"
        #self.print_history()
        

        self.history_to_file()
        self.make_xml()
        
        f = open('experiment_data','a')
        f.write('Transitions: {0} \n  Sent: {1}\n  Lost{2}'\
                .format(self.evaluations,
                        self.nr_sent,self.percentage_lost))
        f.close()

    def history_to_file(self):
        f = open('/homes/ap3012/individual_project/home/NewYork/history_pickle','w')
        pickle.dump(self.history,f)
        f.close()
        
    def make_xml(self):
        result = Element('result')
        #Get info about the declarations at the start
        messages_elem = do_declarations(self.message_templates,'messages')
        tables_elem = do_declarations(self.table_templates,'tables')

        result.append(messages_elem)
        result.append(tables_elem)

     
        messages_lost_elem = Element('messages_lost')
        messages_lost_elem.attrib['nr_sent'] = str(self.nr_sent)
        messages_lost_elem.attrib['nr_received'] = str(self.nr_received)
        messages_lost_elem.attrib['percentage_lost'] = str(self.percentage_lost)
        append_messages_lost(messages_lost_elem,self.lost_list)
        result.append(messages_lost_elem)

        cycle_result = detect_cycle(self.history)
        if cycle_result:
            start = str(cycle_result[0])
            end = str(cycle_result[1])

        outcome_elem = Element('outcome')
        outcome_elem.attrib['transitions'] = str(self.evaluations)
        outcome_elem.attrib['time'] = str(int(self.simulation_time))

        if self.outcome == "Converged":
            outcome_elem.attrib['outcome'] = "Converged"
        elif self.outcome == "Hit Limit":
            if not cycle_result:
                outcome_elem.attrib['outcome'] = "Hit Limit"
            else:
                outcome_elem.attrib['outcome'] = "Cycle ({0} - {1})".format(start,end)
        
        result.append(outcome_elem)
        
        cycle_elem = Element('cycle')
        
        if cycle_result:
            cycle_elem.attrib['cycle'] = '1'
            cycle_elem.attrib['start'] = start
            cycle_elem.attrib['end'] = end
        else:
            cycle_elem.attrib['cycle'] = '0'
                                
        result.append(cycle_elem)


        

        states_elem = do_states(self.history)
        result.append(states_elem)

        f = open(output,'w')
        res = tostring(result, xml_declaration=True, pretty_print=True)
        f.write(res)
        f.close()


    def print_history(self):
        for node in self.history:
                print "  {0}".format(node)
                for state in self.history[node]:
                    print state


if __name__ == "__main__":
    #Add DSMEngine to the loadpath
    os.environ['PATH'] += ':/homes/ap3012/individual_project/unzipped23/bin'
    os.environ['PROJECT_HOME'] = '/homes/ap3012/individual_project/home'
    os.environ['DSM_HOME'] = '/homes/ap3012/individual_project/unzipped23'
    
    print "DSM_HOME {0}".format(os.environ['DSM_HOME'])
    print "PATH {0}".format(os.environ['PATH'])
    parser = OptionParser()

    (options,args) = parser.parse_args()
    if len(args) != 2:
        print "Need to supply config file and output file"
        exit(-1)

    config = args[0]
    output = args[1]
    if config[0] == '"':
        config = config[1:len(config)-1]
    if output[0] == '"':
        output = output[1:len(output)-1]

    simulator = Simulator(config,output)
    simulator.run_simulation()
