from optparse import OptionParser
from ConfigReader import MyXMLParser
from SimulationMonitor import SimulationMonitor, DynamicChangeTypeException
from CentralController import CentralController
from Inserter import Inserter
import os, time, subprocess
import tools, socket

class Simulator:

    def __init__(self,config_file, output):
        #To redirect ouput of engines away 
        self.engine_output = open(os.devnull,'w')
        self.process_dict = {}

        self.output = output
        
        #PORT for communication
        self.port = None

        self.config = MyXMLParser(config_file)
        self.one_rule = self.config.one_rule
        #List of all the instances to start
        self.nodes = self.config.nodes
        self.limit = self.config.limit
        self.rule = self.config.rule

        #self.do_table_dicts()
 
        #List of tables by type
        if self.one_rule:
            self.tran_table_dict = tools.get_table_dict(self.rule,'transport')
            self.pers_table_dict = tools.get_table_dict(self.rule,'persistent')
        else:
            print "Can't deal with more than one rule for now"

        self.topology = self.config.topology
        self.pre_inputs = self.config.pre_inputs
        self.post_inputs = self.config.post_inputs

        print "Tran table dict\n {0}\n".format(self.tran_table_dict)
        print "Persistent table dict\n {0}\n".format(self.pers_table_dict)
        print "Pre Inputs\n {0}".format(self.pre_inputs)
        print "Post Inputs\n {0}".format(self.post_inputs)
        print "Topology\n {0}".format(self.topology)

        print "Nodes \n {0}".format(self.nodes)

        #Get the simulation monitor ready
        try:
            self.monitor = SimulationMonitor(self.limit,self.post_inputs,self.nodes)
        except DynamicChangeTypeException:
            sys.stderr.write("Wrong type for post_input")
            exit(-1)


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
            filename = 'init{0}.dsmi'.format(node_id)
            f = open(filename,'w')
            for neighbour in topology_map[node_id]:
                f.write('neighbour({0});\n'.format(neighbour))

            try:
                for inp in pre_input_map[node_id]:
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

    def kill_engines(self):
        cmd = '$PROJECT_HOME/clear_controllable.sh'
        subprocess.call(cmd,shell=True, executable='/bin/bash')
        print "Engines Killed"

    def start_central_controller(self):
        self.port = 55000
        started = False
        while not started:
            try:
                self.central_controller = CentralController(self.port,self.monitor)
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
        self.start_central_controller()

        self.start_engines()

        try:
            while 1:
                x = 1
        except KeyboardInterrupt:
            print "Hitory is:"


            history = self.central_controller.stop()
            for node in history:
                print "  {0}".format(node)
                for state in history[node]:
                    print state
            
if __name__ == "__main__":
    #Add DSMEngine to the loadpath
    os.environ['PATH'] += ':/homes/ap3012/individual_project/unzipped22/bin'
    os.environ['PROJECT_HOME'] = '/homes/ap3012/individual_project/home'
    os.environ['DSM_HOME'] = '/homes/ap3012/individual_project/unzipped22'
    
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
