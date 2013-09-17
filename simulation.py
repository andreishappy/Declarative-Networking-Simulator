from optparse import OptionParser
from ConfigReader import MyXMLParser
from SimulationMonitor import SimulationMonitor,DynamicChangeTypeException
import os,time
import tools

class Simulator:

    def __init__(self,config_file, output):
        self.output = output
        
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
            monitor = SimulationMonitor(self.limit,self.post_inputs,self.nodes)
        except DynamicChangeTypeException:
            sys.stderr.write("Wrong type for post_input")
            exit(-1)

        
        



    def start_node(self):
        return

    def start_central_controller(self):
        return

    #|================================================|
    #|<><> MAIN FUNCTION CONTROLS THE SIMULATION ><><>|
    #|================================================|
    def run_simulation(self):
        self.start_central_controller()


        self.start_nodes()

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
