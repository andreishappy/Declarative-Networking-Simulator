try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import string

def remove_starting_white_space(st):
    return st.strip()

#Returns a list of dicts of tables of the type in the argument
def get_table_dict(rule,typ):
    f = open(rule,'r')
    result = []

    for line in f:
        #Example input add_neighbour(char [128] to_add, int cost)
        if typ in line:
            table_dict = {}
            result.append(table_dict)
            first_space = line.find(' ')
            open_bracket = line.find('(')
            table_dict['name'] = line[first_space+1:open_bracket].strip()
            result_columns = []

            #get something of the form: "char [128] to_add, int cost"
            in_bracket = line[open_bracket+1:len(line)].replace(');','')
            columns = string.split(in_bracket,',')
            for col in columns:
                col = remove_starting_white_space(col)
                words = col.split()
                
                #check for space after char
                if len(words) == 3:
                    words[0]+= words[1]#the type is currently lost
                    words[1] = words[2]
                #print words
                result_columns.append(words[1])
            table_dict['columns'] = ';'.join(result_columns)

    return result


#Could use
#    for elem in tree.iter(tag='node'):
#        print elem.tag, elem.attrib
#To find all the nodes in the whole document
class MyXMLParser:
    def __init__(self, config):
        self.tree = ET.parse(config)
        self.tree = self.tree.getroot()
        self.nodes = []
        self.hosts = {}
        self.limit = 0
        self.topology = []
        self.pre_inputs = []
        self.post_inputs = []
        self.node_to_class = {}

        self.class_to_rule = {}
        for element in self.tree:
            if element.tag == 'nodes':
                nodes = element
            elif element.tag == 'limit':
                self.limit=int(element.attrib['lim'])
            elif element.tag == 'topology':
                topology = element
            elif element.tag == 'pre_inputs':
                pre_inputs = element
            elif element.tag == 'post_inputs':
                post_inputs = element
            elif element.tag == 'classes':
                for clas in element:
                    self.class_to_rule[clas.attrib['name']] = clas.attrib['rule']
                
        for inp in post_inputs:
            input_result = {}
            for key in inp.attrib:
                input_result[key] = inp.attrib[key]
            self.post_inputs.append(input_result)

        for inp in pre_inputs:
            input_result = {}
            for key in inp.attrib:
                input_result[key] = inp.attrib[key]

            input_result['rows'] = []
            for row in inp:
                input_result['rows'].append(row.attrib['content']) 
            self.pre_inputs.append(input_result)

        for link in topology:
            link_result = (link.attrib['node1'],link.attrib['node2'])
            self.topology.append(link_result)
            
        for node in nodes:
            clas = node.attrib['class']
            rule = self.class_to_rule[clas]
            node_id = node.attrib['id']
            self.nodes.append((node_id,rule))
            self.node_to_class[node_id] = clas

        #Make the template dictionaries
        self.table_templates = {}
        self.message_templates = {}
        for clas in self.class_to_rule:
            rule = self.class_to_rule[clas]
            to_add = get_table_dict(rule,'persistent')
            self.table_templates[clas] = to_add
            

            to_add2 = get_table_dict(rule,'input') + get_table_dict(rule,'transport')
            self.message_templates[clas] = to_add2
        print "Templates"
        print self.table_templates
        print self.message_templates
    
    


if __name__ == '__main__':
    config = XMLParser('topology.xml')
    print 'nodes are \n',config.nodes
    print 'hosts are \n',config.hosts
    print 'limit is ', config.limit
