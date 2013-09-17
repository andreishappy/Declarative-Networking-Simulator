try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import string

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
        self.one_rule = False
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
            elif element.tag == 'rule':
                self.rule=element.attrib['filename']
            elif element.tag == 'one_rule':
                if element.attrib['one_rule'] == '1':
                    self.one_rule = True

        for inp in post_inputs:
            input_result = {}
            for key in inp.attrib:
                input_result[key] = inp.attrib[key]
            self.post_inputs.append(input_result)

        for inp in pre_inputs:
            input_result = {}
            for key in inp.attrib:
                input_result[key] = inp.attrib[key]

            if inp.attrib['type'] == 'csv':
                input_result['rows'] = []
                for row in inp:
                    input_result['rows'].append(row.attrib['content']) 
            self.pre_inputs.append(input_result)

        for link in topology:
            link_result = (link.attrib['node1'],link.attrib['node2'])
            self.topology.append(link_result)
            
        for node in nodes:
            if self.one_rule:
                self.nodes.append((node.attrib['id'],self.rule))
            else:
                self.nodes.append((node.attrib['id'],node.attrib['rule']))

if __name__ == '__main__':
    config = XMLParser('topology.xml')
    print 'nodes are \n',config.nodes
    print 'hosts are \n',config.hosts
    print 'limit is ', config.limit
