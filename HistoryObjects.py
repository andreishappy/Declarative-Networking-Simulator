import string
import datetime
import time
from collections import OrderedDict
import hashlib

#Takes instance name at initialization
class Message:
    def __init__(self,table_name,source,destination,timestamp,content):
        self.table_name = table_name
        self.src = source
        self.dest = destination
        self.time = timestamp
        self.content = content
        self.will_be_lost = 1
        self.unique_id = -1

    def __str__(self):
        #If you want to see the full header
        result = "{0} {1} ==> {2} AT {3} UID:{4}\n"\
                 .format(self.table_name,self.src,self.dest,self.time,self.unique_id)
        result += "Content: {0}".format(self.content)
        return result

    def __eq__(self,other):
        return self.table_name == other.table_name and \
               self.src == other.src and \
               self.dest == other.dest and \
               self.time == other.time and \
               self.content == other.content

    def __hash__(self):
        to_hash = "{0}{1}{2}{3}{4}"\
                  .format(self.table_name,self.src,\
                          self.dest,self.time,self.content)

        result = hash(to_hash)
        return result

class State:
    def __init__(self,instance,state_nr,sent,received,state_tables):
        self.tables = []
        self.instance = instance
        self.state_nr = state_nr

        #Lists of Message Objects (received also has input tuples)
        self.received = received
        self.sent = sent

        #Dict of {table_name:[[row],[row],...],...}
        ordered = OrderedDict()
        for table in sorted(state_tables.iterkeys()):
           ordered[table] = sorted(state_tables[table],key=lambda x: x[0])


        self.state_tables = ordered 

    def __str__(self):
        title = "State for {0} | NR. {1}:\n".format(self.instance,self.state_nr)
        for table in self.state_tables:
            title += "   Name: {0}\n".format(table)
            for row in self.state_tables[table]:
                title += "    {0}\n".format(row)

        title += "Sent messages=========\n"
        for mess in self.sent:
            title += '   ' + mess.__str__() + '\n'

        title += 'Received messages=========\n'
        for mess in self.received:
            title += '   ' + mess.__str__() + '\n'

        return title

    def __hash__(self):
        to_hash = self.state_tables.__str__()
        return hash(to_hash)
            
class Table:

    #NOTE: could use tuples for column_types
    def __init__(self,name,rows):
        self.name = name
        self.column_and_type = []
        self.rows = rows

    def __str__(self):
        result = "Table {0}\n".format(self.name)
        '''result += "Columns: "
        for column in self.column_and_type:
            result += "{0} --> {1} || ".format(column[0],column[1])
        result += "\nCONTENT\n"
        '''
        for row in self.rows:
            result += row + '\n'

        return result


class GlobalState:
    def __init__(self,dic):
        print "Initializing a global state with {0}".format(dic)
        ordered = OrderedDict()
        for node in sorted(dic.iterkeys()):
            ordered[node] = dic[node]

        self.states = ordered

    def __hash__(self):
        to_hash = ''
        for node in self.states:
            to_hash += node + '/'
            state_obj = self.states[node]
            to_hash += str(hash(state_obj)) + '//'
        print "Trying to hash: {0}".format(to_hash)
        return hash(to_hash)

    def __str__(self):
        to_return = 'Global State \n'
        for node in self.states:
            to_return += '   {0}\n'.format(node)
            state = self.states[node]
            to_return += '     {0}\n'.format(state)

        return to_return

    def __eq__(self,other):
        return self.states == other.states

#LOGGER TOOLS =====================================
#Returns a list of the names of transport tables
def get_transport_table_name_list(filename):
    f = open(filename, 'r')
        
    result = []
    for line in f:
        words = line.replace('(',' ').split()
        if words and words[0] == 'transport':
            result.append(words[1])

    return result

def remove_starting_white_space(st):
    while st[0] == ' ':
        st = st[1:len(st)]
    return st

#Returns a list of dicts of tables of the type in the argument
def get_table_dict(rule,typ):
    f = open(rule,'r')
    result = []

    for line in f:
        #Example input add_neighbour(char [128] to_add, int cost)
        if typ + ' '  == line[0:len(typ)+1]:
            table_dict = {}
            result.append(table_dict)
            first_space = line.find(' ')
            open_bracket = line.find('(')
            table_dict['name'] = line[first_space+1:open_bracket]
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
                result_columns.append(words[1])
            table_dict['columns'] = ';'.join(result_columns)

    return result


#Returns a state with received and sent messages added
def state_log_to_state(raw_state,instance,state_nr,sent,received,persistent_names):
    result = State(instance,state_nr)
    tables = string.split(raw_state,'Table: ')

    for table in tables[1:len(tables)]:
                    #Find out the name of the table
        first_comma_index = table.find(',')
        name = table[0:first_comma_index]
        if "DUMMIE" in name or name not in persistent_names or\
           "PREV" in name or name == 'peers':
           continue

        table_result = Table(name)
        
        table_descriptor_start = table.find('[') + 1
        table_descriptor_end = table.find('][')
        table_descriptor = table[table_descriptor_start:table_descriptor_end]
        column_descriptors = string.split(table_descriptor,'ColumnDescriptor: ')

        for column_descriptor in column_descriptors[1:len(column_descriptors)]:
            temp = string.split(column_descriptor,', ')
            name = string.split(temp[0],'=')[1]
            typ  = string.split(temp[1],'=')[1]
            table_result.add_column(name,typ)
                
        tuples = table[table_descriptor_end+3:len(table)]
        tuples = tuples.replace(', ','').replace('Tuple[','').replace(']','').splitlines()
        for tup in tuples:
            if tup == '' or tup == 'null':
                pass

            else:
                tup = string.split(tup,'TupleEntry: ')
                
                columns = []
                
                for column in tup[1:len(tup)-1]:
                    value = string.split(column,'=')[1]
                    columns.append(value)
                columns = ';'.join(columns)
                table_result.add_row(columns)
        result.add_table(table_result)

    #Add the messages
    result.sent = sent
    result.received = received

    return result

#Returns a list of message objects in as many tables as present
def sent_message_to_list(raw_message,state_nr):
    result = []
    '''
    table_specific_strings = raw_message.split('\n], ')
    print "Table_specific_strings: {0}".format(table_specific_strings)

    for table_specific_string in table_specific_strings:
       ''' 


    transport_tables = string.split(raw_message[1:len(raw_message)],'\n], ')
    #Transport tables separated past this point
    for transport_table in transport_tables:
        #Get the name of the table example: talk=Tupleset....
        equals = transport_table.find('=')
        
        table_name = transport_table[0:equals]
        
        #Make a list of the column names and types [(name,type),....]
        column_name_and_type_list = []
        #Get the column descriptors 
        table_descriptor_start = transport_table.find('[') + 1
        table_descriptor_end = transport_table.find('][')
        table_descriptor = transport_table[table_descriptor_start:table_descriptor_end]
        column_descriptors = string.split(table_descriptor,'ColumnDescriptor: ')
                
        for column_descriptor in column_descriptors[1:len(column_descriptors)]:
            temp = string.split(column_descriptor,', ')
            name = string.split(temp[0],'=')[1]
            typ  = string.split(temp[1],'=')[1]
            column_name_and_type_list.append((name,typ))
        #Now the (name,type) list is populated

        #get the individual messages
        tuples = transport_table[table_descriptor_end+3:len(transport_table)]
        tuples = tuples.replace(', ','').replace('Tuple[','').replace(']','').replace('}','').splitlines()
        #Each message corresponds to a tup
        for tup in tuples:
            content = ''
            tup = string.split(tup,'TupleEntry: ')
            if tup == ['']:
        
                continue

            content = []
            for i in range(1,len(tup)):
                _type = column_name_and_type_list[i-1][1]
                _name = column_name_and_type_list[i-1][0]
                value = string.split(tup[i],'=')[1]
                if _name == "DEST_INTERNAL_":
                    dest = value
                elif _name == "SRC_INTERNAL_":
                    src = value
                elif _name == "TOC_INTERNAL_":
                    timestamp = datetime.datetime.strptime(value,'%Y-%m-%d %H:%M:%S.%f')
                    timestamp = int(float(timestamp.strftime('%s.%f'))*1000)
                else:
                    content.append(value)

            content = ';'.join(content)
        
        
            #takes out nodes that are not part of the network
            if len(dest) <10:
                message = Message(state_nr,table_name,src,dest,timestamp,content)
                result.append(message)
        
    return result
        
def has_transport_table(line, tran_names):
    index_start = line.find('name=') + 5
    index_finish = line.find(', [')
    name = line[index_start:index_finish]
    return name in tran_names

def received_messages_to_list(line,state_nr):
    #print 'line =====> ' +line
    #print "RECEIVED MESSAGE"
    #print "LINE : " + line

    result = []

    index_start = line.find('name=') + 5
    index_finish = line.find(', [')
    table_name = line[index_start:index_finish]


    #Make a list of the column names and types [(name,type),....]
    column_name_and_type_list = []
        #Get the column descriptors 
    table_descriptor_start = line.find('[') + 1
    table_descriptor_end = line.find('][')
    table_descriptor = line[table_descriptor_start:table_descriptor_end]
    column_descriptors = string.split(table_descriptor,'ColumnDescriptor: ')
        
    for column_descriptor in column_descriptors[1:len(column_descriptors)]:
        temp = string.split(column_descriptor,', ')
        name = string.split(temp[0],'=')[1]
        typ  = string.split(temp[1],'=')[1]
        column_name_and_type_list.append((name,typ))
        #Now the (name,type) list is populated

    #print column_name_and_type_list
    #get the individual messages
    tuples = line[table_descriptor_end+3:len(line)]
    tuples = tuples.replace(', ','').replace('Tuple[','').replace(']','').replace('}','').splitlines()


    for tup in tuples:
        content = []
        tup = string.split(tup,'TupleEntry: ')
        if tup == ['']:
            
            continue
        for i in range(1,len(tup)):
            _type = column_name_and_type_list[i-1][1]
            _name = column_name_and_type_list[i-1][0]
            value = string.split(tup[i],'=')[1]
            if _name == "DEST_INTERNAL_":
                dest = value
            elif _name == "SRC_INTERNAL_":
                src = value
            elif _name == "TOC_INTERNAL_":
                timestamp = datetime.datetime.strptime(value,'%Y-%m-%d %H:%M:%S.%f')
                timestamp = int(float(timestamp.strftime('%s.%f'))*1000)
                
            else:
                 content.append(value)
        
        content = ';'.join(content)
        
        
            #takes out nodes that are not part of the network
        if len(dest) <10:
            message = Message(state_nr,table_name,src,dest,timestamp,content)
            result.append(message)
            #print message
        
    return result


if __name__ == "__main__":
    
    mess1 = Message('hello','me','you',100203,['yo','you','ganster'])
    mess2 = Message('hello','me','you',100203,['yo','yo','ganster'])

    mess3 = Message('hello','me','you',100203,['yo','you','ganster'])
    print hash(mess1) == hash(mess2)
    print hash(mess1) == hash(mess3)


    st1 = {'hello':[['he','is','a','bender'],['no','he','isnt','man']],\
           'man': [['she','is','hot'],['for','real','dude']],\
           'what': []}

    state1 = State('n001',1,[],[],st1)

    st2 = {'man': [['she','is','hot'],['for','real','dude']],\
           'hello':[['he','is','a','bender'],['no','he','isnt','man']],\
           'what': []}

    state2 = State('n002',1,[],[],st2)

    print hash(state1) == hash(state2)
