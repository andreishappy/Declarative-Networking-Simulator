from lxml.etree import *

def do_declarations(dic,tag):
    result = Element(tag)
    for clas in dic:
        for table in dic[clas]:
            template_elem = Element('template')
            table_name = table['name']
            columns = table['columns']
            if table_name != 'neighbour':
                table_type = '{0}.{1}'.format(clas,table_name)
            else:
                table_type = table_name
            template_elem.attrib['type'] = table_type
            template_elem.attrib['name'] = table_name
            template_elem.attrib['columns'] = columns
            template_elem.attrib['separator'] = ';'
            result.append(template_elem)
    return result

def append_messages_lost(messages_lost_elem,lost_list):
    for mess in lost_list:
        mess_elem = Element('message')
        mess_elem.attrib['from'] = mess.src
        mess_elem.attrib['to'] = mess.dest
        mess_elem.attrib['type'] = mess.table_name
        mess_elem.attrib['state_nr'] = str(mess.state_nr)
        mess_elem.attrib['content'] = ';'.join(mess.content)
        messages_lost_elem.append(mess_elem)

def do_state_messages(mess_list,tag):
    result = Element(tag)
    for mess in mess_list:
        mess_elem = Element('message')
        result.append(mess_elem)
        mess_elem.attrib['content'] = ';'.join(mess.content)
        mess_elem.attrib['type'] = mess.table_name
        #Unnecessary - provides no info for the user
        #mess_elem.attrib['timestamp'] = str(mess.time)
        mess_elem.attrib['unique_id'] = str(mess.unique_id)
        if tag == 'sent':
            mess_elem.attrib['will_be_lost'] = str(mess.will_be_lost)
        mess_elem.attrib['to'] = mess.dest
        mess_elem.attrib['from'] = mess.src
    return result

def do_state_content(table_dic):
    result = Element('content')
    for table in table_dic:
        table_elem = Element('table')
        result.append(table_elem)
        table_elem.attrib['type'] = table
        for row in table_dic[table]:
            row_elem = Element('row')
            row_elem.attrib['content'] = ';'.join(row)
            table_elem.append(row_elem)
    
    return result

def do_one_state(state):
    result = Element('state')
    result.attrib['id'] = str(state.state_nr)
    
    result.append(do_state_messages(state.sent,'sent'))
    result.append(do_state_messages(state.received,'received'))
    result.append(do_state_content(state.state_tables))

    return result

def do_states(node_state_dic):
    result = Element('nodes')
    for node in node_state_dic:
        node_elem = Element('node')
        node_elem.attrib['id'] = node
        state_list = node_state_dic[node]
        states_elem = Element('states')
        node_elem.append(states_elem)
        result.append(node_elem)

        for state in state_list:
            states_elem.append(do_one_state(state))

    return result
