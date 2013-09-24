#from logger_tools import *

#Iterates through received messages to find the one
#we are looking for
def has_received(state,mess,unique_id):
    for received in state.received:
        if mess == received:
            if unique_id != -1:
                received.unique_id = unique_id
            return True
    return False

#Starts from one sent message and moves down list of
#the receiving node. If it finds any sent messages by
#the receiving node, then it moves donw the tree of
#that one
def two_way_recurse(node_state_dict, node_state_list, current_time):
    for src_state in node_state_list:
        src = node_state_list[0].instance
        #print "In the list for {0}".format(src)

        if current_time > src_state.state_nr:
            #print "incrementing state_nr of {0} | {1} ==> {2}"\
            #       .format(src,src_state.state_nr,current_time)
            src_state.state_nr = current_time
        elif current_time == src_state.state_nr:
            break
        else:
            #print 'broke here'
            break
        for sent_message in src_state.sent:
            #print "Checking message"
            #print sent_message
            dest = sent_message.dest
            try:
                dest_node_state_list = node_state_dict[dest]

                for i in range(0,len(dest_node_state_list)):
                    if has_received(dest_node_state_list[i],sent_message,-1):
                    #print 'calling two_way_recurse on destination {0} with current time {1}'\
                        #       .format(dest,current_time)
                        two_way_recurse(node_state_dict,dest_node_state_list[i:],current_time+1)
               

            except KeyError:
                pass
            
  
        current_time += 1

def find_mistery_received(node_state_dict):
    for node in node_state_dict:
        state_list = node_state_dict[node]
        for state in state_list:
            for mess in state.received:
                if mess.unique_id == -1 and mess.src != 'admin':
                    print "FOUND MISTERY"

def find_lost_messages(node_state_dict):
    result = []
    for node in node_state_dict:
        state_list = node_state_dict[node]
        for state in state_list:
            for sent_message in state.sent:
                if sent_message.will_be_lost == 1:
                    sent_message.state_nr = state.state_nr
                    result.append(sent_message)

    #Now sort them
    return sorted(result, key=lambda x: x.state_nr)

def give_unique_ids(node_state_dict):
    unique_id = 0
    for src in node_state_dict:
        #print "TOP LEVEL SRC is {0}".format(src)
        #print "============"
        #Iterate through every state looking for sent messages
        src_state_list = node_state_dict[src]
    
        for src_state in src_state_list:

            #For every sent message find the receiver state and
            #change the state number accordingly
            for sent_message in src_state.sent:
                #Give a unique_id to the sent message
                sent_message.unique_id = unique_id
                dest = sent_message.dest
                try:
                    dest_state_list = node_state_dict[dest]
                    #Iterate through states until 
                    for i in range(0,len(dest_state_list)):
                        if has_received(dest_state_list[i],sent_message,unique_id):
                            sent_message.will_be_lost = 0
                            break

                except KeyError:
                    pass

                #Prepare the unique_id for the next sent message
                unique_id += 1
    #Tests if the state which contain messages with a -1 unique
    # id are identical to the ones before
def minus_one_test(history):
    for node in history:
        state_list = history[node]
        prev = state_list[0]
        for i in xrange(1,len(state_list)):
            current = state_list[i]
            for mess in current.received:
                if mess.unique_id == -1 and mess.src != "admin":
                    
                    if current.state_tables == prev.state_tables:
                        print "Prev is the same"
                    else:
                        print "Prev is different"
                        print "Comparing states: \n{0} \nAND\n{1}".format(prev,current)

                        
                    if current.received == prev.received:
                        print "Trigger is the same as prev"
                    else:
                        print "Trigger is different to prev"
            prev = state_list[i]

def delete_minus_one(history):
    for node in history:
            state_list = history[node]
            to_del = []
            for i in xrange(0,len(state_list)):
                current = state_list[i]
                for mess in current.received:
                    if mess.unique_id == -1 and mess.src != "admin":
                        print "added {0} to to_del, len is {1}".format(i,len(state_list))
                        to_del.append(i)

            to_del.reverse()
            for index in to_del:
                print "trying to del {0}, len is {1}".format(index,len(state_list))
                del state_list[index]

def old_lamport(node_state_dict):
    messages_received = 0
    messages_sent = 0
    for src in node_state_dict:
        #print "TOP LEVEL SRC is {0}".format(src)
        #print "============"
        #Iterate through every state looking for sent messages
        src_state_list = node_state_dict[src]
    
        for src_state in src_state_list:

            #For every sent message find the receiver state and
            #change the state number accordingly
            for sent_message in src_state.sent:
                #Give a unique_id to the sent message
                dest = sent_message.dest
                messages_sent += 1
                try:
                    dest_state_list = node_state_dict[dest]
                    #Iterate through states until 
                    for i in range(0,len(dest_state_list)):
                        if has_received(dest_state_list[i],sent_message,-1):
                            print "Going down for mess to {0} at time {1}".format(dest,src_state.state_nr)
                            messages_received += 1
                            two_way_recurse(node_state_dict,dest_state_list[i:],src_state.state_nr+1)
                            break

                except KeyError:
                    pass

    print "Messages Received {0}".format(messages_received)
    print "Messages Sent {0}".format(messages_sent)

    messages_lost = find_lost_messages(node_state_dict)
    find_mistery_received(node_state_dict)
    return messages_lost, messages_received, messages_sent

def old_old_lamport_transformation(node_state_dict):
    messages_received = 0
    messages_sent = 0
    unique_id = 0
    for src in node_state_dict:
        #print "TOP LEVEL SRC is {0}".format(src)
        #print "============"
        #Iterate through every state looking for sent messages
        src_state_list = node_state_dict[src]
    
        for src_state in src_state_list:

            #For every sent message find the receiver state and
            #change the state number accordingly
            for sent_message in src_state.sent:
                #Give a unique_id to the sent message
                sent_message.unique_id = unique_id
                dest = sent_message.dest
                messages_sent += 1
                try:
                    dest_state_list = node_state_dict[dest]
                    #Iterate through states until 
                    for i in range(0,len(dest_state_list)):
                        if has_received(dest_state_list[i],sent_message,unique_id):
                            #print "Going down for mess to {0} at time {1}".format(dest,src_state.state_nr)
                            messages_received += 1
                            sent_message.will_be_lost = 0
                            two_way_recurse(node_state_dict,dest_state_list[i:],src_state.state_nr+1)
                            break

                except KeyError:
                    pass

                #Prepare the unique_id for the next sent message
                unique_id += 1
    print "Messages Received {0}".format(messages_received)
    print "Messages Sent {0}".format(messages_sent)

    messages_lost = find_lost_messages(node_state_dict)
    find_mistery_received(node_state_dict)
    return messages_lost, messages_received, messages_sent

def renumber(history):
    for node in history:
        state_list = history[node]
        for i in xrange(0,len(state_list)):
            current = state_list[i]
            current.state_nr = i

def count_states(node_state_dict):
    result = 0
    for node in node_state_dict:
        state_list = node_state_dict[node]
        result += len(state_list)
    return result


#Changes the dict in place
def lamport_transformation(node_state_dict):
    give_unique_ids(node_state_dict)
    delete_minus_one(node_state_dict)
    renumber(node_state_dict)
    return old_lamport(node_state_dict), count_states(node_state_dict)
    
    #return old_old_lamport_transformation(node_state_dict)




if __name__ == '__main__':

    A = []
    B = []
    C = []
    D = []
    ABCD = {'A':A, 'B':B, 'C':C, 'D':D}

    for i in range(0,5):
        for let in ['A','B','C','D']:

            state = State(let,i)
            ABCD[let].append(state)


    messBA = Message(0,'advertise','B','A','2013-08-05 11:36:58.124','')
    messAB = Message(0,'advertise','A','B','2013-08-05 11:36:58.300','')
    messAC = Message(0,'advertise','A','C','2013-08-05 11:36:59.123','')
    messCD = Message(0,'advertise','C','D','2013-08-05 11:36:59.133','') 
    messDC = Message(0,'advertise','D','C','2013-08-05 11:33:32.155','') 
    messDA = Message(0,'advertise','D','A','2013-08-05 11:33:32.178','') 


    A[1].received.append(messBA)
    A[1].sent.append(messAC)
    A[4].sent.append(messAB)

    B[2].sent.append(messBA)
    B[3].received.append(messAB)

    C[2].received.append(messAC)
    C[4].sent.append(messCD)
    C[3].received.append(messDC)
    
    D[1].sent.append(messDC)
    D[2].received.append(messCD)
    

    D[4].sent.append(messDA)
    A[2].received.append(messDA)

    for let in ABCD:
        print "States of {0}".format(let)
        for state in ABCD[let]:
            print state

    lamport_transformation(ABCD)

    for let in ABCD:
        print "States of {0}".format(let)
        for state in ABCD[let]:
            print state
