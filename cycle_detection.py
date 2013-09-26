import pickle
from HistoryObjects import Message, State, Table, GlobalState
def find_max(history):

    result = 0
    for node in history:
        state_list = history[node]
        if len(state_list) > result:
            result = len(state_list)

    return result

def get_global_state(history,i):
    dic = {}
    for node in history:
        state_list = history[node]
        prev = state_list[0]
        print "prev is {0}".format(prev)
        for x in xrange(1,len(state_list)):
            current = state_list[x]
            if current.state_nr > i:
                print "adding prev to dic for node {0}".format(node)
                dic[node] = prev
                continue
            prev = state_list[x]

    return GlobalState(dic)

    


def detect_cycle(history):
    max_state_nr = find_max(history)
    cycle_dict = {}

    for i in range(0,max_state_nr):
        #print 'Getting global state for clock {0}'.format(i)
        global_state = get_global_state(history,i)
        #print 'Global State is: {0}'.format(global_state)
        hashed = hash(global_state)
        #print "STATE HASHED TO {0}".format(hashed)
        if hashed not in cycle_dict:
            print "Inserting the global state in the cycle_dict"
            cycle_dict[hashed] = [(global_state, i)]
        else:
            #start = cycle_dict[hashed][1]
            #first_state = cycle_dict[hashed][0]
            #end = i
            #return (start,end)#If you want more,(first_state,global_state)
            cycle_dict[hashed].append((global_state, i))

    result = None
    latest = 0
    for cycle in cycle_dict.values():
        if len(cycle) > 1:
            print cycle
            tup1 = cycle[-1]
            tup2 = cycle[-2]
            end = tup1[1]
            start = tup2[1]
            if end > latest:
                latest = end
                result = (start,end)


    return result
       
      
if __name__ == "__main__":
    f = open('/homes/ap3012/individual_project/home/NewYork/history_pickle','r')
    history = pickle.load(f)
    print history
    f.close()
    print detect_cycle(history)
    '''print "Cycle found from {0} to {1}".format(start,end)
    print "State 1: {0}".format(start_state)
    print "State 2: {0}".format(end_state)
    '''
