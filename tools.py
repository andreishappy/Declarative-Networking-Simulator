import string
class ClosedSocketException(Exception):
    pass
#Reads the next line and take out the newline at the end 
def andreadline(stream):
    result = stream.readline()
    if not result:
        raise ClosedSocketException

    return result[:-1]


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
