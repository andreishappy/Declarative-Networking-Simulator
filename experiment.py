import subprocess

if __name__ == "__main__":
    for i in range(0,10):
        print "Call nr {0}".format(i+1)
        subprocess.call('python simulation.py surprise_gadget.xml output.xml', shell=True)
    
