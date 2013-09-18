from threading import *
import subprocess

class Inserter(Thread):
    def __init__(self, cmd):
        Thread.__init__(self)
        self.cmd = cmd

    def run(self):
        success = False
        while not success:
            p = subprocess.Popen(self.cmd, shell=True,
                                 stderr=subprocess.PIPE,\
                                 stdout=subprocess.PIPE,\
                                 executable = '/bin/bash')

            stdout = p.communicate()[0]
            
            stdout = stdout.splitlines()
            for line in stdout:
                if 'Tuples sent' in line:
                    print "INSERTED: {0}".format(self.cmd)
                    success = True
                    continue
                if 'Tuples NOT sent' in line:
                    print "FAILED: {0}".format(self.cmd)
                    continue
                             

                          
