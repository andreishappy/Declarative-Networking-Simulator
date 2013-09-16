from ClientThread import ClientThread
from CentralController import CentralController
from LocalController import LocalController
import time

if __name__ == "__main__":
    port = 40012
    threads = []

    central_controller = CentralController(port)
    central_controller.start()
    
    for i in xrange(1,4):
        local = LocalController('127.0.0.1',port,i)
        local.start()
        threads.append(local)

    for thread in threads:
        thread.join()
    
    
    history = central_controller.stop()
    print "History is {0}".format(history)
    
