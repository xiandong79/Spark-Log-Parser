#exec_id = data["Executor ID"]
#self.executors[exec_id] = Executor(data)



#exec_id = data["Executor ID"]
#self.executors[exec_id].remove(data)


#self.executors[bm.executor_id].block_managers.append(bm)
#self.executors[t.executor_id].task.append(t)

#for e in self.executors.values():
#    s += e.report(0)

from datetime import datetime
from numpy import array

class Executor:
    """
        {
          "Event": "SparkListenerExecutorAdded",
          "Timestamp": 1499523032826,
          "Executor ID": "6",
          "Executor Info": {
            "Host": "172.31.47.174",
            "Total Cores": 1,
            "Log Urls": {
              "stdout": "http://ec2-52-32-91-150.us-west-2.compute.amazonaws.com:8081/logPage/?appId=app-20170708141026-0013&executorId=6&logType=stdout",
              "stderr": "http://ec2-52-32-91-150.us-west-2.compute.amazonaws.com:8081/logPage/?appId=app-20170708141026-0013&executorId=6&logType=stderr"
            }
          }
        }
    """
    def __init__(self, data):
        self.executor_id = data["Executor ID"]
        self.host = data["Executor Info"]["Host"]
        self.block_managers = [] # block_managers belong to
        self.start_timestamp = data["Timestamp"]
        self.total_cores = data["Executor Info"]["Total Cores"]
        self.tasks = []

        self.remove_reason = None
        self.remove_timestamp = None
        print "Executor +1"

    def add_block_manager(self, bm):
        self.block_managers.append(bm)

    def remove(self, data):
        self.remove_reason = data["Removed Reason"]
        self.remove_timestamp = data["Timestamp"]

    def calc_task_times(self):
        runtimes = []
        for t in self.tasks:
            runtimes.append(int(t.finish_time or 0) - int(t.launch_time or 0))
        runtimes = array(runtimes)
        return runtimes.mean(), runtimes.std(), runtimes.min(), runtimes.max()

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "Executor {}\n".format(self.executor_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Host: " + self.host + "\n"
        s += pfx + "Total cores: {}\n".format(self.total_cores)
        s += pfx + "Started at: {}\n".format(datetime.fromtimestamp(self.start_timestamp/1000))
        if self.remove_timestamp is not None:
            s += pfx + "Run time: {}ms\n".format(self.remove_timestamp - self.start_timestamp)
        s += pfx + "Number of block managers: {}\n".format(len(self.block_managers))
        s += pfx + "Number of executed tasks: {}\n".format(len(self.tasks))
        
        avgrt, stdrt, minrt, maxrt = self.calc_task_times()
        s += pfx + "Average task runtime: {} (stddev {})\n".format(avgrt, stdrt)
        s += pfx + "Min/max task time: {} min, {} max\n".format(minrt, maxrt)
        s += pfx + "Termination reason: {}\n".format(self.remove_reason)
        return s
