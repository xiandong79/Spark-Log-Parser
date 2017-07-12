# self.executors[bm.executor_id].block_managers.append(bm)

# self.executors[t.executor_id].task.append(t)

# also recall self.block_manager is a list []
from datetime import datetime

from utils import sizeof_fmt


class BlockManager:
    """
    {
      "Event": "SparkListenerBlockManagerAdded",
      "Block Manager ID": {
        "Executor ID": "6",
        "Host": "172.31.47.174",
        "Port": 42281
      },
      "Maximum Memory": 1508062003,
      "Timestamp": 1499523032956
    }
    """
    def __init__(self, data):
        self.maximum_memory = data["Maximum Memory"]
        self.add_timestamp = data["Timestamp"]
        self.executor_id = data["Block Manager ID"]["Executor ID"]
        print "BlockManager +1"

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "Block manager\n"
        indent += 1
        pfx = " " * indent
        s += pfx + "Executor ID: {}\n".format(self.executor_id)
        s += pfx + "Time added: {}\n".format(datetime.fromtimestamp(self.add_timestamp/1000))
        s += pfx + "Maximum memory: {}\n".format(sizeof_fmt(self.maximum_memory))
        return s
