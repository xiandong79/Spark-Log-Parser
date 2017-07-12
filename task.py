# task_id = data["Task Info"]["Task ID"]
# self.tasks[task_id] = Task(data)

# End
# self.tasks[task_id].finish(data)


# self.executors[t.executor_id].task.append(t)

# if s.stage_id == t.stage_id:
# s.tasks.append(t)


# end_reason


# s += t.report(0)

from datetime import datetime


class Task:
    """
    {
      "Event": "SparkListenerTaskStart",
      "Stage ID": 0,
      "Stage Attempt ID": 0,
      "Task Info": {
        "Task ID": 1,
        "Index": 1,
        "Attempt": 0,
        "Launch Time": 1499523032866,
        "Executor ID": "7",
        "Host": "172.31.47.174",
        "Locality": "PROCESS_LOCAL",
        "Speculative": false,
        "Getting Result Time": 0,
        "Finish Time": 0,
        "Failed": false,
        "Accumulables": []
        }
    }
    """
    def __init__(self, data):
        self.task_id = data["Task Info"]["Task ID"]
        self.stage_id = data["Stage ID"]
        self.executor_id = data["Task Info"]["Executor ID"]
        self.launch_time = data["Task Info"]["Launch Time"]
        self.locality = data["Task Info"]["Locality"]
        self.speculative = data["Task Info"]["Speculative"]

        self.end_reason = None
        self.failed = False
        self.finish_time = None
        # def calc_task_times(self):
        self.getting_result_time = None
        self.index = None
        self.type = None

        self.has_metrics = False
        self.disk_spilled_bytes = None
        self.memory_spilled_bytes = None
        self.executor_deserialize_time = None
        self.executor_run_time = None
        self.jvm_gc_time = None
        self.result_serialize_time = None
        self.result_size = None

    def finish(self, data):
        """
        {
          "Event": "SparkListenerTaskEnd",
          "Stage ID": 0,
          "Stage Attempt ID": 0,
          "Task Type": "ResultTask",
          "Task End Reason": {
            "Reason": "Success"
          },
          "Task Info": {
            "Task ID": 0,
            "Index": 0,
            "Attempt": 0,
            "Launch Time": 1499523032831,
            "Executor ID": "6",
            "Host": "172.31.47.174",
            "Locality": "PROCESS_LOCAL",
            "Speculative": false,
            "Getting Result Time": 0,
            "Finish Time": 1499523035339,
            "Failed": false,
            "Accumulables": [
              {
                "ID": 0,
                "Name": "internal.metrics.executorDeserializeTime",
                "Update": 1945,
                "Value": 1945,
                "Internal": true,
                "Count Failed Values": true
              },...
        """
        self.end_reason = data["Task End Reason"]["Reason"]
        self.failed = data["Task Info"]["Failed"]
        self.finish_time = data["Task Info"]["Finish Time"]
        self.getting_result_time = data["Task Info"]["Getting Result Time"]
        self.index = data["Task Info"]["Index"]
        self.type = data["Task Type"]  # "Task Type": "ResultTask"

        if "Task Metrics" in data:
            self.has_metrics = True
            self.disk_spilled_bytes = data["Task Metrics"]["Disk Bytes Spilled"]
            self.memory_spilled_bytes = data["Task Metrics"]["Memory Bytes Spilled"]
            self.executor_deserialize_time = data["Task Metrics"]["Executor Deserialize Time"]
            self.executor_run_time = data["Task Metrics"]["Executor Run Time"]
            self.jvm_gc_time = data["Task Metrics"]["JVM GC Time"]
            self.result_serialize_time = data["Task Metrics"]["Result Serialization Time"]
            self.result_size = data["Task Metrics"]["Result Size"]


    def report(self, indent):
        pfx = " " * indent
        s = pfx + "Task {} (stage: {}, executor: {})\n".format(self.task_id, self.stage_id, self.executor_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Started at: {}\n".format(datetime.fromtimestamp(self.launch_time / 1000))
        s += pfx + "Run time: {}ms\n".format(int(self.finish_time or 0) - int(self.launch_time or 0))
        s += pfx + "End reason: {}\n".format(self.end_reason)
        s += pfx + "Locality: {}\n".format(self.locality)
        s += pfx + "Speculative: {}\n".format(self.speculative)
        s += pfx + "Type: {}\n".format(self.type)
        s += pfx + "Index: {}\n".format(self.index)
        if self.has_metrics:
            s += pfx + "Metrics:\n"
            indent += 1
            pfx = " " * indent
            s += pfx + "Spilled bytes: {}B memory, {}B disk\n".format(self.memory_spilled_bytes, self.disk_spilled_bytes)
            s += pfx + "Executor deserialize time: {}ms\n".format(self.executor_deserialize_time)
            s += pfx + "Executor run time: {}ms\n".format(self.executor_run_time)
            s += pfx + "JVM GC time: {}ms\n".format(self.jvm_gc_time)
            s += pfx + "Result serialize time: {}ms\n".format(self.result_serialize_time)
            s += pfx + "Result size: {}B\n".format(self.result_size)
        return s
