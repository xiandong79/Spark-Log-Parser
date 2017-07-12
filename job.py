# class/function have been used in spark_run.py

# 1. job = Job(data) # that class Job

# 2. self.jobs[job_id] = job # record into the `dict`

# 3. j.report(0)

# 4. s.complete(data) in Class Stage

from datetime import datetime


class Job:
    def __init__(self, start_data):
        # you can find a line with event_type = "SparkListenerJobStart" and copy it into https://jsonformatter.org
        # see the hierarchical structure
        """ example of "SparkListenerJobStart"
        {
          "Event": "SparkListenerJobStart",
          "Job ID": 8,
          "Submission Time": 1499523114857,
          "Stage Infos": [
            {
              "Stage ID": 8,
              "Stage Attempt ID": 0,
              "Stage Name": "collect at KMeans.scala:446",
              "Number of Tasks": 32,
              "RDD Info": [
        """

        self.job_id = start_data["Job ID"]
        self.stages = []
        for stage_data in start_data["Stage Infos"]:
            self.stages.append(Stage(stage_data))   # class Stage
        self.submission_time = start_data["Submission Time"]

        self.result = None
        self.end_time = None

    def complete(self, data):
        """def do_SparkListenerJobEnd(self, data):
            job_id = data["Job ID"]
            self.jobs[job_id].complete(data)

            {
            "Event": "SparkListenerJobEnd",
            "Job ID": 7,
            "Completion Time": 1499523114773,
            "Job Result": {
            "Result": "JobSucceeded"
            }
            }
        """
        self.result = data["Job Result"]["Result"]
        self.end_time = data["Completion Time"]

    def report(self, indent):
        pfx = " " * indent
        # indent means 'tab'
        s = pfx + "Job {}\n".format(self.job_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Submission time: {}\n".format(datetime.fromtimestamp(self.submission_time/1000))
        s += pfx + "Run time: {}ms \n".format(int(self.end_time or 0) - int(self.submission_time))
        s += pfx + "Result: {}\n".format(self.result)
        s += pfx + "Number of stages: {}\n".format(len(self.stages))
        for stage in self.stages:
            s += stage.report(indent)
            # self.stages.append(Stage(stage_data))
        return s


class Stage:
    def __init__(self, stage_data):
        self.stage_id = stage_data["Stage ID"]
        self.details = stage_data["Details"]
        self.task_num = stage_data["Number of Tasks"]
        self.RDDs = []
        for rdd_data in stage_data["RDD Info"]:
            self.RDDs.append(RDD(rdd_data))
            # class RDD is needed
        self.name = stage_data["Stage Name"]
        self.tasks = []

        self.completion_time = None
        self.submission_time = None

    def complete(self, data):
        self.completion_time = data["Stage Info"]["Completion Time"]
        self.submission_time = data["Stage Info"]["Submission Time"]

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "Stage {} ({})\n".format(self.name, self.stage_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Number of tasks: {}\n".format(self.task_num)
        s += pfx + "Number of executed tasks: {}\n".format(len(self.tasks))
        s += pfx + "Completion time: {}ms\n".format(int(self.completion_time or 0) - int(self.submission_time or 0))
        for rdd in self.RDDs:
            s += rdd.report(indent)
        return s


class RDD:
    """
    "RDD Info": [
        {
          "RDD ID": 24,
          "Name": "MapPartitionsRDD",
          "Scope": "{\"id\":\"57\",\"name\":\"mapPartitionsWithIndex\"}",
          "Callsite": "mapPartitionsWithIndex at KMeans.scala:438",
          "Parent IDs": [
            23
          ],
          "Storage Level": {
            "Use Disk": false,
            "Use Memory": false,
            "Deserialized": false,
            "Replication": 1
          },
          "Number of Partitions": 32,
          "Number of Cached Partitions": 0,
          "Memory Size": 0,
          "Disk Size": 0
        }, ...
    """

    def __init__(self, rdd_data):
        self.rdd_id = rdd_data["RDD ID"]
        self.disk_size = rdd_data["Disk Size"]
        self.memory_size = rdd_data["Memory Size"]
        self.name = rdd_data["Name"]
        self.partitions = rdd_data["Number of Partitions"]
        self.replication = rdd_data["Storage Level"]["Replication"]

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "RDD {} ({})\n".format(self.name, self.rdd_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Size: {}B memory {}B disk\n".format(self.memory_size, self.disk_size)
        s += pfx + "Partitions: {}\n".format(self.partitions)
        s += pfx + "Replication: {}\n".format(self.replication)
        return s
