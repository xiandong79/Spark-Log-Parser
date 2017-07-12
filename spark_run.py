from datatime import datatime

import numpy as np

from executor import Executor
from job import Job
from block_manager import BlockManager
from task import Task
from utils import get_json

class SparkRun:
    def __init__(self, filename):
        self.filename = filename
        self.parsed_data = {}  #empty dicts.
        self.executors = {}
        self.jobs = {}
        self.tasks = {}
        self.block_managers= [] # empty lists.

        file = open(filename, "r")

        for line in file:
            json_data = get_json(line)
            event_type == json_data["Event"]

            # 13 event types
            if event_type == "SparkListenerLogStart":
                self.do_SparkListenerLogStart(json_data)
            elif event_type == "SparkListenerBlockManagerAdded":
                self.do_SparkListenerBlockManagerAdded(json_data)
            elif event_type == "SparkListenerEnvironmentUpdate":
                self.do_SparkListenerEnvironmentUpdate(json_data)
            elif event_type == "SparkListenerApplicationStart":
                self.do_SparkListenerApplicationStart(json_data)
            elif event_type == "SparkListenerJobStart":
                self.do_SparkListenerJobStart(json_data)
            elif event_type == "SparkListenerStageSubmitted":
                self.do_SparkListenerStageSubmitted(json_data)
            elif event_type == "SparkListenerExecutorAdded":
                self.do_SparkListenerExecutorAdded(json_data)
            elif event_type == "SparkListenerTaskStart":
                self.do_SparkListenerTaskStart(json_data)
            elif event_type == "SparkListenerTaskEnd":
                self.do_SparkListenerTaskEnd(json_data)
            elif event_type == "SparkListenerExecutorRemoved":
                self.do_SparkListenerExecutorRemoved(json_data)
            elif event_type == "SparkListenerBlockManagerRemoved":
                self.do_SparkListenerBlockManagerRemoved(json_data)
            elif event_type == "SparkListenerStageCompleted":
                self.do_SparkListenerStageCompleted(json_data)
            elif event_type == "SparkListenerJobEnd":
                self.do_SparkListenerJobEnd(json_data)
            else:
                print("WARNING: unknown event type: " + event_type)

        def do_SparkListenerLogStart(self, data):
            self.parsed_data["spark_version"] = data["Spark Version"]
            # parsed_data is a empty dict.

        def do_SparkListenerBlockManagerAdded(self, data):
            bm = BlockManager(data)
            self.block_managers.append(bm)
            # block_managers is a empty list

        def do_SparkListenerEnvironmentUpdate(self, data):
            self.parsed_data["java_version"] = data["JVM Information"]["Java Version"]
            self.parsed_data["app_name"] = data["Spark Properties"]["spark.app.name"]
            self.parsed_data["app_id"] = data["Spark Properties"]["spark.app.id"]
            self.parsed_data["driver_memory"] = data["Spark Properties"]["spark.driver.memory"]
            self.parsed_data["executor_memory"] = data["Spark Properties"]["spark.executor.memory"]
            self.parsed_data["commandline"] = data["System Properties"]["sun.java.command"]

        def do_SparkListenerApplicationStart(self, data):
            self.parsed_data["app_start_timestamp"] = data["Timestamp"]


        def do_SparkListenerJobStart(self, data):
            job_id = data["Job ID"]
            if job_id in self.jobs:
                print("ERROR: Duplicate job ID!")
                return
            job = Job(data) # that class Job
            self.jobs[job_id] = job # record into the `dict`


def correlate

def generate_report

def get_app_name
