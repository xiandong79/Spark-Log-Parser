
```
{"Event":"SparkListenerTaskStart","Stage ID":0,"Stage Attempt ID":0,"Task Info":{"Task ID":2,"Index":2,"Attempt":0,"Launch Time":1499522864395,"Executor ID":"11","Host":"172.31.38.103","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":0,"Failed":false,"Accumulables":[]}}
```

https://jsonformatter.org

```
{
  "Event": "SparkListenerTaskStart",
  "Stage ID": 0,
  "Stage Attempt ID": 0,
  "Task Info": {
    "Task ID": 2,
    "Index": 2,
    "Attempt": 0,
    "Launch Time": 1499522864395,
    "Executor ID": "11",
    "Host": "172.31.38.103",
    "Locality": "PROCESS_LOCAL",
    "Speculative": false,
    "Getting Result Time": 0,
    "Finish Time": 0,
    "Failed": false,
    "Accumulables": []
  }
}
```

```
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
        },
   ...     
```
