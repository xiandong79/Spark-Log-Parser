# Spark-Log-Parser

## 1. Set up Spark/conf with log-enable

According to the [official gudience](https://spark.apache.org/docs/latest/configuration.html), you can enable Spark to log the detailed execution infomation in `spark/confspark-defaults.conf` file.

```
spark.eventLog.enabled  true
spark.eventLog.dir	 XXX
# default is file:///tmp/spark-events
```

## 2. the log look likes
```
{"Event":"SparkListenerTaskStart","Stage ID":0,"Stage Attempt ID":0,"Task Info":{"Task ID":2,"Index":2,"Attempt":0,"Launch Time":1499522864395,"Executor ID":"11","Host":"172.31.38.103","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":0,"Failed":false,"Accumulables":[]}}
```

You can convert it by `https://jsonformatter.org`

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

## Architecture of Spark-Log-Parser
![](http://wx2.sinaimg.cn/large/006BEqu9gy1fhhg5agb26j30u00gwgn1.jpg)

## how to run this parser

```
python /Spark-Log-Parser/main.py  /Spark-Log-Parser/log-example/app-20170708141026-0013
```

## Reference
- https://github.com/kayousterhout/trace-analysis
- https://github.com/DistributedSystemsGroup/SparkEvents
