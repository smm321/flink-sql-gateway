# Flink SQL Gateway

基于社区版分支修改的flink-sql-gateway，可适用于flink on yarn + hive metastore的环境运行Flink SQL.

##当前支持的版本

flink-1.17.1


## 新增的feature

### 新增的语法

- show create table [table_name]
- lineage [sql]
- validate [sql]
- 支持执行statement set


### 新增的接口
- #### YarnJobSubmitHandler
  #### request rul
  ```
  localhost:8083/v1/job/submit
  ```
  #### request body
  
  ```json
    {
        "cmd": "run-application -t yarn-application -Dyarn.application.queue=queue -Dyarn.application.name=job_1  --class main.class.path --parallelism 1 -Djobmanager.memory.process.size=1G -Dtaskmanager.memory.process.size=1G -Dtaskmanager.numberOfTaskSlots=1 /jarPath ",
        "dml": "insert into a select col from b"
    }
  ```  
  #### response body
  ```json
    {
        "errMsg": "",
        "applicationId": ""
    }
  ```
  

- #### YarnJobInfoHandler
  ##### request rul
  ```
  curl -X GET localhost:8083/v1/job/info
  ```
  ##### response body
  ```json
    {
    "errMsg": "",
    "data": [
        {
            "appId": "application_1691879773392_7492",
            "jobName": "job_1",
            "trackingUrl": "http://id-staging-hadoop02-10-162-34-125:8088/proxy/application_1691879773392_7492/",
            "flinkJob": {
                "jobs": [
                    {
                        "jid": "c9fbc5e7d36d4ae799adc41724a52265",
                        "name": "job_1",
                        "state": "RUNNING",
                        "starttime": 1692166584952,
                        "endtime": -1,
                        "duration": 1543563587,
                        "lastmodification": 1692166598695,
                        "tasks": {
                            "total": 1,
                            "created": 0,
                            "scheduled": 0,
                            "deploying": 0,
                            "running": 1,
                            "finished": 0,
                            "canceling": 0,
                            "canceled": 0,
                            "failed": 0,
                            "reconciling": 0,
                            "initializing": 0
                        }
                    }
                ]
            }
        }
    ]
  }

  
- #### YarnJobStopHandler
  #### request url
  ```
  curl -X POST localhost:8083/v1/job/stop
  ```
  #### request body
  ```json
    {"job_name": "job_1"}
  ```
  #### response body
  ```json
    {
      "errMsg": "",
      "data": [
        {
          "appId": "application_1692908509691_0937",
          "jobName": "job_1",
          "jobId": "aba961751cd19d7bbf262cf76e3bb5ea",
          "savepointUrl": "hdfs:///checkpoint/job_1/savepoint-aba961-474dff935940"
        }
      ]
    }
  ```


- #### YarnSessionHandler
    #### request url
    ```
       curl -X POST localhost:8083/v1/job/session
    ```
    #### request body
    ```json
    {
      "cmd": "-s 4 -jm 4096 -tm 4096 -nm flink-session-cluster -d -Dyarn.application.queue=queue",
      "dml":""
    }
    ```
    #### response body
    ```json
    {
        "errMsg": "",
        "applicationId": ""
    }
    ```

- #### SavepointHandler
  #### request url
    ```
       curl -X POST localhost:8083/v1/job/savepoint
    ```
  #### request body
    ```json
    {
      "job_name": "job_1"
    }
    ```
  #### response body
    ```json
    {
        "errMsg": "",
        "data": [
            {
                "appId": "application_1693600076624_0949",
                "jobName": "job_1",
                "jobId": "aba961751cd19d7bbf262cf76e3bb5ea",
                "savepointUrl": "hdfs:///checkpoint/job_1/savepoint-cc157a-e2309081942a"
            }
        ]
    }
    ```

- #### JarUploadHandler
  #### request url
    ```
    curl -X POST localhost:8083/v1/jars/upload
    ```
  #### request body (form-data)
    
    ```
    UPLOADED_FILES /jar_local_url
    request {"job_name": "job_1"}
    ```
  #### response body
    ```json
    {
        "filename": "/tmp/job_1/jar_local_url.jar",
        "status": "success"
    }
    ```
  
## 运行
