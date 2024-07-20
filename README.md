# mongodb_monitoring-python-

This is a python source that collects MongoDB monitoring metrics in JSON format.
Depending on the MongoDB Version, you can specify the serverstatus() metric name through a separate configuration file. 
The metrics collected also vary depending on mongod and mongos role. 
For MongoS, the connection pool (taskexecutorpool) is collected separately in addition to serverstatus.

MongoDB 모니터링 메트릭을 JSON 형태로 수집하는 python 소스입니다.
MongoDB Version 에 따라서 별도 설정파일을 통해서 serverstatus() 메트릭 이름을 지정하면 됩니다.
mongod 및 mongos role 에 따라서도 수집하는 메트릭이 달라집니다. 
MongoS 의 경우 serverstatus 이외에 connection pool ( taskexecutorpool)을 별도로 수집합니다.
