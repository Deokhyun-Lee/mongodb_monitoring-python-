import os,sys,psutil,time,threading,abc
#import requests
import pymongo
import json
import math
import re
from bson.json_util import dumps
from datetime import datetime
from optparse import OptionParser
from pprint import pprint
from pymongo import MongoClient
from pathlib import Path

# ------------------------------------------------------------------------------
# User Define Variable
# ------------------------------------------------------------------------------
# Config

config={}
mongod_metric_list=None
mongos_metric_list=None

mongod_metric_vector_list=None
mongos_metric_vector_list=None

metric_vector_value={}

mongo_data_directory=""
metrics_file_directory=""

recent_sent = 0
recent_recv = 0
recent_disk_read=0
recent_disk_write=0

IS_INIT=True

# ------------------------------------------------------------------------------
# Default Delay - 10 Seconds
metric_interval_second=10
delete_interval_hour=6

# ------------------------------------------------------------------------------
# Globla MongoDB Client

uri=""
client=None

# ------------------------------------------------------------------------------
# User Define Function
# ------------------------------------------------------------------------------
# ----------------------------------------------------------
def get_metrics_list(filename):
    lists = []
    with open(filename) as f:
        for line in f:
            lists.append(line.rstrip('\n'))
    return lists

# ----------------------------------------------------------
def flatten_dict(dd, separator='.', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dd, dict) else { prefix : dd }

# ----------------------------------------------------------
# Vector 계산 해야할 부분에 대한 처리
def cal_vector(object,metric):
    metric_return=None
    metric_value=None

    global IS_INIT
    try :
        if IS_INIT == True:
            metric_value=object[metric]
            metric_return = metric_value
        else :
            metric_value=object[metric]
            metric_return = round( (metric_value - metric_vector_value[metric]) / metric_interval_second, 2 )
    except :
        # TODO : Cloud Insight 팀에 처리 문의 해야할 예외 사항
        metric_return = None

    metric_vector_value[metric]=metric_value

    return metric_return

# Vector 계산할 것과 안할 것을 분리 로직
def get_object_finder(object,metric,metric_list):
    metric_return=None
    try:
        if metric in metric_list:
            metric_return=cal_vector(object,metric)
        else :
            metric_return=object[metric]
    except:
        metric_return=None
    return metric_return

# ----------------------------------------------------------
def delete_old_files(path_target, hour_elapsed):
    for f in os.listdir(path_target):
        f = os.path.join(path_target, f)
        if os.path.isfile(f):
            timestamp_now = datetime.now().timestamp()
            is_old = os.stat(f).st_mtime < timestamp_now - (hour_elapsed * 60 * 60)

            if is_old:
                try:
                    os.remove(f)
                    print(f, 'is deleted')
                except OSError:
                    print(f, 'can not delete')

# ----------------------------------------------------------
def get_mongo_uri(userid,password,host,port):
    uri=[]
    try:
        uri.append("mongodb://")
        uri.append(userid)
        uri.append(":")
        uri.append(password)
        uri.append("@")
        uri.append(host)
        uri.append(":")
        uri.append(str(port))
    except:
        uri=[]
    return ''.join(uri)

# ----------------------------------------------------------
def flatten_dict(dd, separator='.', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }

# ----------------------------------------------------------
def get_task_executor_pool():
    #global client
    pprint("get_task_executor_pool Running")
    db=client.get_database("admin")
    conn_pool_stats=db.command({"connPoolStats":1})
    #pprint(conn_pool_stats)

    flat_dict=flatten_dict(conn_pool_stats)
    pprint("flat_dict :")
    pprint(flat_dict)

    global regex_host_filter

    #regc = re.compile('^pools.NetworkInterfaceTL-TaskExecutorPool-([0-9]*).([a-zA-Z0-9]*):([0-9]*).inUse')
    #regc = re.compile('^pools.NetworkInterfaceTL-TaskExecutorPool-([0-9]*).(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}[^0-9]):([0-9]*).inUse')
    regc = re.compile('^pools.NetworkInterfaceTL-TaskExecutorPool-([0-9]*)\.(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):([0-9]*).inUse')

    task_executor_pool_dict={}

    for key in flat_dict.keys():
        if(regc.match(key)):
            keys = re.split('pools.NetworkInterfaceTL-TaskExecutorPool-([0-9]*).',key)
            keys = re.split('.inUse',keys[2])
            pprint(keys[0] + ":" + str(flat_dict[key]))

            if keys[0] in task_executor_pool_dict.keys():
                task_executor_pool_dict[keys[0]] = task_executor_pool_dict.get(keys[0]) + flat_dict[key]
            else:
                task_executor_pool_dict[keys[0]]=flat_dict[key]

    pprint("Get task_executor_pool_dict OK!!!!!!!!!!!")
    pprint(task_executor_pool_dict)
    return task_executor_pool_dict

# ----------------------------------------------------------
def save_mongo_stats():

    global IS_INIT

    server_info=get_mongodb_server_info()
    server_info_flatten=flatten_dict(server_info)

    monitoring_data = {}

    # 추가하고 싶은 내용 Config 에서 가지고 와서 추가
    monitoring_data['mbrNo']=config['mbrNo']
    monitoring_data['dimension']={'instanceNo':config['instanceNo'],'system':'linux'}
    #monitoring_data['clusterNo']=config['clusterNo']

    # get directory size
    global mongo_data_directory
    storage = psutil.disk_usage(mongo_data_directory)

    global recent_sent
    global recent_recv
    global recent_disk_read
    global recent_disk_write

    net = psutil.net_io_counters()
    disk = psutil.disk_io_counters()

    load1, load5, load15 = os.getloadavg()

    if IS_INIT != True:
        nic_total = (net.bytes_sent - recent_sent) + ( net.bytes_recv - recent_recv )
        nic_tx = net.bytes_sent - recent_sent
        nic_rx = net.bytes_recv - recent_recv
        disk_read = disk.read_bytes - recent_disk_read
        disk_write = disk.write_bytes - recent_disk_write
    else :
        nic_total = 0
        nic_tx = 0
        nic_rx = 0
        disk_read = 0
        disk_write = 0

    recent_sent = net.bytes_sent
    recent_recv = net.bytes_recv
    recent_disk_read = disk.read_bytes
    recent_disk_write = disk.write_bytes

    mongo_data = {
        'cpu_total': psutil.cpu_times_percent().user + psutil.cpu_times_percent().system,
        'cpu_user': psutil.cpu_times_percent().user,
        'cpu_sys': psutil.cpu_times_percent().system,
        'cpu_pct': psutil.cpu_percent(interval=None),
        #'cpu_iowait':psutil.cpu_times().iowait ,
        'cpu_load_1': load1,
        'cpu_load_5': load5,
        'cpu_load_15': load15,
        'nic_total': nic_total,
        'nic_tx': nic_tx,
        'nic_rx': nic_rx,
        'mem_pct': psutil.virtual_memory().percent,
        'mem_free': psutil.virtual_memory().available * 100 / psutil.virtual_memory().total,
        'swap_pct': psutil.swap_memory().percent,
        'disk_read': disk_read,
        'disk_write': disk_write,
        #'disk_read': ,
        #'disk_write': ,
        # ----------------------------------------------------------------------
        # Disk Usage
        # used 는 데이터 디렉터리
        # free 는 10/20/30~2TB
        'disk_mongodb_free': storage.free,
        'disk_mongodb_used': storage.used,
    }

    # --------------------------------------------------------------------------
    # MongoDB Status Append
    if config['role'] == "mongod" :
        for metric in mongod_metric_list:
            metric_data=get_object_finder(server_info_flatten,metric,mongod_metric_vector_list)
            if metric_data != None:
                #pprint(metric.replace(" ", "_")+":"+str(metric_data))
                mongo_data[metric.replace(" ", "_").replace(".","_")]=metric_data
        # --------------------------------------------------------------------------
        # WT Cache Ratio 예외처리, 0 Divide / Divide by 0
        try:
            cache_ratio = round( server_info['wiredTiger']['cache']['bytes currently in the cache'] / server_info['wiredTiger']['cache']['maximum bytes configured'], 2)
            mongo_data['wiredTiger.cache.ratio_currently_in_the_cache']=cache_ratio
        except:
            # TODO : Cloud Insight 팀에 처리 문의 해야할 예외 사항
            cache_ratio=None
            # mongo_data['wiredTiger.cache.ratio_currently_in_the_cache']=cache_ratio

        # --------------------------------------------------------------------------

    elif config['role'] == "mongos" :
        for metric in mongos_metric_list:
            metric_data=get_object_finder(server_info_flatten,metric,mongos_metric_vector_list)
            if metric_data != None:
                #pprint(metric.replace(" ", "_")+":"+str(metric_data))
                mongo_data[metric.replace(" ", "_").replace(".","_")]=metric_data

    # data 에 metric 들 저장
    monitoring_data['data']={'dataType':'system','metrics':mongo_data}

    # Debug Print
    # pprint(monitoring_data)

    # Metric Time 수집에 약간 차이가 날 수 있으므로 File 저장 직전 추가되어야 함
    monitoring_data['time']=time.time()

    # 최초 실행시에는 벡터 값 비교를 위해 저장하지 않는다.

    if IS_INIT != True:
        save_file(monitoring_data)
    else:
        IS_INIT=False

    # For Debug
    # pprint(server_info)

    return True

# ----------------------------------------------------------
def get_mongodb_server_info():
    #global client
    server_status=None
    try :
        db=client.get_database("admin")
        server_status=db.command("serverStatus",{'tcmalloc':True})
    except:
        # TODO : 서버 예외사항일 경우 Alert 관련 Function
        server_status=None
    #pprint(server_status)
    return server_status

# ----------------------------------------------------------
def save_file(monitoring_data):
    try:
        # ----------------------------------------------------------------------
        # File 저장
        # 시간
        now = datetime.now()
        # 시간 포맷
        date_time_str=now.strftime("%Y%m%d%H%M%S")
        # Json File 저장 : Python 기본 JSON 은 BSON 처리가 되지 않는다. BSON Util 이용
        global metrics_file_directory

        #pprint("Get Shard Data")
        shard_dict=get_task_executor_pool()
        #pprint("shard_dict :")
        #pprint(shard_dict)

        if config['role'] == "mongos" :
            with open (metrics_file_directory+"mongodb_"+date_time_str+".json","w") as logfile :
                #pprint(monitoring_data)
                logfile.write(dumps(monitoring_data))
                logfile.write("\n")

                for shard_dict_key in shard_dict.keys():
                    shard_save_dict={}
                    shard_save_dict['mbrNo']=config['mbrNo']
                    shard_save_dict['time']=time.time()
                    shard_save_dict['data']={
                        "metrics":{
                            "connection_pool_count":shard_dict[shard_dict_key]
                        },
                        "dimensions": {
                            "task_executor_pool":shard_dict_key,
                            "instanceNo":config['instanceNo']
                        }
                    }
                    #pprint("shard_save_dict:")
                    pprint(shard_save_dict)
                    logfile.write(dumps(shard_save_dict))
                    logfile.write("\n")

                logfile.close

        else :
            with open (metrics_file_directory+"mongodb_"+date_time_str+".json","w") as logfile :
                logfile.write(dumps(monitoring_data))
                logfile.close
        # ----------------------------------------------------------------------
        # RESTFUL API POST
        # ----------------------------------------------------------------------
        # post_url("http://localhost:8080/api/v1/dash_board_post")
        # requests.post(post_url, data=monitoring_data)

        # ----------------------------------------------------------------------
        # 최근 X 시간이 지난 파일 삭제
        # Full Path 넣어주셔도 됩니다.

        # delete_old_files(path_target='./log', hours_elapsed=6)
        # ----------------------------------------------------------------------

    except :
        dummy=None
        #pprint("Save Fail")
        #pprint(server_info)

# ----------------------------------------------------------
def parse_options():
    parser = OptionParser()
    parser.add_option("-t", action="store", type="int", dest="threadNum", default=1,help="thread count [1-N] EA")
    parser.add_option("-d", action="store", type="int", dest="delayNum", default=1,help="delay count [1-N] Second")
    parser.add_option("-c", action="store", type="str", dest="cfgFile", default=1,help="config file")
    parser.add_option("-u", action="store", type="str", dest="mongoUri", default="mongodb://localhost:27017/",help="MongoDB URI")
    (options, args) = parser.parse_args()
    return options

# ----------------------------------------------------------
class thread_work(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
        self.kill_received = False

    def run(self):
        while not self.kill_received:
            # ------------------------------------------------------------------
            # your code
            #print (self.name, " is active")
            save_mongo_stats()
            time.sleep(metric_interval_second)
            # ------------------------------------------------------------------

# ----------------------------------------------------------
def has_live_threads(threads):
    return True in [t.isAlive() for t in threads]

# ----------------------------------------------------------
def main():

    global config
    try:
        with open('./cfg/config.json') as config_file:
            config=json.load(config_file)
            pprint(config)
    except :
        pprint("Could not load config file [./cfg/config.json],Exit")
        exit

    global mongod_metric_list
    global mongos_metric_list
    global mongod_metric_vector_list
    global mongos_metric_vector_list

    try :
        mongod_metric_list=get_metrics_list('./cfg/mongod_metrics.txt')
    except:
        pprint("Could not load metric file [./cfg/mongod_metrics.txt],Exit")
        exit

    try :
        mongos_metric_list=get_metrics_list('./cfg/mongos_metrics.txt')
    except:
        pprint("Could not load metric file [./cfg/mongos_metrics.txt],Exit")
        exit

    try :
        mongod_metric_vector_list=get_metrics_list('./cfg/mongod_metric_vector_list.txt')
    except:
        pprint("Could not load metric file [./cfg/mongod_metric_vector_list.txt],Exit")
        exit

    try :
        mongos_metric_vector_list=get_metrics_list('./cfg/mongos_metric_vector_list.txt')
    except:
        pprint("Could not load metric file [./cfg/mongos_metric_vector_list.txt],Exit")
        exit

    # ----------------------------------------------------------------------
    # CMD Line Option 이나 Json Config File 에서 가지고 각종 변수 들을
    # 여기서 Global 변수로 셋팅하여 편하게 사용
    # ----------------------------------------------------------------------
    options = parse_options()
    threads = []

    global metric_interval_second
    global uri
    global delete_interval_hour
    global mongo_data_directory
    global metrics_file_directory

    uri=get_mongo_uri(config['monitoring_user'], config['monitoring_password'], config['user_hostname'], config['port'])
    #pprint('uri:'+uri)
    metric_interval_second=config['metric_interval_second']
    delete_interval_hour=config['delete_interval_hour']

    mongo_data_directory=config['mongo_data_directory']
    metrics_file_directory=config['metrics_file_directory']

# ----------------------------------------------------------
# Conncect to MongoDB
    global client

    try:
        #client=MongoClient("mongodb://admin:admin@localhost:27017/?authSource=admin")
        #pprint( "uri :" + uri + "/admin?authSource=admin" )
        client=MongoClient(uri+"/admin?authSource=admin",maxPoolSize=10)
    except pymongo.errors.ConnectionFailure as cfe:
        pprint("Could not connect to server: %s",cfe.details)
        exit

# ----------------------------------------------------------
# Get Thread Count from options
    for i in range(options.threadNum):
            thread = thread_work("Thread #" + str(i))
            thread.start()
            threads.append(thread)

    while has_live_threads(threads):
        try:
            # synchronization timeout of threads kill
            [t.join(1) for t in threads
             if t is not None and t.isAlive()]
        except KeyboardInterrupt:
            # Ctrl-C handling and send kill to threads
            print ("Sending kill to threads...")
            for t in threads:
                t.kill_received = True

    print ("Exited")
# end of function
# ----------------------------------------------------------

# ----------------------------------------------------------
if __name__ == '__main__':
   main()
