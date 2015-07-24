#!/usr/bin/python

import sys, random, time, base64
from boto import kinesis
import hashlib
import json
import time
import logging

aws_access_key_id       = ''
aws_secret_access_key   = ''
shard_count     = 1
shard_name      = 'demo'
region      = 'eu-west-1'


###############################################
#       logging
###############################################
logging.basicConfig(stream = sys.stdout, level=logging.INFO, format=' -> %(message)s')




def connect_kinesis():
        auth = {"aws_access_key_id":aws_access_key_id, "aws_secret_access_key":aws_secret_access_key}
        try:
                conn = kinesis.connect_to_region(region,**auth)
                return conn
        except Exception as e:
                print('KINESIS connection error: '+str(e))
                sys.exit(1)


def read_data(conn):
        shard_id = 'shardId-000000000000' #we only have one shard!

        shard_it = conn.get_shard_iterator(shard_name, shard_id, "TRIM_HORIZON")["ShardIterator"]
        while 1==1:
                try:
                        logging.info('get_records')
                        out = conn.get_records(shard_it, limit=10)
                        shard_it = out["NextShardIterator"]

                        #logging.info('records (full)')
                        #print out
                        logging.info('records (pretty)')
                        print json.dumps(out["Records"], sort_keys=True, indent=4, separators=(',', ': '))

                        if out["MillisBehindLatest"] == 0:
                                print 'catched up latest data, resting a bit'
                                time.sleep(5)
                        else:
                                time.sleep(0.1)
                except KeyboardInterrupt:
                        print "Bye"
                        sys.exit()


if __name__ == "__main__":
	conn=connect_kinesis()
	read_data(conn)
