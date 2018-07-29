import redis 
import time
import os

'''LISTENER SCRIPT that has the find speaker, logic'''

REDIS_SETUP = {
'host' : os.environ['REDIS_ENDPOINT'],
'port': os.environ['REDIS_PORT'],
'db': 0,
}

REDIS_SPEAKER_FLAG_NAMESPACE = 'speaker'
REDIS_TIMEOUT_FLAG_NAMESPACE = 'timeout'
REDIS_QUEUE_NAME = 'matching'

LAMBDA_TIMEOUT_SECONDS = 60
LAMBDA_WAIT_SPEAKER_SECONDS = 3

USER_QUERY_STRING = os.environ['USER_QUERY_STRING']

RESPONSE_STATUS_CODE = 'status_code'
RESPONSE_ERROR_MESSAGE = 'error_message'
RESPONSE_DATA = 'data'

def handler(event, context):
    rq = RedisQueue(name=REDIS_QUEUE_NAME, **REDIS_SETUP)

    # if there is no user in the url query string stop the logic 
    if USER_QUERY_STRING not in event or event[USER_QUERY_STRING].__eq__(""):
        return {RESPONSE_STATUS_CODE: 400, RESPONSE_ERROR_MESSAGE: 'No user provided in the query string', RESPONSE_DATA: ''}

    # get the user from the url query string
    listener = event[USER_QUERY_STRING] 

    # create the hashtable namespaced key
    listener_key = f'{REDIS_SPEAKER_FLAG_NAMESPACE}:{listener}'
    print(listener_key)

    # add the listener in the queue
    rq.put(listener)

    # get a reference in time
    time_reference = time.time()

    # wait for the listener to find a speaker -> the speaker will add a flag to announce the listener that has been grabbed
    speaker = rq.get_redis_client().get(listener_key)
    print("Initial speaker: ", speaker)
    time_pass_out = False

    while speaker is None or speaker.decode('utf-8').__eq__(""):
        
        # check if the time passed -> if not wait for the speaker flag
        if time_reference + LAMBDA_WAIT_SPEAKER_SECONDS > time.time():
            speaker = rq.get_redis_client().get(listener_key)
            print('Wait: ', speaker)
        else:
            # add a flag that this listener timeout and is no longer available
            time_passed_out = True

            # if the data at time_out_key is 0 it means that the listener is available else 
            # the number cached in the redis[time_out_key] shows the number of timed out listeners
            time_out_key = f'{REDIS_TIMEOUT_FLAG_NAMESPACE}:{listener}'
            if rq.get_redis_client().get(time_out_key) is None:
                rq.get_redis_client().set(time_out_key, 1)
            else:
                rq.get_redis_client().set(time_out_key, 
                    get_int_from_redis_hashtable(rq.get_redis_client(), time_out_key) + 1)
                 
            # if the speaker was added in this case is too late but we still have to cleanup the flag   
            if rq.get_redis_client().get(listener_key) is not None and not rq.get_redis_client().get(listener_key).__eq__(""):
                # delete the flag after the matching has been made
                rq.get_redis_client().set(listener_key, "")
                
            # time passed so get out of the loop
            return {RESPONSE_STATUS_CODE: 200, RESPONSE_ERROR_MESSAGE: 'No speaker', RESPONSE_DATA: ''}

   
    if speaker is not None and not speaker.decode('utf-8').__eq__(""):
        # delete the flag after the matching has been made
        rq.get_redis_client().set(listener_key, "")
        
        # return the speaker
        return {RESPONSE_STATUS_CODE: 200, RESPONSE_ERROR_MESSAGE: '', RESPONSE_DATA: speaker.decode('utf-8')}

    # it means that the speaker has been taken so return an emptry string
    return {RESPONSE_STATUS_CODE: 200, RESPONSE_ERROR_MESSAGE: 'Speaker already had been taken or no speaker flagged this listener', RESPONSE_DATA: ''}


def get_int_from_redis_hashtable(redis_client, key: str):
    return int(redis_client.get(key).decode('utf-8'))
    

class RedisQueue(object):
    """Simple Queue with Redis Backend"""
    def __init__(self, name, namespace='queue', **redis_kwargs):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.__db= redis.StrictRedis(**redis_kwargs)
        self.key = '%s:%s' %(namespace, name)

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def put(self, item):
        """Put item into the queue."""
        self.__db.rpush(self.key, item)

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue. 

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)

        if item:
            item = item[1]
        return item
        
    def get_redis_client(self):
        return self.__db

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)

    