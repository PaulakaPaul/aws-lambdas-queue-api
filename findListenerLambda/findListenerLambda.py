import redis 
import os

'''SPEAKER SCRIPT that has the find listener, logic'''

REDIS_SETUP = {
'host' : os.environ['REDIS_ENDPOINT'],
'port': os.environ['REDIS_PORT'],
'db': 0,
}

REDIS_SPEAKER_FLAG_NAMESPACE = 'speaker'
REDIS_TIMEOUT_FLAG_NAMESPACE = 'timeout'
REDIS_QUEUE_NAME = 'matching'

USER_QUERY_STRING = os.environ['USER_QUERY_STRING']

RESPONSE_STATUS_CODE = "status_code"
RESPONSE_ERROR_MESSAGE = "error_message"
RESPONSE_DATA = "data"

def handler(event, context):
    rq = RedisQueue(name=REDIS_QUEUE_NAME, **REDIS_SETUP)
    
     # if there is no user in the url query string stop the logic 
    if USER_QUERY_STRING not in event or event[USER_QUERY_STRING].__eq__(""):
        return {RESPONSE_STATUS_CODE: 400, RESPONSE_ERROR_MESSAGE: "No user provided in the query string", RESPONSE_DATA: ""}

    # get the user from the url query string
    speaker = event[USER_QUERY_STRING] 

    # try to get the listener from the queue and check if it has been timed out
    listener_not_valid = True
    while listener_not_valid:
        listener = rq.get(timeout=1)
        print(listener)
        

        if listener is None:
            # if there is no listener return empty string
            return {RESPONSE_STATUS_CODE: 200, RESPONSE_ERROR_MESSAGE: "No listener cached", RESPONSE_DATA: ""}

        listener = listener.decode('utf-8')
        listener_timeout_key = f'{REDIS_TIMEOUT_FLAG_NAMESPACE}:{listener}'
        
        if rq.get_redis_client().get(listener_timeout_key) is not None and get_int_from_redis_hashtable(rq.get_redis_client(), listener_timeout_key) > 0:
            # decrement so we know that we took one timed out listener from the queue and loop again
            rq.get_redis_client().set(listener_timeout_key, 
                get_int_from_redis_hashtable(rq.get_redis_client(), listener_timeout_key) -1)
        else:
            # if the cached flag is 0 or None continue 
            listener_not_valid = False
   
    # create the hashtable namespaced key
    listener_key = f'{REDIS_SPEAKER_FLAG_NAMESPACE}:{listener}'
    print(listener_key)

    # add flag to announce the listener that the matching had been done and also pass him the speaker        
    rq.get_redis_client().set(listener_key, speaker)

    return {RESPONSE_STATUS_CODE: 200, RESPONSE_ERROR_MESSAGE: "", RESPONSE_DATA: listener}


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

    