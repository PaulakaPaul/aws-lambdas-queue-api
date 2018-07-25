import redis 
import os

'''SPEAKER SCRIPT that has the find listener, logic'''

REDIS_SETUP = {
'host' : os.environ['REDIS_ENDPOINT'],
'port': os.environ['REDIS_PORT'],
'db': 0,
}

REDIS_QUEUE_NAMESPACE = 'queue'
REDIS_QUEUE_NAME = 'matching'

USER_QUERY_STRING = os.environ['USER_QUERY_STRING']

RESPONSE_STATUS_CODE = 'status_code'
RESPONSE_ERROR_MESSAGE = 'error_message'
RESPONSE_DATA = 'data'

def handler(event, context):
    rq = RedisQueue(name=REDIS_QUEUE_NAME, namespace=REDIS_QUEUE_NAMESPACE, **REDIS_SETUP)
    
     # if there is no user in the url query string stop the logic 
    if USER_QUERY_STRING not in event:
        return {RESPONSE_STATUS_CODE: 400, RESPONSE_ERROR_MESSAGE: 'No user provided in the query string', RESPONSE_DATA: ''}

    # get the user from the url query string
    speaker = event[USER_QUERY_STRING] 

    # try to get the listener from the queue
    listener = rq.get(timeout=1)

    if listener is None:
        # if there is no listener return empty string
        return {RESPONSE_STATUS_CODE: 200, RESPONSE_ERROR_MESSAGE: 'No listener cached', RESPONSE_DATA: ''}
    else:
        listener = listener.decode('utf-8')

        # create the hashtable namespaced key
        listener_key = f'{REDIS_QUEUE_NAMESPACE}:{listener}'

        # add flag to announce the listener that the matching had been done and also pass him the speaker        
        rq.get_redis_client().set(listener_key, speaker)

    return {RESPONSE_STATUS_CODE: 200, RESPONSE_ERROR_MESSAGE: '', RESPONSE_DATA: listener}




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

    