import redis 

REDIS_SETUP = {
'host' : 'lowkeyapp-redis-001.hznbrp.0001.euc1.cache.amazonaws.com',
'port': 6379,
'db': 0,
}

def handler(event, context):
    rq = RedisQueue('queue', **REDIS_SETUP)
    
    listener = rq.get(timeout=1)
    if listener is None:
        return "There are no listeners"
    else:
        listener = listener.decode('utf-8')
        rq.get_redis_client().set(listener, event['user'])

    return listener




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

    