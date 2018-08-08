from common.RedisQueue import RedisQueue
import common.functions as f
import common.settings as s

def lambda_handler(event, context):

    resp = f.create_response_if_no_user(event)
    if resp:
        return resp

    speaker = event[s.USER_QUERY_STRING]
    rq = RedisQueue(namespace=s.REDIS_QUEUE_NAMESPACE, **s.REDIS_SETUP)
    print("Queue size: ", rq.qsize())

    listener = rq.get(timeout=1)
    print("Listener: ", listener)
    if listener is None:
        return f.create_response(200, 'There are no listeners in the queue', '')
    
    while not f.check_if_listener_has_lobby(rq, listener):
        listener = rq.get(timeout=1)
        print("Listener: ", listener)
        if listener is None:
            return f.create_response(200, 'There are no listeners in the queue', '')
    
    listener = listener.decode('utf-8')
    lobby_key = f'{s.REDIS_LOBBY_NAMESPACE}:{listener}'
    
    if rq.db.scard(lobby_key) - 1 < s.REDIS_MAX_LOBBY_NUMBER:
        rq.db.sadd(lobby_key, speaker)
        print("Lobby key: ", lobby_key)
        print("Lobby members: ", rq.db.smembers(lobby_key))
        return f.create_response(200, '', listener)
    else:
        return f.create_response(200, f'Lobby for {lobby_key} already full', '')

