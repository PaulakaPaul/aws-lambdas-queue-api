from common.RedisQueue import RedisQueue
import common.functions as f
import common.settings as s

def lambda_handler(event, context):

    resp = f.create_response_if_no_user(event)
    if resp:
        return resp

    listener = event[s.USER_QUERY_STRING]
    rq = RedisQueue(namespace=s.REDIS_QUEUE_NAMESPACE, **s.REDIS_SETUP)
    
    if f.check_if_listener_has_lobby(rq, listener.encode()):
        return f.create_response(500, f'Listener already added. Has a lobby and in queue.', '')
    
    # add the listener in the queue for every speaker in the lobby
    for _ in range(s.REDIS_MAX_LOBBY_NUMBER):
        rq.put(listener)
    
    lobby_key = f'{s.REDIS_LOBBY_NAMESPACE}:{listener}'
    rq.db.sadd(lobby_key, s.REDIS_LOBBY_CREATE_FLAG)
    print(f'Lobby key: {lobby_key}')
    
    return f.create_response(200, '', f'Listener added to {rq.key} and to {lobby_key}')


    
