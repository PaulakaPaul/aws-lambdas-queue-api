from common.RedisQueue import RedisQueue
import common.functions as f
import common.settings as s

def lambda_handler(event, context):

    resp = f.create_response_if_no_user(event)
    if resp:
        return resp

    speaker = event[s.USER_QUERY_STRING]
    rq = RedisQueue(namespace=s.REDIS_QUEUE_NAMESPACE, **s.REDIS_SETUP)

    listener = rq.get(timeout=0)
    if listener is None:
        return f.create_response(200, 'There are no listeners in the queue', '')

    while not check_if_listener_has_lobby(rq, listener):
        listener = rq.get(timeout=0)
        if listener is None:
            return f.create_response(200, 'There are no listeners in the queue', '')

    lobby_key = f'{s.REDIS_QUEUE_NAMESPACE}{listener}'
    rq.db.sadd(lobby_key, speaker)
    return f.create_response(200, '', 'Speaker added to lobby')



def check_if_listener_has_lobby(rq: RedisQueue, listener: str) -> bool:
    lobby_key = f'{s.REDIS_QUEUE_NAMESPACE}{listener}'
    return bool(rq.db.sismember(lobby_key, s.REDIS_LOBBY_CREATE_FLAG))