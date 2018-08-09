import common.functions as f
from common.RedisQueue import RedisQueue
import common.settings as s
import redis

def lambda_handler(event, context):

    resp = f.create_response_if_no_user(event)
    if resp:
        return resp

    has_listener = f.check_event_for_item(event, s.LISTENER_QUERY_STRING)
    if not has_listener:
        return f.create_response(400, 'The url has no listener', '')

    rq = RedisQueue(namespace=s.REDIS_QUEUE_NAMESPACE, **s.REDIS_SETUP)
    speaker = event[s.USER_QUERY_STRING].encode()
    listener = event[s.LISTENER_QUERY_STRING]

    lobby_key = f'{s.REDIS_LOBBY_NAMESPACE}:{listener}'
    
    print("Lobby members before: ", redis_client.smembers(lobby_key))

    is_deleted = rq.db.srem(lobby_key, speaker)
    # If the speaker was deleted from the lobby the listener has to be put
    # back in the queue to be grabbed by another speaker. 
    if is_deleted:
        rq.put(listener)

    print("Lobby members after: ", redis_client.smembers(lobby_key))
    return f.create_response(200, '', '', delete_info_message=f'Speaker had been deleted: {is_deleted}')
