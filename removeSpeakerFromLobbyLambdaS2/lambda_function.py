import common.functions as f
import common.settings as s
import redis

def lambda_handler(event, context):

    resp = f.create_response_if_no_user(event)
    if resp:
        return resp

    has_listener = f.check_event_for_item(event, s.LISTENER_QUERY_STRING)
    if not has_listener:
        return f.create_response(400, 'The url has no listener', '')

    redis_client = redis.StrictRedis(**s.REDIS_SETUP)
    speaker = event[s.USER_QUERY_STRING].encode()
    listener = event[s.LISTENER_QUERY_STRING]

    lobby_key = f'{s.REDIS_LOBBY_NAMESPACE}:{listener}'

    print("Lobby members before: ", redis_client.smembers(lobby_key))
    is_deleted = redis_client.srem(lobby_key, speaker)
    print("Lobby members after: ", redis_client.smembers(lobby_key))
    return f.create_response(200, '', '', delete_info_message=f'Speaker had been deleted: {is_deleted}')
