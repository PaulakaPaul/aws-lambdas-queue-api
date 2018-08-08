import common.functions as f
import common.settings as s
import redis


def lambda_handler(event, context):

    resp = f.create_response_if_no_user(event)
    if resp:
        return resp

    redis_client = redis.StrictRedis(**s.REDIS_SETUP)
    listener = event[s.USER_QUERY_STRING]

    # delete lobby
    lobby_key = f'{s.REDIS_LOBBY_NAMESPACE}:{listener}'
    is_deleted = redis_client.delete(lobby_key)

    # delete the flag
    lobby_ready_for_listener_key = f'{lobby_key}:{s.REDIS_LOBBY_READY_FOR_LISTENER_NAMESPACE}'
    flag_deleted = redis_client.delete(lobby_ready_for_listener_key)
    
    return f.create_response(200, '', '', delete_info_message=f'The lobby is deleted: {is_deleted}. The flag is deleted: {flag_deleted}')