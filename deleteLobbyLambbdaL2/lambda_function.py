import common.functions as f
import common.settings as s
import redis


def lambda_handler(event, context):

    resp = f.create_response_if_no_user(event)
    if resp:
        return resp

    redis_client = redis.StrictRedis(**s.REDIS_SETUP)
    listener = event[s.USER_QUERY_STRING]

    lobby_key = f'{s.REDIS_LOBBY_NAMESPACE}:{listener}'
    is_deleted = redis_client.delete(lobby_key)
    return f.create_response(200, '', f'The lobby is deleted: {is_deleted}')