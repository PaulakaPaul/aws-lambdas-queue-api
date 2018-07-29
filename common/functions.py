import common.settings as s
from common.RedisQueue import RedisQueue

def create_response(status_code:int, error_message='', data='') -> dict:
    return {'statusCode': status_code, 
            'errorMessage': error_message,
            'data': data}

def check_event_for_item(event, item) -> bool:
    if item not in event:
        return False
    
    if event[item].__eq__(''):
        return False

    return True

def check_event_for_user(event) -> bool:
    return check_event_for_item(event, s.USER_QUERY_STRING)

def create_response_if_no_user(event) -> dict:
    if not check_event_for_user(event):
        return create_response(400, 'The url contains no user', '')

    return None

def check_if_listener_has_lobby(rq: RedisQueue, listener: bytes) -> bool:
    listener = listener.decode('utf-8')
    lobby_key = f'{s.REDIS_LOBBY_NAMESPACE}:{listener}'
    return bool(rq.db.sismember(lobby_key, s.REDIS_LOBBY_CREATE_FLAG))

def get_int_from_bytes(b: bytes) -> int:
    return int(b.decode('utf-8'))