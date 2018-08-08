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
    speaker = event[s.USER_QUERY_STRING]
    listener = event[s.LISTENER_QUERY_STRING]

    lobby_key = f'{s.REDIS_LOBBY_NAMESPACE}:{listener}'
    
    lobby_number_of_speakers = redis_client.scard(lobby_key) - 1 
    # '-1' because the set has one extra item that it's added at the beggining 
    # by the listener
    if lobby_number_of_speakers == -1: 
        # if there is only the flag there is need for correction 
        # (the algortim works only with natural numbers)
        lobby_number_of_speakers = lobby_number_of_speakers + 1
        
    print("Lobby key: ", lobby_key)
    print("Lobby number of speaker: ", lobby_number_of_speakers)
    print("Lobby members: ", redis_client.smembers(lobby_key))
    
    if lobby_number_of_speakers < s.REDIS_MAX_LOBBY_NUMBER:
        return f.create_response(200, 
        f'The lobby needs {s.REDIS_MAX_LOBBY_NUMBER - lobby_number_of_speakers} speakers until it\'s full', '')
    elif lobby_number_of_speakers == s.REDIS_MAX_LOBBY_NUMBER:
        # decode the speakers
        speakers = [] 
        for speaker in redis_client.smembers(lobby_key):
            speaker = speaker.decode('utf-8')
            if not speaker.__eq__(s.REDIS_LOBBY_CREATE_FLAG):
                speakers.append(speaker)

        # when speakers_ready == s.REDIS_MAX_LOBBY_NUMBER then the listener will get the lobby and delete it 
        lobby_ready_for_listener_key = f'{lobby_key}:{s.REDIS_LOBBY_READY_FOR_LISTENER_NAMESPACE}'
        speakers_ready = redis_client.get(lobby_ready_for_listener_key)
        if speakers_ready:
            redis_client.set(lobby_ready_for_listener_key,
                redis_client.get(lobby_ready_for_listener_key) + 1)
        else:
            redis_client.set(lobby_ready_for_listener_key, 1)
        
        return f.create_response(200, 
        '', speakers)
    else:
        return f.create_response(500, 
        f'There are {lobby_number_of_speakers - s.REDIS_MAX_LOBBY_NUMBER} more members in the lobby', '') 

