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
    
    lobby_number_of_speakers = redis_client.scard(lobby_key) - 1 
    # '-1' because the set has one extra item that it's added at the beggining 
    # by the listener
    if lobby_number_of_speakers == -1: 
        # This is the case that the lobby is not created yet. 
        # So there is a problem, hence we throw a 400.
        return f.create_response(400, error_message='There is no lobby to check', data='')
        
    print(f'Lobby key: {lobby_key}')
    print(f'Lobby number of speakers: {lobby_number_of_speakers}')
    print(f'Lobby members: {redis_client.smembers(lobby_key)}')
    
    if lobby_number_of_speakers < s.REDIS_MAX_LOBBY_NUMBER:
        return f.create_response(200, 
        f'The lobby needs {s.REDIS_MAX_LOBBY_NUMBER - lobby_number_of_speakers} speakers until it\'s full', '')
    elif lobby_number_of_speakers == s.REDIS_MAX_LOBBY_NUMBER:

        # when speakers_ready == s.REDIS_MAX_LOBBY_NUMBER then the listener will get the lobby and delete it 
        lobby_ready_for_listener_key = f'{lobby_key}:{s.REDIS_LOBBY_READY_FOR_LISTENER_NAMESPACE}'
        speakers_ready = redis_client.get(lobby_ready_for_listener_key)
        if speakers_ready is None or f.get_int_from_bytes(speakers_ready) < s.REDIS_MAX_LOBBY_NUMBER:
            
            if speakers_ready is None:
                speakers_ready = 0
                
            return f.create_response(200, 
            f'Wait for all the speakers to grab the lobby: {s.REDIS_MAX_LOBBY_NUMBER - speakers_ready} more', 
                '')

        # now delete the flag
        redis_client.delete(lobby_ready_for_listener_key)

        # decode the speakers
        speakers = [] 
        for speaker in redis_client.smembers(lobby_key):
            speaker = speaker.decode('utf-8')
            if not speaker.__eq__(s.REDIS_LOBBY_CREATE_FLAG):
                speakers.append(speaker)
        
        # delete the lobby so others speakers won't access it anymore
        redis_client.delete(lobby_key) 
        
        return f.create_response(200, 
        '', {s.RESPONSE_FROM_1_SPEAKERS: speakers, s.RESPONSE_FROM_1_LISTENER: listener})
    else:
        return f.create_response(500, 
        f'There are {lobby_number_of_speakers - s.REDIS_MAX_LOBBY_NUMBER} more members in the lobby', '') 
