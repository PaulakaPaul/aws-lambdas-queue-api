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
        # if there is only the flag there is need for correction 
        # (the algortim works only with natural numbers)
        lobby_number_of_speakers = lobby_number_of_speakers + 1
        
    print(lobby_key)
    print(lobby_number_of_speakers)
    print(redis_client.smembers(lobby_key))
    
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
        
        # delete the lobby so others speakers won't access it anymore
        redis_client.delete(lobby_key) 
        
        return f.create_response(200, 
        '', speakers)
    else:
        return f.create_response(500, 
        f'There are {lobby_number_of_speakers - s.REDIS_MAX_LOBBY_NUMBER} more members in the lobby', '') 

