# aws-lambdas
### * que 
### * messaging 

#### !!!!!!!!! findListenerLambda and findSpeakerLambda depricated

# If you want to upload a lambda to aws you have to:
```
    - pip install redis -t 'path/to/lambda/file'
    - copy the ./common folder to 'path/to/lambda/file'
    - zip the folder content 
```

# API (referenced to the base url that it's stored on aws API Gateway on the Redis project)
```
    * queue/listener -> DELETE, POST, GET
    * queue/speaker -> DELETE, POST, GET
```
