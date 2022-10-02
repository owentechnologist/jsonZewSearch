## You will need an instance of Redis running Search and JSON modules to use this example.
#### You can sign up for a free cloud instance here: https://redis.com/try-free/
### This code demonstrates
1) adding JSON objects to RedisJson
2) creating a RediSearch Index using JSON
3) creating a Search Alias - because it allows for some additional decoupling between client and index details
4) executing a search query and examining the returned results
### To run the program execute the following replacing host and port values with your own:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000"
```


