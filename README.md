## You will need an instance of Redis running Search and JSON modules to use this example.
#### You can sign up for a free cloud instance here: https://redis.com/try-free/
### This code demonstrates
1) adding JSON objects to RedisJson
2) creating a RediSearch Index using JSON
3) creating a Search Alias - because it allows for some additional decoupling between client and index details
4) executing a search query and examining the returned results
5) Allowing the user to test the auto-complete feature by typing characters and getting suggestions for event species, locations, and types

### To run the program execute the following replacing host and port values with your own:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000"
```

### To run the program loading a larger quantity of JSON activity Objects use the --quantity arg like this:
* If you do not specify any quantity, the default of 2 objects will be written

```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 200000"
```

### <em>Note that default limit on # of results is 3 results - to modify this, use --limitsize like this:</em>
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 200000 --limitsize 20"
```
### To add a test of auto-complete suggestions you can also use this flag and determine how many times to prompt the user for input:
* --autocomplete 3
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 200000 --limitsize 20 --autocomplete 2"
```
## Setting quantity to 0 will prevent the deletion and recreation of the index - and results will be consistent
* To run queries against an existing dataset (loaded by a previous run) use --quantity 0 
* Setting quantity to  0 will prevent the deletion and recreation of the index - and results will be consistent
```
--quantity 0
```
like this:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 0 --limitsize 20 --autocomplete 2"
```
## User and Password can also be provided as args:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 0 --limitsize 2 --user default --password secretpassword12"
```

### You can also specify an indexsleeptime value measured in milliseconds (this allows time for a newly created index to index pre-loaded documents from a different run)
* You can adjust this value to determine and match how long it takes to index whatever old docs exist in redis (new docs written after the index is created are indexed in realtime)
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --user default --password secretpassword12 --quantity 2 --limitsize 2 --indexsleeptime 30000"
```

The JSON Objects represent fake Zewtopia Zoo events and look like this:
``` 
{
	"times": [{
		"military": "0800",
		"civilian": "8 AM"
	}, {
		"military": "1500",
		"civilian": "3 PM"
	}, {
		"military": "2200",
		"civilian": "10 PM"
	}],
	"responsible-parties": {
		"number_of_contacts": 2,
		"hosts": [{
			"phone": "715-876-5522",
			"name": "Duncan Mills",
			"email": "dmilla@zew.org"
		}, {
			"phone": "815-336-5598",
			"name": "Xiria Andrus",
			"email": "xiriaa@zew.org"
		}]
	},
	"cost": 0,
	"name": "Gorilla Feeding",
	"days": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
	"location": "Gorilla House South"
}
```

