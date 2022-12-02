package com.redislabs.sa.ot.jzs;
import com.github.javafaker.Faker;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.*;
import redis.clients.jedis.providers.PooledConnectionProvider;
import redis.clients.jedis.search.*;
import redis.clients.jedis.search.aggr.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;

/**
 * You will need an instance of Redis running Search and JSON modules to use this example.
 * You can sign up for an eternally free cloud instance here: https://redis.com/try-free/
 * This code demonstrates
 * 1) adding JSON objects to RedisJson
 * 2) creating a RediSearch Index using JSON
 * 3) creating a Search Alias - because it allows for some additional decoupling between client and index details
 * 4) executing a search query and examining the returned results
 * To run the program execute the following replacing host and port values with your own:
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000"
 * to run the program loading a larger quantity of JSON activity Objects use the --quantity arg like this:
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 200000"
 * Note that default limit on # of results is 3 results - to modify this use --limitsize like this:
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 200000 --limitsize 20"
 * To add a test of auto-complete suggestions you can also use this flag and determine how many times to prompt the user for input:
 * --autocomplete 3
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 200000 --limitsize 20 --autocomplete 2"
 * To run queries against an existing dataset (loaded by a previous run) use --quantity 0 (or do not specify any quantity and the default of 2 objects will be written)
 * Setting quantity to  0 will prevent the deletion and recreation of the index - and results will be consistent
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 0 --limitsize 20 --autocomplete 2"
 * User and Password can also be provided as args:
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --quantity 0 --limitsize 2 --user default --password secretpassword12"
 * You can also specify an indexsleeptime value measured in milliseconds (this allows time for a newly created index to index pre-loaded documents from a different run)
 * You can adjust this value to determine and match how long it takes to index whatever old docs exist in redis (new docs written after the index is created are indexed in realtime)
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --user default --password secretpassword12 --quantity 2 --limitsize 2 --indexsleeptime 30000"
 * If you want to try out JSON multi-value search https://github.com/RediSearch/RediSearch/releases/tag/v2.6.1 and have search V2.6.1 and JSON 2.4.0 or better installed you can specify --multivalue true
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12000 --user default --password secretpassword12 --quantity 2 --multivalue true --limitsize 2 --indexsleeptime 30000"
 */
public class Main {

    private static final String INDEX_1_NAME = "idx_zew_events";
    private static final String INDEX_ALIAS_NAME = "idxa_zew_events";
    private static final String PREFIX_FOR_SEARCH = "zew:activities:";
    private static final String SUGGESTION_KEY = "zew:suggest";
    private static int howManyResultsToShow = 3;
    private static int autocompleteTries = 0;
    private static int quantity = 0;
    private static boolean multiValueSearch = false;
    private static int dialectVersion = 2;//Dialect 3 is needed for complete multivalue results
    private static ConnectionHelper connectionHelper = null;

    public static void main(String[] args) {
        String host = "192.168.1.20";
        int port = 12000;
        String username = "default";
        String password = "";
        int indexSleepTime = 0;
        boolean isOnlyTwo = true;  // by default write 2 JSON objects so there is something to query against
        if (args.length > 0) {
            ArrayList<String> argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--host")) {
                int hostIndex = argList.indexOf("--host");
                host = argList.get(hostIndex + 1);
            }
            if (argList.contains("--port")) {
                int portIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(portIndex + 1));
            }
            if (argList.contains("--indexsleeptime")) {
                int indexsleeptimeIndex = argList.indexOf("--indexsleeptime");
                indexSleepTime = Integer.parseInt(argList.get(indexsleeptimeIndex + 1));
            }
            if (argList.contains("--quantity")) {
                isOnlyTwo = false; // the user is specifying what number of JSON objects to write
                int quantityIndex = argList.indexOf("--quantity");
                quantity = Integer.parseInt(argList.get(quantityIndex + 1));
            }
            if (argList.contains("--limitsize")) {
                int limitIndex = argList.indexOf("--limitsize");
                howManyResultsToShow = Integer.parseInt(argList.get(limitIndex + 1));
            }
            if (argList.contains("--autocomplete")) {
                int autocompleteIndex = argList.indexOf("--autocomplete");
                autocompleteTries = Integer.parseInt(argList.get(autocompleteIndex + 1));
            }
            if (argList.contains("--user")) {
                int userNameIndex = argList.indexOf("--user");
                username = argList.get(userNameIndex + 1);
            }
            if (argList.contains("--password")) {
                int passwordIndex = argList.indexOf("--password");
                password = argList.get(passwordIndex + 1);
            }
            if (argList.contains("--multivalue")) {
                int multiValueIndex = argList.indexOf("--multivalue");
                multiValueSearch = Boolean.parseBoolean(argList.get(multiValueIndex + 1));
                if (multiValueSearch) {
                    dialectVersion = 3;
                }
            }
        }
        HostAndPort hnp = new HostAndPort(host, port);
        System.out.println("Connecting to " + hnp.toString());
        URI uri = null;
        try {
            if (!("".equalsIgnoreCase(password))) {
                uri = new URI("redis://" + username + ":" + password + "@" + hnp.getHost() + ":" + hnp.getPort());
            } else {
                uri = new URI("redis://" + hnp.getHost() + ":" + hnp.getPort());
            }
        } catch (URISyntaxException use) {
            use.printStackTrace();
            System.exit(1);
        }
        connectionHelper = new ConnectionHelper(uri);
        //Make sure index and alias are in place before we start writing data or querying:
        // dropping and recreating the index can result in partial matches on existing data
        try {
            if (quantity > 0 || isOnlyTwo) {
                dropIndex();
                addIndex();
                System.out.println("Sleeping for " + indexSleepTime + " milliseconds to give the newly created index time to catch up with pre-loaded documents");
                Thread.sleep(indexSleepTime); // give the index some time to catch up with any pre-existing data
            }
        } catch (Throwable t) {
            System.out.println("" + t.getMessage());
            try {
                JedisPooled jedis = connectionHelper.getPooledJedis();
                System.out.println("There are " + jedis.dbSize() + " keys in the redis database");
            } catch (Throwable t2) {
                System.out.println("" + t2.getMessage());
            }
        }
        System.out.println("LOADING JSON DATA...");
        loadData(isOnlyTwo, quantity);
        testJedisConnection();
        System.out.println("\n\nTESTING SEARCH QUERY ...");
        testJSONSearchQuery();
        if (autocompleteTries > 0) {
            prepareAutoComplete();
            System.out.println("\nTesting auto-complete ...[try the letter h or l]");
            testAutoComplete(autocompleteTries);
        }
    }

    private static void testJedisConnection() {
        JedisPooled jedis = connectionHelper.getPooledJedis();
        System.out.println("Testing connection by executing 'DBSIZE' response is: " + jedis.dbSize());
        System.out.println("Testing index state by executing 'FT.INFO' " + INDEX_1_NAME + " response is: " + jedis.ftInfo(INDEX_1_NAME));
    }

    private static void testAutoComplete(int howManyTimes) {
        JedisPooled jedis = connectionHelper.getPooledJedis();
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        for (int x = 0; x < howManyTimes; x++) {
            System.out.println("Please type part of an animal Species name and hit enter to receive auto-complete suggestions.");
            String input = null;
            try{
                input = reader.readLine();
            }catch(Throwable t){System.out.println("\nNOW ITS ALL MESSED UP! "+t.getMessage());}
            List<String> stringList = jedis.ftSugGet(SUGGESTION_KEY, input);
            System.out.println("Did you mean one of these:");
            for (String suggestion : stringList) {
                System.out.print("[ " + suggestion + " ],");
            }
            System.out.println("\n*****  End of Suggestions *****");
        }
    }
    /*
    This example provides a single auto-complete key for: species of animal, the activities, and the locations available
    Each category could live in its own auto-complete suggestion key which would allow for more discreet treatment of the suggestions
    That would, however, require multiple calls to fetch the results as each key would need to be checked separately
     */
    private static void prepareAutoComplete(){
        JedisPooled jedis = connectionHelper.getPooledJedis();
            for(String animal:JsonZewActivityBuilder.animalSpecies) {
                jedis.ftSugAdd(SUGGESTION_KEY,animal,1.0 );
            }
            for(String activity : JsonZewActivityBuilder.activityTypes){
                jedis.ftSugAdd(SUGGESTION_KEY,activity,1.0 );
            }
            for(String location : JsonZewActivityBuilder.locationTypes){
                jedis.ftSugAdd(SUGGESTION_KEY,location,.75 );
            }
            for(String direction :JsonZewActivityBuilder.locationDirections){
                jedis.ftSugAdd(SUGGESTION_KEY,direction,.5 );
            }
    }

    private static void dropIndex() {
        JedisPooled jedis = connectionHelper.getPooledJedis();
        try{
            jedis.ftDropIndex(INDEX_1_NAME);
        }catch(Throwable t){System.out.println("While attempting to drop index "+INDEX_1_NAME+"    >>> "+t.getMessage());}
    }

    /*
     * sample JSON that is being queried: (note that only the specified return fields are returned from a query)
     {"times":[{"military":"0800","civilian":"8 AM"},{"military":"1500","civilian":"3 PM"},{"military":"2200","civilian":"10 PM"}]
     * ,"responsible-parties":{"number_of_contacts":2,
     * "hosts":[{"phone":"715-876-5522","name":"Duncan Mills","email":"dmilla@zew.org"},{"phone":"815-336-5598",
     * "name":"Xiria Andrus","email":"xiriaa@zew.org"}]},
     * "cost":0,"name":"Gorilla Feeding",
     * "days":["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],
     * "location":"Gorilla House South"}
     *
     */
    private static void testJSONSearchQuery() {
        ArrayList<String> perfTestResults = new ArrayList<>();
        JedisPooled jedis = connectionHelper.getPooledJedis();
            long startTime = System.currentTimeMillis();
            // Query that only works with Dialect 3: (won't fire if DIALECT VERSION is lower than 3)
        if(dialectVersion==3){
            // This query does not return the whole document but will return
            // a specified subsection of the JSON document as well as individual indexed attributes:
            /*
            With our JSON document looking like this:
            *************************************
            JSON.RESP zew:activities:58506 $
            1) 1) "{"
               2) "times"
               3) 1) "["
                  2) 1) "{"
                     2) "military"
                     3) "1700"
                     4) "civilian"
                     5) "5:00 PM"
                  3) 1) "{"
                     2) "military"
                     3) "2000"
                     4) "civilian"
                     5) "8:00 PM"
                  4) 1) "{"
               4) "responsible-parties"
               5) 1) "{"
                  2) "number_of_contacts"
                  3) "3"
                  4) "hosts"
                  5) 1) "["
                     2) 1) "{"
                        2) "phone"
                        3) "281-729-4206"
                        4) "name"
                        5) "Chadwick Johnston"
                        6) "email"
                        7) "Chadwick@zew.org"
                     3) 1) "{"
                        2) "phone"
                        3) "264.618.7058"
                        4) "name"
                        5) "Benedict Mante"
                        6) "email"
                        7) "Benedict@zew.org"
                     4) 1) "{"
                        2) "phone"
                        3) "510-347-9788"
                        4) "name"
                        5) "Chadwick Kautzer DDS"
                        6) "email"
                        7) "Chadwick@zew.org"
               6) "cost"
               7) "10"
               8) "name"
               9) "Zebra Live Show"
               10) "days"
               11) 1) "["
                  2) "Mon"
                  3) "Wed"
                  4) "Thu"
                  5) "Sat"
                  6) "Sun"
               12) "location"
               13) "Zebra Habitat North"
            **********************
            And our working Index being this:
            FT.CREATE idx_json1 ON JSON PREFIX 1 zew:activities: SCHEMA
            $.name AS event_name TEXT PHONETIC dm:en $.cost AS cost NUMERIC $.days.* AS days TAG
            $.location AS location TEXT PHONETIC dm:en
            $.responsible-parties.*.name AS contact_name TEXT $.times.*.military as times TAG

            If we only want to retrieve the responsible-parties subsection of the document that matches
            the contact_name in our query we need to specify the filter in both the query and in the returned field
            NB: this will not work with Phonetic matching nor synonyms
             */
            String query = "@contact_name:(Chadw*)";
            SearchResult result = jedis.ftSearch(INDEX_ALIAS_NAME, new Query(query)
                    .returnFields(
                            FieldName.of("event_name"),// This is a simple field from the root of the JSON doc (it is aliased in the index)
                            FieldName.of("$.location").as("EVENT_LOCATION"),// This is a simple field from the root of the JSON doc
                            FieldName.of("$.responsible-parties.hosts[?(@.name =~ \"(?i)^Chadw\")]") // note the ability to partially match with regex
                                    .as("matched_party_by_name") // this demonstrates the discreet and aligned response capability
                    ).limit(0,howManyResultsToShow).dialect(dialectVersion)
            );
            perfTestResults.add("Dialect3 (with "+result.getTotalResults()+" results and limit size of "+howManyResultsToShow+") Execution took: "+(System.currentTimeMillis()-startTime)+" milliseconds");
            printResultsToScreen(query, result);
            perfTestResults.add("Dialect3 (with "+result.getTotalResults()+" results and limit size of "+howManyResultsToShow+")) Execution plus printing results to screen took: "+(System.currentTimeMillis()-startTime)+" milliseconds");
        }
        // first Query:
        String query = "@days:{Sat,Sun} @times:{1400,2000} -@location:(House)";
        SearchResult result = jedis.ftSearch(INDEX_ALIAS_NAME, new Query(query)
                .returnFields(
                        FieldName.of("location"), // only a single value exists in a document
                        FieldName.of("$.times[?(@.military==\"1400\" || @.military==\"2000\")]")
                                .as("matched_times"),
                        FieldName.of("$.times[*].civilian").as("civilian"), //  dialect determines multiple or single results
                        FieldName.of("$.days").as("days"), // multiple days may be returned
                        FieldName.of("$.responsible-parties.hosts.[*].email").as("contact_email"), //  dialect determines multiple or single results
                        FieldName.of("$.responsible-parties.hosts.[*].phone").as("contact_phone"), //  dialect determines multiple or single results
                        FieldName.of("event_name"), // only a single value exists in a document
                        FieldName.of("$.times[*].military").as("military"), //  dialect determines multiple or single results
                        FieldName.of("$.description")
                ).limit(0,howManyResultsToShow).dialect(dialectVersion)
        );
        perfTestResults.add("Query1 (with "+result.getTotalResults()+" results and limit size of "+howManyResultsToShow+") Execution took: "+(System.currentTimeMillis()-startTime)+" milliseconds");
        printResultsToScreen(query, result);
        perfTestResults.add("Query1 (with "+result.getTotalResults()+" results and limit size of "+howManyResultsToShow+")) Execution plus printing results to screen took: "+(System.currentTimeMillis()-startTime)+" milliseconds");

        //Second query:
        startTime = System.currentTimeMillis();
        query = "@contact_name:(Jo* Hu*) @times:{2000}";
        result = jedis.ftSearch(INDEX_ALIAS_NAME, new Query(query)
                .returnFields(
                        FieldName.of("location"), // only a single value exists in a document
                        FieldName.of("$.times[*].civilian").as("first_event_time"), // Dialect determines if this is a single result
                        FieldName.of("$.times").as("all_times"), // multiple times may be returned when not filtered
                        FieldName.of("$.times[?(@.military==\"2000\")]").as("matched_times"),//Dialect determines if this is a single result
                        FieldName.of("$.days").as("days"), // multiple days may be returned
                        FieldName.of("$.responsible-parties[*].hosts").as("hosts"),//Dialect determines if this is a single result
                        FieldName.of("event_name"), // only a single value exists in a document
                        FieldName.of("$.responsible-parties.number_of_contacts").as("hosts_size")
                ).limit(0,howManyResultsToShow).dialect(dialectVersion)
        );
        perfTestResults.add("Query2 (with "+result.getTotalResults()+" results and limit size of "+howManyResultsToShow+") Execution took: "+(System.currentTimeMillis()-startTime)+" milliseconds");
        printResultsToScreen(query, result);
        perfTestResults.add("Query2 (with "+result.getTotalResults()+" results and limit size of "+howManyResultsToShow+")) Execution plus printing results to screen took: "+(System.currentTimeMillis()-startTime)+" milliseconds");

        //Third query:
        startTime = System.currentTimeMillis();
        query = "@cost:[-inf 5.00]";
        result = jedis.ftSearch(INDEX_ALIAS_NAME, new Query(query)
                .returnFields(
                        FieldName.of("location"), // only a single value exists in a document
                        FieldName.of("$.times.[*].civilian").as("all_times"), //  dialect determines multiple or single results
                        FieldName.of("$.days").as("days"), // multiple days may be returned
                        FieldName.of("event_name"), // only a single value exists in a document
                        FieldName.of("$.cost").as("cost_in_us_dollars")
                ).limit(0,howManyResultsToShow).dialect(dialectVersion)
        );
        perfTestResults.add("Query3 (with "+result.getTotalResults()+" results and limit size of "+howManyResultsToShow+") Execution took: "+(System.currentTimeMillis()-startTime)+" milliseconds");
        printResultsToScreen(query, result);
        perfTestResults.add("Query3 (with "+result.getTotalResults()+" results and limit size of "+howManyResultsToShow+")) Execution plus printing results to screen took: "+(System.currentTimeMillis()-startTime)+" milliseconds");

        //TEST Simple AGGREGATION...
        ArrayList<String> groupByFields = new ArrayList<>();
        groupByFields.add("@cost");
        groupByFields.add("@location");
        groupByFields.add("@event_name");
        ArrayList<Reducer> reducerCollection = new ArrayList<>();
        reducerCollection.add(Reducers.count().as("event_match_count"));
        AggregationBuilder builder = new AggregationBuilder("@event_name:Petting @cost:[1.00 +inf] " +
                "@location:Gorilla @location:East -@days:{Tue Wed Thu}")
                .groupBy(groupByFields,reducerCollection).filter("@cost <= 9").dialect(dialectVersion);
        AggregationResult aggregationResult = jedis.ftAggregate(INDEX_ALIAS_NAME,builder);
        String queryForDisplay = "FT.AGGREGATE idxa_zew_events \"@event_name:Petting @cost:[1.00 +inf] @location:Gorilla @location:East -@days:{Tue Wed Thu}\" GROUPBY 3 @cost @location @event_name REDUCE COUNT 0 AS event_match_count FILTER @cost <= 9";
        printAggregateResultsToScreen(queryForDisplay,aggregationResult);

        System.out.println("\n\tPerformance Results from this test run: \n");
        for(String perfResults : perfTestResults){
            System.out.println(perfResults);
        }
    }

    private static void printAggregateResultsToScreen(String query,AggregationResult result){
        System.out.println("\nFired Aggregation Query:\n"+query+"\n\t -  received "+result.getTotalResults()+" results:\n");
        List<Map<String, Object>> r = result.getResults();
        System.out.println("The number of rows returned is affected by any filters applied.  Returning this many: "+r.size());
        for(int row = 0;row < r.size();row++){
            Set<String> rr = r.get(row).keySet();
            Iterator<String> keySetIterator = rr.iterator();
            System.out.println("");
            while(keySetIterator.hasNext()) {
                String keyName = keySetIterator.next();
                System.out.print(keyName+":   "+result.getRow(row).getString(keyName)+"\t");
            }
        }
        System.out.println(""); // returning display cursor to start of page on next line
    }

    private static void printResultsToScreen(String query,SearchResult result){
        System.out.println("\n\tFired Query - \""+query+"\"\n Received a total of "+result.getTotalResults()+" results.\nDisplaying a maximum of "+howManyResultsToShow+" results:\n");

        List<Document> doclist = result.getDocuments();
        Iterator<Document> iterator = doclist.iterator();
        while (iterator.hasNext()) {
            Document d = iterator.next();
            Iterator<Map.Entry<String, Object>> propertiesIterator = d.getProperties().iterator();
            while (propertiesIterator.hasNext()) {
                Map.Entry<String, Object> pi = propertiesIterator.next();
                String propertyName = pi.getKey();
                Object propertyValue = pi.getValue();
                System.out.println(propertyName + " " + propertyValue);
            }
        }
    }

    //create Search Schema that will allow searches against the JSON objects
    //"FT.CREATE" "idx_zew_events" "ON" "JSON" "PREFIX" "1" "zew:activities:"
    // "SCHEMA" "$.name" "AS" "event_name" "TEXT" "$.cost" "AS" "cost" "NUMERIC"
    // "$.days.*" "AS" "days" "TAG" "$.times.*.military" "AS" "times" "TAG"
    // "$.location" "AS" "location" "TEXT"

    // This query returns the Gorilla feeding with all the times:
    // (note that to get multiple values from an Array the json path is needed)
    // FT.SEARCH idx_zew_events "@days:{Mon} @days:{Tue} @times:{08*}" return 3 event_name $.times location

    // This next query returns the bonobo lecture with only the first time from the array of times (there could be more):
    // FT.SEARCH idx_zew_events "@days:{Mon} -@location:('House')" return 3 event_name times location

    private static void addIndex(){
        /* Sample JSON object:
        [{"times":[{"military":"0800","civilian":"8 AM"},{"military":"1500","civilian":"3 PM"},{"military":"2200","civilian":"10 PM"}],
        "responsible-parties":[{"phone":"715-876-5522","name":"Duncan Mills","email":"dmilla@zew.org"}],
        "cost":"0.00","name":"Gorilla Feeding","days":["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],
        "location":"Gorilla House South"}]
        //working Search idx:
        FT.CREATE idx_json1 ON JSON PREFIX 1 zew:activities: SCHEMA
        $.name AS event_name TEXT PHONETIC dm:en $.cost AS cost NUMERIC $.days.* AS days TAG
        $.location AS location TEXT PHONETIC dm:en
        $.responsible-parties.*.phone AS phone TEXT $.times.*.military as times TAG
         */
        JedisPooled jedis = connectionHelper.getPooledJedis();
            Schema schema = new Schema().addField(new Schema.TextField(FieldName.of("$.name").as("event_name")))
                    .addSortableNumericField("$.cost").as("cost")
                    .addField(new Schema.Field(FieldName.of("$.days.*").as("days"), Schema.FieldType.TAG))
                    .addField(new Schema.Field(FieldName.of("$.times[*].military").as("times"), Schema.FieldType.TAG))
                    .addField(new Schema.Field(FieldName.of("$.location").as("location"), Schema.FieldType.TEXT))
                    .addTextField("$.responsible-parties.hosts[*].name", .75).as("contact_name"); //use with search 2.6.1 allows TEXT in multivalues
            IndexDefinition indexDefinition = new IndexDefinition(IndexDefinition.Type.JSON)
                    .setPrefixes(new String[]{PREFIX_FOR_SEARCH});

            jedis.ftCreate(INDEX_1_NAME, IndexOptions.defaultOptions().setDefinition(indexDefinition), schema);
            //AND THEN: add schema alias so we can toggle between indexes:
        /*
        Added use of search index Alias (this allows for possible
        re-assigning of the alias to an alternate index that perhaps targets a different underlying dataset
        - maybe including additional or entirely different prefixes
        This example doesn't demonstrate that reassignment of the alias - just its effective use as a layer of indirection.
         */
            jedis.ftAliasAdd(INDEX_ALIAS_NAME, INDEX_1_NAME);
            System.out.println("Successfully created search index and search index alias");
    }


    //load JSON Objects for testing
    private static void loadData(boolean onlyLoadTwoObjects,int howManyObjects){
        if(onlyLoadTwoObjects) {
            JedisPooled jedis = connectionHelper.getPooledJedis();
                jedis.del("zew:activities:gf");
                jedis.del("zew:activities:bl");

                JSONObject obj = new JSONObject();
                obj.put("name", "Gorilla Feeding");
                obj.put("cost", 0.00);
                obj.put("location", "Gorilla House South");

                JSONObject timeObj8am = new JSONObject();
                timeObj8am.put("military", "0800");
                timeObj8am.put("civilian", "8 AM");
                JSONObject timeObj3pm = new JSONObject();
                timeObj3pm.put("military", "1500");
                timeObj3pm.put("civilian", "3 PM");
                JSONObject timeObj10pm = new JSONObject();

                //timeObj10pm.put("military","2200");
                //included a null value to test the processing of null:
                timeObj10pm.put("military", (Object) null);
                timeObj10pm.put("civilian", "10 PM");

                ArrayList<JSONObject> timeObjects = new ArrayList<>();
                timeObjects.add(timeObj8am);
                timeObjects.add(timeObj3pm);
                timeObjects.add(timeObj10pm);

                JSONArray times = new JSONArray(timeObjects);

                obj.put("times", times);
                JSONArray days = new JSONArray(JsonZewActivityBuilder.DAYS_OF_WEEK);
                obj.put("days", days);

                JSONObject hostsHolder = new JSONObject();
                hostsHolder.put("number_of_contacts", 2);
                JSONArray hosts = new JSONArray();
                JSONObject contact = new JSONObject();
                contact.put("name", "Duncan Mills");
                contact.put("phone", "715-876-5522");
                contact.put("email", "dmilla@zew.org");
                hosts.put(contact);
                JSONObject contact2 = new JSONObject();
                contact2.put("name", "Xiria Andrus");
                contact2.put("phone", "815-336-5598");
                contact2.put("email", "xiriaa@zew.org");
                hosts.put(contact2);
                hostsHolder.put("hosts", hosts);
                obj.put("responsible-parties", hostsHolder);
                jedis.jsonSet("zew:activities:gf", obj);

                //build second zew event:
                obj = new JSONObject();
                obj.put("name", "Bonobo Lecture");
                obj.put("cost", 10.00);
                obj.put("location", "Mammalian Lecture Theater");
                times = new JSONArray();
                JSONObject timeObj11am = new JSONObject();
                timeObj11am.put("military", "1100");
                timeObj11am.put("civilian", "11 AM");
                times.put(timeObj11am);
                obj.put("times", times);
                days = new JSONArray();
                days.put(JsonZewActivityBuilder.DAYS_OF_WEEK[0]);
                days.put(JsonZewActivityBuilder.DAYS_OF_WEEK[3]);
                obj.put("days", days);
                hostsHolder = new JSONObject();
                hostsHolder.put("number_of_contacts", 1);
                hosts = new JSONArray();
                contact = new JSONObject();
                contact.put("name", "Dr. Clarissa Gumali");
                contact.put("phone", "715-322-5992");
                contact.put("email", "cgumali@zew.org");
                hosts.put(contact);
                hostsHolder.put("hosts", hosts);
                obj.put("responsible-parties", hostsHolder);
                jedis.jsonSet("zew:activities:bl", obj);
        }
        else{
            Pipeline pipeline = connectionHelper.getPipeline();
            int howManyBatches = (howManyObjects/200)>0?(howManyObjects/200)+1:1;//makes sure at least 1 batch gets fired
            int countDownOfObjects = howManyObjects;
            System.out.println("Writing "+howManyObjects+" objects to Redis in "+howManyBatches+" batches of 200 or less...");
            for (int x = 0; x < howManyBatches; x++) {
                int innerBatchQuantity = 200;
                if(countDownOfObjects<=200){
                    innerBatchQuantity=countDownOfObjects;
                }
                for(int innerX = 0; innerX < innerBatchQuantity; innerX++) {
                    //System.out.println("innerX == "+innerX);
                    pipeline.jsonSet(PREFIX_FOR_SEARCH + countDownOfObjects, JsonZewActivityBuilder.createFakeJsonZewActivityObject());
                    countDownOfObjects--;
                }
                pipeline.sync(); // execute batch of 200 JSON Set commands
                System.out.print("<"+countDownOfObjects+" JSON objects still to go> ");
            }
        }
    }
}

class JsonZewActivityBuilder{

    static final String[] DAYS_OF_WEEK = {"Mon","Tue","Wed","Thu","Fri","Sat","Sun"};
    static Faker faker = new Faker();
    static String[] activityTypes = new String[]{"Feeding","Training","Live Show","Lecture","Documentary","Petting","Ride"};
    static String[] locationTypes = new String[]{"House","Habitat","Theater","Lecture Hall","Area"};
    static String[] locationDirections = new String[]{"North","South","East","West"};
    static float[] costsOverZero = new float[]{2.00f,5.00f,10.00f,25.00f};
    static String[] animalSpecies = new String[]{"Lion","Tiger","Elephant","Giant Panda","Gorilla","Giraffe","Polar Bear","Hippo","Cheeta","Zebra","Meerkat","Penguin","Kangaroo","Flamingo","Koala","Chimpanzee","Llama","Green Anaconda","Hyena","Bonobo","Alligator","Orangutan"};
    static String[] militaryTimes = new String[]{"0800","0900","1000","1100","1130","1200","1230","1300","1330","1400","1430","1500","1600","1700","1800","1900","2000","2030","2100","2200"};
    static String[] civilianTimes = new String[]{"8 AM","9 AM","10 AM","11 AM","11:30 AM","12 Noon","12:30 PM","1:00 PM","1:30 PM","2:00 PM","2:30 PM","3:00 PM","4:00 PM","5:00 PM","6:00 PM","7:00 PM","8:00 PM","8:30 PM","9:00 PM","10:00 PM"};

    static JSONObject createFakeJsonZewActivityObject(){
        Random random = new Random();
        int randomValue = random.nextInt(111);
        JSONObject obj = new JSONObject();
        obj.put("name", animalSpecies[randomValue%animalSpecies.length]+" "+activityTypes[randomValue%activityTypes.length]);
        obj.put("cost", random.nextInt(3)>1?0.00f:costsOverZero[random.nextInt(111) % costsOverZero.length]);
        obj.put("location", animalSpecies[randomValue%animalSpecies.length]+" "+locationTypes[random.nextInt(111)%locationTypes.length]+" "+locationDirections[random.nextInt(111)%locationDirections.length]);

        JSONObject timeObj1 = new JSONObject();
        randomValue = randomValue+random.nextInt(111);
        timeObj1.put("military", militaryTimes[randomValue%militaryTimes.length]);
        timeObj1.put("civilian", civilianTimes[randomValue%civilianTimes.length]);
        JSONObject timeObj2 = new JSONObject();
        randomValue = randomValue+3;
        timeObj2.put("military", militaryTimes[randomValue%militaryTimes.length]);
        timeObj2.put("civilian", civilianTimes[randomValue%civilianTimes.length]);
        JSONObject timeObj3 = new JSONObject();
        //included a null value to test the processing of null:
        timeObj3.put("military", (Object) null);
        timeObj3.put("civilian", (Object) null);

        ArrayList<JSONObject> timeObjects = new ArrayList<>();
        timeObjects.add(timeObj1);
        timeObjects.add(timeObj2);
        timeObjects.add(timeObj3);

        JSONArray times = new JSONArray(timeObjects);
        obj.put("times", times);
        JSONArray days = new JSONArray(DAYS_OF_WEEK);//start with 7 days - then remove some random days:
        for(int dayVal=random.nextInt(111)%DAYS_OF_WEEK.length;dayVal < DAYS_OF_WEEK.length;dayVal=dayVal+2){
            days.remove(dayVal);
        }
        obj.put("days", days);

        JSONObject hostsHolder = new JSONObject();
        int numberOfContacts = (random.nextInt(111)%3)+1; //1-3 contacts for the event
        hostsHolder.put("number_of_contacts", numberOfContacts);
        JSONArray hosts = new JSONArray();
        JSONObject contact = new JSONObject();
        contact.put("name", faker.name().fullName());
        contact.put("phone", faker.phoneNumber().cellPhone());
        contact.put("email", ((String)contact.get("name")).split(" ")[0]+"@zew.org");
        hosts.put(contact);
        if(numberOfContacts>1) {
            JSONObject contact2 = new JSONObject();
            contact2.put("name", faker.name().fullName());
            contact2.put("phone", faker.phoneNumber().cellPhone());
            contact2.put("email", ((String)contact2.get("name")).split(" ")[0]+"@zew.org");
            hosts.put(contact2);
        }
        if(numberOfContacts>2) {
            JSONObject contact3 = new JSONObject();
            contact3.put("name", faker.name().fullName());
            contact3.put("phone", faker.phoneNumber().cellPhone());
            contact3.put("email", ((String)contact3.get("name")).split(" ")[0]+"@zew.org");
            hosts.put(contact3);
        }

        hostsHolder.put("hosts", hosts);
        obj.put("responsible-parties", hostsHolder);
        return obj;
    }
}

class ConnectionHelper{

    final PooledConnectionProvider connectionProvider;
    final JedisPooled jedisPooled;

    public Pipeline getPipeline(){
        return new Pipeline(connectionProvider.getConnection());
    }

    public JedisPooled getPooledJedis(){
        return jedisPooled;
    }

    public ConnectionHelper(URI uri){
        HostAndPort address = new HostAndPort(uri.getHost(), uri.getPort());
        JedisClientConfig clientConfig = null;
        System.out.println("$$$ "+uri.getAuthority().split(":").length);
        if(uri.getAuthority().split(":").length==3){
            String user = uri.getAuthority().split(":")[0];
            String password = uri.getAuthority().split(":")[1];
            System.out.println("\n\nUsing user: "+user+" / password @@@@@@@@@@");
            clientConfig = DefaultJedisClientConfig.builder().user(user).password(password)
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings

        }else {
            clientConfig = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings
        }
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxTotal(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWait(Duration.ofMinutes(1));
        poolConfig.setTestOnCreate(true);

        this.connectionProvider = new PooledConnectionProvider(new ConnectionFactory(address, clientConfig), poolConfig);
        this.jedisPooled = new JedisPooled(connectionProvider);
    }
}
