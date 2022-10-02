package com.redislabs.sa.ot.jzs;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.*;
import redis.clients.jedis.search.*;

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
 */
public class Main {

    private static final String[] DAYS_OF_WEEK = {"Mon","Tue","Wed","Thu","Fri","Sat","Sun"};
    private static final String INDEX_1_NAME = "idx_zew_events";
    private static final String PREFIX_FOR_SEARCH = "zew:activities:";

    public static void main(String[] args){
        String host = "192.168.1.20";
        Integer port = 12000;
        if(args.length>0){
            ArrayList<String> argList = new ArrayList<>(Arrays.asList(args));
            if(argList.contains("--host")){
                int hostIndex = argList.indexOf("--host");
                host = argList.get(hostIndex+1);
            }
            if(argList.contains("--port")){
                int portIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(portIndex+1));
            }
        }
        HostAndPort hnp = new HostAndPort(host,port);
        System.out.println("Connecting to "+hnp.toString());
        //Make sure index and alias are in place before we start writing data or querying:
        try{
            dropIndex(hnp);
            addIndex(hnp);
        }catch(Throwable t){
            System.out.println(""+t.getMessage());
            try{
                Jedis jedis = new Jedis(hnp.getHost(), hnp.getPort(), 500);
                System.out.println("There are "+jedis.dbSize()+" keys in the redis database");
            }catch(Throwable t2){System.out.println(""+t2.getMessage());}
        }
        System.out.println("LOADING JSON DATA...");
        loadData(hnp);
        System.out.println("TESTING SEARCH QUERY ...");
        testJSONSearchQuery(hnp);
    }

    private static void dropIndex(HostAndPort hnp) {
        try (UnifiedJedis jedis = new UnifiedJedis(hnp)) {
            jedis.ftDropIndex(INDEX_1_NAME);
        }catch(Throwable t){t.printStackTrace();}
    }

    /*
     * sample JSON that should be returned:
     * {"times":[{"military":"1100","civilian":"11 AM"}]
     * ,"responsible-parties":[{"phone":"715-322-5992","name":"Dr. Clarissa Gumali","email":"cgumali@zew.org"}]
     * ,"cost":10,"name":"Bonobo Lecture","days":["Mon","Thu"],"location":"Mammalian Lecture Theater"}
     * @param hnp
     */
    private static void testJSONSearchQuery(HostAndPort hnp) {
        try (UnifiedJedis jedis = new UnifiedJedis(hnp)) {
            String query = "@days:{Mon} -@location:('House')";
            SearchResult result = jedis.ftSearch(INDEX_1_NAME, new Query(query)
                    .returnFields(
                            FieldName.of("location"), // only a single value exists in a document
                            FieldName.of("$.times.*.civilian").as("first_event_time"), // only retuning 1st time in array due to use of *
                            FieldName.of("$.days").as("days"), // multiple days may be returned
                            FieldName.of("$.responsible-parties.hosts.[0].email").as("contact_email"), // Returning the first email only even though there could be more
                            FieldName.of("$.responsible-parties.hosts.[0].phone").as("contact_phone"), // Returning the first phone only even though there could be more
                            FieldName.of("event_name") // only a single value exists in a document
                    )
            );
            printResultsToScreen(query, result);

            //Second query:
            query = "@days:{Mon} @days:{Tue} @times:{08*}";
            result = jedis.ftSearch(INDEX_1_NAME, new Query(query)
                    .returnFields(
                            FieldName.of("location"), // only a single value exists in a document
                            FieldName.of("$.times.*.civilian").as("first_event_time"), // only retuning 1st time in array due to use of *
                            FieldName.of("$.times").as("all_times"), // multiple times may be returned when not filtered
                            FieldName.of("$.days").as("days"), // multiple days may be returned
                            FieldName.of("$.responsible-parties.hosts").as("hosts"),
                            FieldName.of("event_name"), // only a single value exists in a document
                            FieldName.of("$.responsible-parties.number_of_contacts").as("hosts_size")
                    )
            );
            printResultsToScreen(query, result);

            //Third query:
            query = "@cost:[-inf 5.00]";
            result = jedis.ftSearch(INDEX_1_NAME, new Query(query)
                    .returnFields(
                            FieldName.of("location"), // only a single value exists in a document
                            FieldName.of("$.times.*.civilian").as("first_event_time"), // only retuning 1st time in array due to use of *
                            FieldName.of("$.times.[1].civilian").as("second_event_time"), // only retuning 1st time in array due to use of *
                            FieldName.of("$.times").as("all_times"), // multiple times may be returned when not filtered
                            FieldName.of("$.days").as("days"), // multiple days may be returned
                            FieldName.of("event_name"), // only a single value exists in a document
                            FieldName.of("$.cost").as("cost_in_us_dollars")
                    )
            );
            printResultsToScreen(query, result);
        }
    }

    private static void printResultsToScreen(String query,SearchResult result){
        System.out.println("\n\tFired Query - \""+query+"\"\nHere is the response:\n");

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

    //load JSON Objects for testing
    private static void loadData(HostAndPort hnp){
        try (UnifiedJedis jedis = new UnifiedJedis(hnp)) {
            jedis.del("zew:activities:gf");
            jedis.del("zew:activities:bl");

            JSONObject obj = new JSONObject();
            obj.put("name", "Gorilla Feeding");
            obj.put("cost", 0.00);
            obj.put("location","Gorilla House South");

            JSONObject timeObj8am = new JSONObject();
            timeObj8am.put("military","0800");
            timeObj8am.put("civilian","8 AM");
            JSONObject timeObj3pm = new JSONObject();
            timeObj3pm.put("military","1500");
            timeObj3pm.put("civilian","3 PM");
            JSONObject timeObj10pm = new JSONObject();
            timeObj10pm.put("military","2200");
            timeObj10pm.put("civilian","10 PM");

            ArrayList<JSONObject> timeObjects = new ArrayList<>();
            timeObjects.add(timeObj8am);
            timeObjects.add(timeObj3pm);
            timeObjects.add(timeObj10pm);

            JSONArray times = new JSONArray(timeObjects);

            obj.put("times",times);
            JSONArray days = new JSONArray(DAYS_OF_WEEK);
            obj.put("days",days);

            JSONObject hostsHolder = new JSONObject();
            hostsHolder.put("number_of_contacts",2);
            JSONArray hosts = new JSONArray();
            JSONObject contact = new JSONObject();
            contact.put("name", "Duncan Mills");
            contact.put("phone","715-876-5522");
            contact.put("email","dmilla@zew.org");
            hosts.put(contact);
            JSONObject contact2 = new JSONObject();
            contact2.put("name", "Xiria Andrus");
            contact2.put("phone","815-336-5598");
            contact2.put("email","xiriaa@zew.org");
            hosts.put(contact2);
            hostsHolder.put("hosts",hosts);
            obj.put("responsible-parties",hostsHolder);
            jedis.jsonSet("zew:activities:gf", obj);

            //build second zew event:
            obj = new JSONObject();
            obj.put("name", "Bonobo Lecture");
            obj.put("cost", 10.00);
            obj.put("location","Mammalian Lecture Theater");
            times = new JSONArray();
            JSONObject timeObj11am = new JSONObject();
            timeObj11am.put("military","1100");
            timeObj11am.put("civilian","11 AM");
            times.put(timeObj11am);
            obj.put("times",times);
            days = new JSONArray();
            days.put(DAYS_OF_WEEK[0]);
            days.put(DAYS_OF_WEEK[3]);
            obj.put("days",days);
            hostsHolder = new JSONObject();
            hostsHolder.put("number_of_contacts",1);
            hosts = new JSONArray();
            contact = new JSONObject();
            contact.put("name", "Dr. Clarissa Gumali");
            contact.put("phone","715-322-5992");
            contact.put("email","cgumali@zew.org");
            hosts.put(contact);
            hostsHolder.put("hosts",hosts);
            obj.put("responsible-parties",hostsHolder);
            jedis.jsonSet("zew:activities:bl", obj);
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

    private static void addIndex(HostAndPort hnp){
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
        JedisPooled jedisPooled = new JedisPooled(new ConnectionPoolConfig(), hnp.getHost(), hnp.getPort(), 2000);

        Schema schema2 = new Schema().addField(new Schema.TextField(FieldName.of("$.name").as("event_name")))
                .addField(new Schema.Field(FieldName.of("$.cost").as("cost"), Schema.FieldType.NUMERIC))
                .addField(new Schema.Field(FieldName.of("$.days.*").as("days"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.times.*.military").as("times"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.location").as("location"), Schema.FieldType.TEXT));
        IndexDefinition indexDefinition = new IndexDefinition(IndexDefinition.Type.JSON)
                .setPrefixes(new String[]{PREFIX_FOR_SEARCH});

        jedisPooled.ftCreate(INDEX_1_NAME,IndexOptions.defaultOptions().setDefinition(indexDefinition),schema2);
        //AND THEN: add schema alias so we can toggle between indexes:
        jedisPooled.ftAliasAdd("idxa_zew_events",INDEX_1_NAME);
        System.out.println("Successfully created search index and search index alias");
    }
}
