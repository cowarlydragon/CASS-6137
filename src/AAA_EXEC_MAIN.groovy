/*
 Main execution / coordination script. 
 To reproduce apparently requires:
 1) a set of existing data (this is created in SETUP_CREATE_INITIAL_DATA), we create 20,000 rows
 2) start three threads:
 ==> an AUDIT thread constantly checking for column key subsets that don't match
 ==> an INSERT thread providing a large amount of fresh data insertion (it goes to a different keyspace)
 ==> an UPDATE thread performing intermittent updates to existing rows (the 20,000 we create)
 3) a bit of time (I'm trying to get a number on this, but perhaps an hour or two, or perhaps some total volume threshold or compaction exec time)
 When you see !!NOMATCH!! messages appear on the console, it has appeared. 
 */

String cassandra_url = "jdbc:cassandra://localhost:9160/system?version=3.0.0"

Binding binding = new Binding()
binding.setVariable("args", new String[0])
binding.setVariable("GLOBAL_entityIDs", [] as Set)
binding.setVariable("GLOBAL_attributes", [])

initialize_environment = true

if (initialize_environment) {
  (new SETUP_CREATE_SCHEMA(binding)).run()
  (new SETUP_CREATE_INITIAL_DATA(binding)).run()
} else {
  (new SELECT_EXISTING_ENTITY_IDS(binding)).run()
}

// kickoff the threads: AUDIT checks for the problem, UPDATE does intermittent mutations to existing data, INSERT is shoving in new data as fast as possible
(new THREAD_AUDIT(binding)).run()
(new THREAD_UPDATE(binding)).run()
(new THREAD_INSERT(binding)).run()
