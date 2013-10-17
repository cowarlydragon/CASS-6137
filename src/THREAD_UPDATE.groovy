import groovy.sql.Sql

import org.apache.commons.lang3.StringUtils


/**
 * This scriptlet does a steady stream of mutations to existing data. Some may be new column key 
 * mutations, while others will be overwrites. It uses a file of UTF-8 test data to get the data 
 * values to assign.
 *
 */


String cassandra_url = this?.args.size() > 0 ? this.args[0] : "jdbc:cassandra://localhost:9160/system?version=3.0.0"

Sql cqlUPDATE = Sql.newInstance(cassandra_url, null, null, "org.apache.cassandra.cql.jdbc.CassandraDriver")


// local cassandra

String keyspaceScope = "wayfair"

// create the server

String quickbrown_file = "src/testdata-quickbrown.txt"
String utf8stress_file = "src/UTF-8-test.txt"

List<String> quicklines = new File(quickbrown_file).readLines("UTF8")
List<String> utf8lines = new File(utf8stress_file).readLines("UTF8")
List<String> evildata = quicklines + utf8lines

boolean keepgoing = true
Random rand = new Random(new Date().time)

List<String> entIDList = []
entIDList.addAll(GLOBAL_entityIDs)

def threadUPDATE = Thread.start {

  println("UPDATE start")
  long updateCount = 0
  while (keepgoing) {
    String randomEnt = entIDList[rand.nextInt(entIDList.size())]
    try {
      String prop = GLOBAL_attributes[rand.nextInt(GLOBAL_attributes.size())]
      String INSERT = "BEGIN BATCH INSERT INTO mayfair_submission.entity_job (e_entid, p_prop, p_val) VALUES ('"+randomEnt+"','"+prop+"','"+StringUtils.replace(evildata[rand.nextInt(evildata.size())].toString(), "'", "''")+"'); APPLY BATCH;"
      cqlUPDATE.execute(INSERT)
    } catch (Exception e) {
    }
    updateCount++
    sleep(rand.nextInt(200))
    if (updateCount %1000 == 0) println "UPDATE count: "+updateCount
  }

}
