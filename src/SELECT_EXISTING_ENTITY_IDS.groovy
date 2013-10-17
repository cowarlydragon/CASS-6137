

import groovy.sql.Sql


println("SELECT IDS:BEGIN")

String cassandra_url = this?.args.size() > 0 ? this.args[0] : "jdbc:cassandra://localhost:9160/system?version=3.0.0"

Sql cqlIDs = Sql.newInstance(cassandra_url, null, null, "org.apache.cassandra.cql.jdbc.CassandraDriver")

cqlIDs.eachRow("SELECT e_entid FROM mayfair_submission.entity_job") { row ->
  GLOBAL_entityIDs.add(row.e_entid)
}

println("SELECT IDS:DONE")
