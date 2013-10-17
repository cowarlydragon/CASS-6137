

import groovy.sql.Sql


String cassandra_url = this?.args.size() > 0 ? this.args[0] : "jdbc:cassandra://localhost:9160/system?version=3.0.0"

Sql cqlAUDIT = Sql.newInstance(cassandra_url, null, null, "org.apache.cassandra.cql.jdbc.CassandraDriver")


def report = { success,message ->
  boolean showMatch = false
  if (success) {
    /* ignore */
  } else {
    println(message)
  }
}

def randomProps = { proplist, numprops, rand ->
  List<String> propcopy = []
  proplist.each { it -> propcopy += proplist }
  def rndprop = []
  for (int i=0; i < numprops; i++) {
    if (propcopy.size() < 1) {
      return rndprop
    }
    int idx = rand.nextInt(propcopy.size())
    rndprop += propcopy[idx]
    propcopy = propcopy - propcopy[idx]
  }
  return rndprop
}

def inClause = { propset ->
  boolean first = true
  StringBuffer inclause = new StringBuffer()
  propset.each { it ->
    if (first) first = false; else inclause.append(',')
    inclause.append("'").append(it).append("'")
  }
  return inclause.toString()
}

def foundRows = { keyspace,table,entid,incl ->
  List<String> foundProps = []
  String cqlselect = "SELECT * FROM "+keyspace+"."+table+" WHERE e_entid = '$entid' AND p_prop IN ($incl)"
  cqlAUDIT.eachRow(cqlselect) {
    foundProps.add(it.p_prop)
  }
  return [foundProps, cqlselect]
}

def getPropNames = { keyspace, table, entid ->
  String getprops = "SELECT p_prop FROM "+keyspace+"."+table+" WHERE e_entid = '$entid'"
  List<String> proplist = []
  cqlAUDIT.eachRow(getprops) { proplist += it.p_prop }
  return proplist
}

def executeWHEREIN = { keyspace,table,proplist,incSize,entid,r ->
  List<String> propselect = randomProps(proplist,incSize,r)
  List<String> propfound = null
  (propfound,cqlstmt) = foundRows(keyspace,table,entid,inClause(propselect))
  if (propselect.sort() == propfound.sort()) {
    return true
  } else {
    report(false,"NOMATCH ID: $entid FOR: $propselect to $propfound CQL: $cqlstmt")
    return false
  }
}

def threadAudit = Thread.start {

  boolean showMatch = true
  Random r = new Random(new Date().time)
  String keyspace = "mayfair_submission"
  String table = "entity_job"

  while (true) {
    try {
      println("AUDIT:: BEGIN")
      int rowcount = 0
      int successes = 0
      int failures = 0

      // this should still be active in the binding scope from SETUP_CREATE_INITIAL_DATA
      GLOBAL_entityIDs.each { String entid ->
        boolean successRow = true
        List<String> proplist = getPropNames(keyspace,table,entid)
        for (int ii=0; ii < 16; ii++) {
          successRow = executeWHEREIN(keyspace,table,proplist,4,entid,r)
          if (!successRow) {
            break
          }
        }
        if (successRow) successes++ else failures++
      }
      println("AUDIT::END stats: $successes succeeded, $failures failed")
    } catch (Exception e) {
      // report error for table, then try next table
      println("ERROR[TABLE]: "+e.getMessage())
    }
  }

}



