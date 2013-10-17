import groovy.sql.Sql

import org.apache.commons.lang3.StringUtils

import au.com.bytecode.opencsv.CSVReader


/**
 * This scriptlet does mass insertion of records into beatles_submission.entity_job, using a file of 350,000 company addresses and CQL3 batches
 * 
 */

String cassandra_url = this?.args.size() > 0 ? this.args[0] : "jdbc:cassandra://localhost:9160/system?version=3.0.0"

String addresses_file = "src/350000.csv"

def cql = Sql.newInstance(cassandra_url, null, null, "org.apache.cassandra.cql.jdbc.CassandraDriver")


def esc = { it -> StringUtils.replace(it.toString(), "'", "''") }
def uuid = { UUID.randomUUID() }


// do five passes (that'll be five passes of 350,000 rows being inserted)
(1..5).each {
  println("INSERT start")
  long start = new Date().time
  long insertCount = 0
  CSVReader reader = new CSVReader(new FileReader(addresses_file))

  String[] line = null
  while ((line = reader.readNext()) != null) {
    StringWriter batch = new StringWriter()
    batch.append("BEGIN BATCH\n")
    String id = "" + UUID.randomUUID() + "-CJOB"
    String name = "$line[1], $line[0]"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,e_entname) VALUES ('$id','__CPSYS_name','$esc(name)');\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,e_enttype) VALUES ('$id','__CPSYS_type','urn:bby:pcm:job');\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,e_entlinks) VALUES ('$id','__CPSYS_links',NULL);\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,p_propid,p_flags,p_val) VALUES('$id','urn:bby:person:firstname','$uuid()','DS1','$esc(line[0])');\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,p_propid,p_flags,p_val) VALUES('$id','urn:bby:pcm:job:sourceparty:uid','$uuid()','DS1','$esc(line[1])');\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,p_propid,p_flags,p_val) VALUES('$id','urn:bby:client:company','$uuid()','DS1','$esc(line[2])');\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,p_propid,p_flags,p_val) VALUES('$id','urn:bby:address:street','$uuid()','DS1','$esc(line[3])');\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,p_propid,p_flags,p_val) VALUES('$id','urn:bby:address:city','$uuid()','DS1','$esc(line[4])');\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,p_propid,p_flags,p_val) VALUES('$id','urn:bby:pcm:ingest:status','$uuid()','DS1','$esc(line[6])');\n"
    batch.append "INSERT INTO beatles_submission.entity_job(e_entid,p_prop,p_propid,p_flags,p_val) VALUES('$id','zip','$uuid()','DS1','$esc(line[7])');\n"
    batch.append "APPLY BATCH;"
    cql.execute(batch.toString())
    insertCount++
    if (insertCount % 10000 == 0) println("INSERT at "+insertCount)
  }
  long end = new Date().time

  sleep(20000)

  println("INSERT time: ${end-start}")
}
