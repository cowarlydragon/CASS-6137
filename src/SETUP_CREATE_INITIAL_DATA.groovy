

import groovy.sql.Sql


String cassandra_url = this?.args.size() > 0 ? this.args[0] : "jdbc:cassandra://localhost:9160/system?version=3.0.0"

// create about two thousand rows

def cqlINITDATA = Sql.newInstance(cassandra_url, null, null, "org.apache.cassandra.cql.jdbc.CassandraDriver")

Random rand = new Random(new Date().time)

GLOBAL_attributes += [
  "urn:bby:pcm:ingest:status:detail",
  "urn:bby:pcm:ingest:status",
  "urn:bby:pcm:job:sourceparty:reference:id",
  "bby:submission:SourceIP",
  "urn:bby:pcm:job:sourceparty:id",
  "urn:bby:pcm:job:sourceparty:uid",
  "dct:dateSubmitted",
  "urn:bby:pcm:job:ingest:content:complete:count",
  "urn:bby:pcm:job:ingest:content:fail:count",
  "urn:bby:pcm:job:ingest:content:warn:count",
  "urn:bby:pcm:job:ingest:content:success:count",
  "urn:bby:pcm:job:ingest:content:hold:count",
  "urn:bby:pcm:ingest:data",
  "DISABLED__SYSTEM__BYPASS__PROP__SUBSET__",
  "dbpedia:Global_Trade_Item_Number",
  "urn:bby:pcm:attribute:pimcat",
  "urn:bby:pcm:definition:describes",
  "urn:bby:pcm:definition:version",
  "dbpedia:Stock_keeping_unit",
  "urn:bby:pcm:attribute:miid",
  "urn:bby:pcm:attribute:pimcat",
  "SDjfk:3829jdsf:sdjfl:8sd98hsdf:nnji",
  "a:count",
  "b:count",
  "b:count",
  "count:",
  "b:count:a",
  "b:count:b",
  "b:count:c",
  "__CPSYS_type",
  "__CPSYS_links",
]


String quickbrown_file = "src/testdata-quickbrown.txt"
String utf8stress_file = "src/UTF-8-test.txt"

List<String> quicklines = new File(quickbrown_file).readLines("UTF8")
List<String> utf8lines = new File(utf8stress_file).readLines("UTF8")
List<String> evildata = quicklines + utf8lines

(1..20000).each {
  String entID = ""+ UUID.randomUUID() + "_CJOB"
  GLOBAL_entityIDs.add(entID)
  String INSERT = "INSERT INTO mayfair_submission.entity_job (e_entid, p_prop, p_val) VALUES (?,?,?)"
  for (String attr : GLOBAL_attributes) {
    if (rand.nextInt(100) < 80) {
      cqlINITDATA.execute(INSERT,[
        entID,
        attr,
        evildata[rand.nextInt(evildata.size())]
      ])
    }
  }
}

println("INSERT INIT Completed")
