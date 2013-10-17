

import groovy.sql.Sql

import org.apache.commons.lang3.StringUtils

String cassandra_url = this?.args.size() > 0 ? this.args[0] : "jdbc:cassandra://localhost:9160/system?version=3.0.0"

// SCHEMA:
//   -- mayfair will be the keyspace that the update and audit threads will pound
//   -- beatles will be the keyspace that the insert thread pounds
String schema = """
CREATE KEYSPACE mayfair_submission WITH REPLICATION= { 'class':'SimpleStrategy', 'replication_factor':1 };
CREATE TABLE mayfair_submission.Entity_Job (e_EntID text,e_EntName text,e_EntType text,e_EntLinks text,p_Prop text,p_Storage text,p_PropID text,p_Flags text,p_Val text,p_ValType text,p_ValUnit text,p_ValLang text,p_ValLinks text,p_Vars text,p_PropLinks text,p_SubEnts text,PartnerID text,UserID text,SubmitDate bigint,SourceIP text,SubmitEvent text,Size int,Describes text,Version text,IngestStatus text,IngestStatusDetail text,ReferenceID text,DNDCondition text,PRIMARY KEY (e_EntID,p_Prop)) with caching = 'keys_only';
CREATE INDEX mayfair_submission__JobUserIDX ON mayfair_submission.Entity_Job (UserID);
CREATE INDEX mayfair_submission__JobIngestStatusIDX ON mayfair_submission.Entity_Job (IngestStatus);
CREATE INDEX mayfair_submission__JobIngestStatusDetailIDX ON mayfair_submission.Entity_Job (IngestStatusDetail);
CREATE INDEX mayfair_submission__JobDNDConditionIDX ON mayfair_submission.Entity_Job (DNDCondition);
CREATE INDEX mayfair_submission__JobDescribesIDX ON mayfair_submission.Entity_Job (Describes);
CREATE INDEX mayfair_submission__JobVersionIDX ON mayfair_submission.Entity_Job (Version);
CREATE INDEX mayfair_submission__JobReferenceIDIDX ON mayfair_submission.Entity_Job (ReferenceID);

CREATE KEYSPACE beatles_submission WITH REPLICATION= { 'class':'SimpleStrategy', 'replication_factor':1 };
CREATE TABLE beatles_submission.Entity_Job (e_EntID text,e_EntName text,e_EntType text,e_EntLinks text,p_Prop text,p_Storage text,p_PropID text,p_Flags text,p_Val text,p_ValType text,p_ValUnit text,p_ValLang text,p_ValLinks text,p_Vars text,p_PropLinks text,p_SubEnts text,PartnerID text,UserID text,SubmitDate bigint,SourceIP text,SubmitEvent text,Size int,Describes text,Version text,IngestStatus text,IngestStatusDetail text,ReferenceID text,DNDCondition text,PRIMARY KEY (e_EntID,p_Prop)) with caching = 'keys_only';
CREATE INDEX beatles_submission__JobUserIDX ON beatles_submission.Entity_Job (UserID);
CREATE INDEX beatles_submission__JobIngestStatusIDX ON beatles_submission.Entity_Job (IngestStatus);
CREATE INDEX beatles_submission__JobIngestStatusDetailIDX ON beatles_submission.Entity_Job (IngestStatusDetail);
CREATE INDEX beatles_submission__JobDNDConditionIDX ON beatles_submission.Entity_Job (DNDCondition);
CREATE INDEX beatles_submission__JobDescribesIDX ON beatles_submission.Entity_Job (Describes);
CREATE INDEX beatles_submission__JobVersionIDX ON beatles_submission.Entity_Job (Version);
CREATE INDEX beatles_submission__JobReferenceIDIDX ON beatles_submission.Entity_Job (ReferenceID);
"""

String dropmayfair = """
DROP KEYSPACE mayfair_submission
"""
String dropbeatles = """
DROP KEYSPACE beatles_submission
"""


Sql cqlSCHEMA = Sql.newInstance(cassandra_url, null, null, "org.apache.cassandra.cql.jdbc.CassandraDriver")

try {
  cqlSCHEMA.execute(dropmayfair)
} catch (Exception e) {
  println("ERROR exec "+dropmayfair)
}

try {
  cqlSCHEMA.execute(dropbeatles)
} catch (Exception e) {
  println("ERROR exec "+dropbeatles)
}

String[] cqls = schema.split(";")

cqls.each { cqlstatement ->
  if (StringUtils.isNotBlank(cqlstatement)) {
    cqlSCHEMA.execute(cqlstatement.trim())
  }
}

println("SCHEMA created")


