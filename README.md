# UUIDChangeSetGenerator
throw away UUIDChangeSetGenerator code. creates liquibase script to convert bsis table ids from BIGINT to UUID 

# To Build
mvn clean compile assembly:single

# To run
java -jar [jarname] [tableName] [dbUserName] [dbPassword] [force]
Note:
- tableName is mandatory
- if dbUserName is specified, dbPassword must be specified
- if force is specified, the db username and password must be specified. The force parameter forces
the generation of the changeset regardless of whether there are foreign keys in the AUDIT table
