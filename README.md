Kafka tools to work with confluent kafka server through HTTP REST proxy. 

# Configuration options


| section                 | option      | default value |
|-------------------------|-------------|---------------|
| ConfluentSchemaRegistry | server      |               |
| ConfluentSchemaRegistry | user        |               |
| ConfluentSchemaRegistry | password    |               |
| ConfluentSchemaRegistry | localSchema |               |
|-------------------------|-------------|---------------|
| ConfluentRestProxy      | server      |               |
| ConfluentRestProxy      | user        |               |
| ConfluentRestProxy      | password    |               |
|-------------------------|-------------|---------------|


## options added since version 2
|--------------------|-------------|-----------------------------|
| ConfluentRestProxy | outboxFile  | /tmp/kafka.outbox           |
| ConfluentRestProxy | outboxLimit | 200000. Set to 0 to disable |
| ConfluentRestProxy | timeToSave  | 30000                       |
|--------------------|-------------|-----------------------------|


## example config

[ConfluentRestProxy]
server=http://1.1.1.1:2222
user=user
password=pass


[ConfluentSchemaRegistry]
server=http://1.1.1.1:2222
user=user
password=pass

[gps]
port=/dev/ttyUSB3
reportInterval=2000

