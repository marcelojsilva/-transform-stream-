## Source
https://kafka-tutorials.confluent.io/transform-a-stream-of-events/kstreams.html

## Run
Run docker-compose
```
docker-compose up -d
```

Create gradle wrapper
```
gradle wrapper
```

Build
```
./gradlew build
```

Create shadowJar
```
./gradlew shadowJar
```
Run jar
```
java -jar build/libs/kstreams-transform-standalone-0.0.1.jar configuration/dev.properties
```

Run console producer
```
docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --topic EstabComercial --broker-list broker:9092 --property value.schema="$(< src/main/avro/EstabComercial.avsc)"
```

Insert avro on console
```
{"id": 1, "CodEc": 121, "tipoEC": "ARTESP", "liberadoOperacao": 1}
{"id": 2, "CodEc": 122, "tipoEC": "ARTESP", "liberadoOperacao": 1}
{"id": 3, "CodEc": 123, "tipoEC": "ARTESP", "liberadoOperacao": 1}
{"id": 4, "CodEc": 124, "tipoEC": "ARTESP", "liberadoOperacao": 1}

{"id": 5, "CodEc": 125, "tipoEC": "ARTESP", "liberadoOperacao": 1}
{"id": 6, "CodEc": 126, "tipoEC": "ARTESP", "liberadoOperacao": 1}

{"id": 7, "CodEc": 127, "tipoEC": "ARTESP", "liberadoOperacao": 1}
```

Run console consumer
```
docker exec -it schema-registry /usr/bin/kafka-avro-console-consumer --topic SituacaoIdentificador --bootstrap-server broker:9092 --from-beginning
```

### Result
Output two lines for each EC
```
{"id":1,"CodEc":121,"idIdent":0,"tipoIdent":"tag","dataAlteracao":"2020-10-10 10:10:00","placa":"AAA1234","ativo":1,"bloqueado":0}
{"id":1,"CodEc":121,"idIdent":1,"tipoIdent":"tag","dataAlteracao":"2020-10-10 10:10:01","placa":"AAA1234","ativo":1,"bloqueado":0}
```