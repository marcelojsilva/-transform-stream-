package com.veloe.si;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.naming.Context;
import javax.naming.spi.DirStateFactory.Result;

import com.veloe.si.avro.EstabComercial;
import com.veloe.si.avro.GrupoEcAggr;
import com.veloe.si.avro.Identificador;
import com.veloe.si.avro.IdentificadorEcResult;
// import com.veloe.si.avro.IdentificadorEcResultJoiner;
import com.veloe.si.avro.SituacaoIdentificador;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class TransformStream {

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String ecTopic = envProps.getProperty("estabComercial.input.topic.name");
        final String grupoEcAggrTopic = envProps.getProperty("grupoEcAggr.input.topic.name");
        final String identTopic = envProps.getProperty("identificador.input.topic.name");
        final String siOutputTopic = envProps.getProperty("si.output.topic.name");

        // KStream<Long, EstabComercial> ec = builder.<Long, EstabComercial>stream(ecTopic)
        //     .selectKey ((k,v) -> v.getId())
        //     .filter((k,v) -> v.getLiberadoOperacao() == 1);

        // KStream<Long, EstabComercial> estabComercialTbl = ec
        //     .groupByKey()
        //     .reduce((v1,v2) -> v2)
        //     .toStream();
        //     // .toTable(Materialized.with(Serdes.Long(), ecAvroSerde(envProps)));



        KStream<Long, EstabComercial> ec2 = builder.<Long, EstabComercial>stream(ecTopic)
            .selectKey ((k,v) -> v.getCodGrupoEc())
            .filter((k,v) -> v.getLiberadoOperacao() == 1);
        
        ec2.to(grupoEcAggrTopic);

        KTable<Long, GrupoEcAggr> grupoEc = builder.stream(grupoEcAggrTopic,
            Consumed.with(Serdes.Long(), ecAvroSerde(envProps)))
            .selectKey ((k,v) -> v.getCodGrupoEc())
            .groupByKey(Grouped.with(Serdes.Long(), ecAvroSerde(envProps)))
            .aggregate(
                // Initialized Aggregator
                GrupoEcAggr::new,
                //Aggregate
                (idGrupoEc, estab, grupoEcAggr) -> {
                    grupoEcAggr.setCodGrupoEc(idGrupoEc);
                    grupoEcAggr.getEcs().add(estab);
                    return grupoEcAggr;
                },
                // store in materialied view GrupoEcAggr
                Materialized.<Long, GrupoEcAggr, KeyValueStore<Bytes, byte[]>>
                    as("GrupoEcAggr")
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(grupoEcAvroSerde(envProps)))
        ;
        
        KTable<Long, Identificador> ident = builder.<Long, Identificador>stream(identTopic)
            .selectKey ((k,v) -> v.getCodGrupoEc())
            .toTable();
        
        

        KStream<Long, IdentificadorEcResult> empResultTable =
            ident.join(grupoEc, 
                (identificador, grupoEcAggr) -> {
                    return IdentificadorEcResult.newBuilder()
                            .setId(identificador.getId())
                            .setIdentificador(identificador)
                            .setEcs(grupoEcAggr.getEcs())
                            .build();
                },
                // store in materialied view EMP-RESULT-MV
                Materialized.<Long, IdentificadorEcResult, KeyValueStore<Bytes, byte[]>>
                    as("EMP-RESULT-MV")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(identEcResultAvroSerde(envProps))
        ).toStream();

        KStream<Long, SituacaoIdentificador> si = empResultTable.flatMap((k, v) -> {
            List<KeyValue<Long, SituacaoIdentificador>> result = convertEcToSI(v);
            return result;
            }
        );                    


        si.to(siOutputTopic, Produced.with(Serdes.Long(), siAvroSerde(envProps)));

        return builder.build();
    }

    public static List<KeyValue<Long, SituacaoIdentificador>> convertEcToSI(IdentificadorEcResult identEc) {
        List<KeyValue<Long, SituacaoIdentificador>> si = new LinkedList<>();

        for (EstabComercial ec : identEc.getEcs()) {
            si.add(KeyValue.pair(identEc.getId(), new SituacaoIdentificador(identEc.getId(), ec.getCodEc(), identEc.getId(), identEc.getIdentificador().getTipoIdent(), identEc.getIdentificador().getDataAlteracao(), identEc.getIdentificador().getPlaca(), identEc.getIdentificador().getAtivo(), identEc.getIdentificador().getBloqueioSaldo())));
        }
        // si.add(KeyValue.pair(identEc.getId(), new SituacaoIdentificador(identEc.getId(), ec.getCodEc(), 0L, "tag", "2020-10-10 10:10:00", "AAA1234", 1, 0)));
        // si.add(KeyValue.pair(identEc.getId(), new SituacaoIdentEcificador(identEc.getId(), identEc.getCodEc(), 1L, "tag", "2020-10-10 10:10:01", "AAA1234", 1, identEc.getLiberadoOperacao())));
        
        return si;
    }

    private SpecificAvroSerde<EstabComercial> ecAvroSerde(Properties envProps) {
        SpecificAvroSerde<EstabComercial> ecAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
                        envProps.getProperty("schema.registry.url"));

        ecAvroSerde.configure(serdeConfig, false);
        return ecAvroSerde;
    }

    private SpecificAvroSerde<GrupoEcAggr> grupoEcAvroSerde(Properties envProps) {
        SpecificAvroSerde<GrupoEcAggr> grupoEcAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
                        envProps.getProperty("schema.registry.url"));

        grupoEcAvroSerde.configure(serdeConfig, false);
        return grupoEcAvroSerde;
    }

    private SpecificAvroSerde<IdentificadorEcResult> identEcResultAvroSerde(Properties envProps) {
        SpecificAvroSerde<IdentificadorEcResult> identEcResultAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
                        envProps.getProperty("schema.registry.url"));

        identEcResultAvroSerde.configure(serdeConfig, false);
        return identEcResultAvroSerde;
    }

    private SpecificAvroSerde<Identificador> identAvroSerde(Properties envProps) {
        SpecificAvroSerde<Identificador> identAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
                        envProps.getProperty("schema.registry.url"));

        identAvroSerde.configure(serdeConfig, false);
        return identAvroSerde;
    }

    private SpecificAvroSerde<SituacaoIdentificador> siAvroSerde(Properties envProps) {
        SpecificAvroSerde<SituacaoIdentificador> siAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
                        envProps.getProperty("schema.registry.url"));

        siAvroSerde.configure(serdeConfig, false);
        return siAvroSerde;
    }

    public void createTopics(Properties envProps) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
        AdminClient client = AdminClient.create(config);

        List<NewTopic> topics = new ArrayList<>();

        topics.add(new NewTopic(
                envProps.getProperty("estabComercial.input.topic.name"),
                Integer.parseInt(envProps.getProperty("estabComercial.input.topic.partitions")),
                Short.parseShort(envProps.getProperty("estabComercial.input.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("grupoEcAggr.input.topic.name"),
                Integer.parseInt(envProps.getProperty("estabComercial.input.topic.partitions")),
                Short.parseShort(envProps.getProperty("estabComercial.input.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("identificador.input.topic.name"),
                Integer.parseInt(envProps.getProperty("identificador.input.topic.partitions")),
                Short.parseShort(envProps.getProperty("identificador.input.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("si.output.topic.name"),
                Integer.parseInt(envProps.getProperty("si.output.topic.partitions")),
                Short.parseShort(envProps.getProperty("si.output.topic.replication.factor"))));

        client.createTopics(topics);
        client.close();
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        TransformStream ts = new TransformStream();
        Properties envProps = ts.loadEnvProperties(args[0]);
        Properties streamProps = ts.buildStreamsProperties(envProps);
        Topology topology = ts.buildTopology(envProps);

        ts.createTopics(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}