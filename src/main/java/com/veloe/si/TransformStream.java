package com.veloe.si;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.veloe.si.avro.EstabComercial;
import com.veloe.si.avro.Identificador;
import com.veloe.si.avro.SituacaoIdentificador;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class TransformStream {

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String ecTopic = envProps.getProperty("estabComercial.input.topic.name");
        final String rekeyedEcTopic = ecTopic.concat("Rekeyed");
        final String identTopic = envProps.getProperty("identificador.input.topic.name");
        final String siOutputTopic = envProps.getProperty("si.output.topic.name");

        // KStream<String, Movie> movieStream = builder.<String, Movie>stream(movieTopic)
        //         .map((key, movie) -> new KeyValue<>(String.valueOf(movie.getId()), movie));
        // movieStream.to(rekeyedMovieTopic);
        // KTable<String, Movie> movies = builder.table(rekeyedMovieTopic);
        // KStream<String, Rating> ratings = builder.<String, Rating>stream(ratingTopic)
        //         .map((key, rating) -> new KeyValue<>(String.valueOf(rating.getId()), rating));
        // KStream<String, RatedMovie> ratedMovie = ratings.join(movies, joiner);
        // ratedMovie.to(ratedMoviesTopic, Produced.with(Serdes.String(), ratedMovieAvroSerde(envProps)));

        // KStream<String, EstabComercial> estabComercialStream = builder.stream(ecTopic)
        //         .map((key, ec) -> new KeyValue<>(String.valueOf(ec.getId()), ec));
        
        // estabComercialStream.to(rekeyedEcTopic);
        // KTable<String, EstabComercial> estabComerciais = builder.table(rekeyedEcTopic);

        // KStream<String, Identificador> identificador = builder.stream(identTopic);

        // KStream<Long, SituacaoIdentificador> si = identificador.flatMap((key, ident) -> {
        //     List<KeyValue<Long, SituacaoIdentificador>> result = convertEcToSI(estabComerciais, ident);
        //     return result;
        //     }
        // );   

        KStream<String, EstabComercial> estabComerciais = builder.stream(ecTopic);
        KStream<Long, SituacaoIdentificador> si = estabComerciais.flatMap((key, estabComercial) -> {
            List<KeyValue<Long, SituacaoIdentificador>> result = convertEcToSI(estabComercial);
            return result;
            }
        );                    


        si.to(siOutputTopic, Produced.with(Serdes.Long(), siAvroSerde(envProps)));

        return builder.build();
    }

    // public static List<KeyValue<Long, SituacaoIdentificador>> convertEcToSI(EstabComercial estabComerciais, Identificador ident) {
    //     List<KeyValue<Long, SituacaoIdentificador>> si = new LinkedList<>();
    //     si.add(KeyValue.pair(estabComerciais.getId(), new SituacaoIdentificador(estabComerciais.getId(), estabComerciais.getCodEc(), 0L, "tag", "2020-10-10 10:10:00", "AAA1234", 1, 0)));
    //     si.add(KeyValue.pair(estabComerciais.getId(), new SituacaoIdentificador(estabComerciais.getId(), estabComerciais.getCodEc(), 1L, "tag", "2020-10-10 10:10:01", "AAA1234", 1, 0)));

    //     return si;
    // }

    public static List<KeyValue<Long, SituacaoIdentificador>> convertEcToSI(EstabComercial estabComercial) {
        List<KeyValue<Long, SituacaoIdentificador>> si = new LinkedList<>();
        si.add(KeyValue.pair(estabComercial.getId(), new SituacaoIdentificador(estabComercial.getId(), estabComercial.getCodEc(), 0L, "tag", "2020-10-10 10:10:00", "AAA1234", 1, 0)));
        si.add(KeyValue.pair(estabComercial.getId(), new SituacaoIdentificador(estabComercial.getId(), estabComercial.getCodEc(), 1L, "tag", "2020-10-10 10:10:01", "AAA1234", 1, 0)));

        return si;
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