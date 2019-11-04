import java.util.Arrays;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Arrays;
import java.util.Properties;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
//http://10.104.120.22:8081


public class KafkaConsumer {

    private final static String TOPIC = "acm-streams-pipe";
    private final static String BOOTSTRAP_SERVERS = "kafka-broker-0.c.radix-shared-inf.internal:9092, kafka-broker-1.c.radix-shared-inf.internal:9092, kafka-broker-2.c.radix-shared-inf.internal:9092";//"localhost:9092,localhost:9093,localhost:9094";
    private final static String KEY_CONNECT_NAME = "projectService.cust_ellisdon.project.Key";

    public static void main(String[] args) {


        Properties props = new Properties();

        // props.put(StreamsConfig.APPLICATION_ID_CONFIG, TOPIC);
        props.put("group.id", "testConsumer");
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.104.120.22:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        final Serde<String> stringSerde = Serdes.String();
        // StreamsBuilder builder = new StreamsBuilder();
        // builder.stream("OldGateThree.cust_ellisdon.project", Consumed.with(keyGenericAvroSerde, valueGenericAvroSerde));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("OldGateThree.cust_ellisdon.project"));//,"projectService.cust_ellisdon.cnf_user","projectService.cust_ellisdon.cnf_userrole","projectService.cust_ellisdon.cnf_module","projectService.cust_ellisdon.cnf_acllevel" ));
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

            }
        }

    }
}



