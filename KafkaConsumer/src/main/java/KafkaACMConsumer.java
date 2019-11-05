import java.util.Arrays;
import java.util.Properties;
import java.time.Duration;
import java.util.Collections;



import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
//import org.apache.pulsar.client.impl.schema.AvroSchema;
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
//import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;



public class KafkaACMConsumer{

    private final static String TOPIC = "OldGateThree.cust_ellisdon.project";//,"projectService.cust_ellisdon.cnf_user","projectService.cust_ellisdon.cnf_userrole","projectService.cust_ellisdon.cnf_module","projectService.cust_ellisdon.cnf_acllevel" ));
    private final static String BOOTSTRAP_SERVERS = "kafka-broker-0.c.radix-shared-inf.internal:9092, kafka-broker-1.c.radix-shared-inf.internal:9092, kafka-broker-2.c.radix-shared-inf.internal:9092";
    private final static String GROUP_ID =  "test-payments";
    private final static String SCHEMA_REGISTRY_URL =  "http://10.104.120.22:8081";
    public static void main(String[] args) {


        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);


       /*Properties props = new Properties();
         props.put("group.id", GROUP_ID);
         props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
         props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
         props.put("enable.auto.commit", "true");
         props.put("auto.commit.interval.ms", "1000");*/

       /* props.put("group.id", "acm-testConsumer");
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

            props.put("specific.avro.reader", "true");
*/

        // props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //   props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


       /* props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://10.104.120.22:8081");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(200L);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

            }
        }*/

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(100);
                for (final ConsumerRecord<String, String> record : records) {
                    final Long offset = record.offset();
                    final String key = record.key();
                    final String value = record.value();
                    System.out.printf("offset = %d, key = %s, value = %s%n",offset, key, value);
                }
            }

        }



    }
}



