package com.abanca.msv.kafka.apps.v1;

import com.example.Customer;
import com.myvaluesolutions.abanca.kafka.avro.TransaccionCategorizable;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroJavaConsumerV1Demo {

    public static void main(String[] args) {

        String bootstrapServers = "xxdkafkaxxxxs01:9092";

        Properties properties = new Properties();
        // normal consumer
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", "categorizables-consumer-group-v2");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "latest");




        properties.put("ssl.truststore.location","C:\\Users\\u024098\\proxectos\\ksql\\config\\jssecacerts");
        properties.put("ssl.truststore.password","changeit");
        //   properties.put("ssl.truststore.location","C:\\Users\\u024098\\confluent\\InstallCert\\jssecacerts");
        //   properties.put("ssl.truststore.password","changeit");
        properties.put("security.protocol","SASL_SSL");
        properties.put("sasl.mechanism","SCRAM-SHA-512");
        properties.put("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"client\" password=\"client-secret\";");

        // avro part (deserializer)

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "https://xxdkafkaxxxxs02:8081");
        properties.setProperty("specific.avro.reader", "true");


        KafkaConsumer<String, TransaccionCategorizable> kafkaConsumer = new KafkaConsumer<>(properties);
        String topic = "QMYV.MYVALUE.CGE.MovimientosCuentaCategorizables";
        kafkaConsumer.subscribe(Collections.singleton(topic));


        System.out.println("Waiting for data...");

        while (true){
            System.out.println("Polling");
            ConsumerRecords<String, TransaccionCategorizable> records = kafkaConsumer.poll(1000);

            for (ConsumerRecord<String, TransaccionCategorizable> record : records){
                TransaccionCategorizable transaccionCategorizable = record.value();
                System.out.println(transaccionCategorizable);
            }

            kafkaConsumer.commitSync();
        }
    }
}
