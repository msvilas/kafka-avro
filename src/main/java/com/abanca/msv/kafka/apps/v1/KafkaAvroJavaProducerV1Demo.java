package com.abanca.msv.kafka.apps.v1;

import com.myvaluesolutions.abanca.kafka.avro.TransaccionCategorizable;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.*;

import java.math.BigDecimal;
import java.util.Properties;

import static java.lang.System.*;


public class KafkaAvroJavaProducerV1Demo {

    public static void main(String[] args) {

        // String bootstrapServers = "localhost:9092";
        String bootstrapServers = "xxdkafkaxxxxs01:9092";
        //String bootstrapServers = "xxekafkaxxxxs01:9092,xxekafkaxxxxs02:9092,xxekafkaxxxxs05:9092,xxekafkaxxxxs06:9092";

        // create Producer properties
        Properties properties = new Properties();

        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("ssl.truststore.location","C:\\Users\\u024098\\proxectos\\ksql\\config\\jssecacerts");
        properties.put("ssl.truststore.password","changeit");
     //   properties.put("ssl.truststore.location","C:\\Users\\u024098\\confluent\\InstallCert\\jssecacerts");
     //   properties.put("ssl.truststore.password","changeit");
        properties.put("security.protocol","SASL_SSL");
        properties.put("sasl.mechanism","SCRAM-SHA-512");
        properties.put("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"client\" password=\"client-secret\";");


        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "https://xxdkafkaxxxxs02:8081");

        Producer<String, TransaccionCategorizable> producer = new KafkaProducer<String, TransaccionCategorizable>(properties);

        String topic = "QMYV.MYVALUE.CGE.MovimientosCuentaCategorizables";

        // copied from avro examples
        TransaccionCategorizable transaccionCategorizable = TransaccionCategorizable.newBuilder()
                .setCodigoOperacion("980")
                .setConcepto("GAS NATURAL FENOSA")
                .setCodigoTransaccion("")
                .setConceptoAmpliado("FACTURA ELECTRICIDAD NOVIEMBRE 2020")
                .setDescripcionOperacion("ADEUDO SEPA")
                .setFechaOperacion(Instant.now())
                .setFechaValor(Instant.now())
                .setImporte(BigDecimal.valueOf(23.00))
                .setMovimientoId("A30099999999902012-12-282012-12-28-00.04.00.123456")
                .setProductoId("ARA300999999999")
                .setSaldoFinal(BigDecimal.valueOf(12324.45)).build();

        ProducerRecord<String, TransaccionCategorizable> producerRecord = new ProducerRecord<String, TransaccionCategorizable>(
                topic, transaccionCategorizable
        );

        out.println(transaccionCategorizable);


 /*
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String >("mytopic2", "hola mundo msv");
*/

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();

    }
}
