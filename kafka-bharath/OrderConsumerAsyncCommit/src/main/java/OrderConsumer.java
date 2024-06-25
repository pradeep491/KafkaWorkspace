import com.test.kafka.customdeserializer.Order;
import com.test.kafka.customdeserializer.OrderDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");

        //props.setProperty("enable.auto.commit", "false");
        //props.setProperty("auto.commit.interval.ms", "200");
        props.setProperty("auto.commit.offset", "false");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderPartitionedTopic"));
        try {
            ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(20));

            while (true) {
                for (ConsumerRecord<String, Order> record : records) {
                    String customerName = record.key();
                    Order order = record.value();
                    System.out.println("Customer name:" + customerName);
                    System.out.println("product:" + order.getProduct());
                    System.out.println("quantity:" + order.getQuantity());
                    System.out.println("partition: " + record.partition());
                }

                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                        if(e != null){
                            System.out.println("commit failed for offset"+offsets);
                        }
                    }
                });
            }
        } finally {
            consumer.close();
        }
    }
}
