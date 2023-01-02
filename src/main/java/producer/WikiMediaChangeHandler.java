package producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangeHandler implements EventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(WikiMediaChangeHandler.class.getSimpleName());
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    public WikiMediaChangeHandler(final KafkaProducer<String, String> kafkaProducer, final String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {}

    @Override
    public void onClosed() {
        LOGGER.info("Closing Kafka Producer...");
        kafkaProducer.close();
    }

    @Override
    public void onMessage(final String s, final MessageEvent messageEvent) throws Exception {
        String data = messageEvent.getData();
        LOGGER.info("Sending: {}", data);
        kafkaProducer.send(new ProducerRecord<>(topic, data));
    }

    @Override
    public void onComment(final String s) throws Exception {

    }

    @Override
    public void onError(final Throwable throwable) {
        LOGGER.error("Error in stream reading: {}", throwable.getMessage());
    }
}
