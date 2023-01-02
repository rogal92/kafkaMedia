package producer;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    private static final String TOPIC_NAME = "wikimedia.recentchange";
    private static final String MSG_SOURCE_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    private static final String SERVER = "localhost:9092";
    private final EventSource eventSource;

    public WikimediaChangesProducer() {
        var handler = new WikiMediaChangeHandler(new KafkaProducer<>(getProperties()), TOPIC_NAME);
        eventSource = new EventSource.Builder(handler, URI.create(MSG_SOURCE_URL)).build();
    }

    public void processKafkaMessagesAsProducer() throws InterruptedException {
        eventSource.start();
        TimeUnit.MINUTES.sleep(10);
    }

    @NotNull
    private Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
