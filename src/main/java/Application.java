import producer.WikimediaChangesProducer;

public class Application {
    public static void main(String[] args) throws InterruptedException {
        WikimediaChangesProducer producer = new WikimediaChangesProducer();
        producer.processKafkaMessagesAsProducer();
    }
}
