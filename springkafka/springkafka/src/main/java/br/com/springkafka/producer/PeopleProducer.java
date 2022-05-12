package br.com.springkafka.producer;

import br.com.springkafka.People;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PeopleProducer {

    private final String topic;
    private final KafkaTemplate<String, People> kafkaTemplate;

    public PeopleProducer(@Value("${topic.name}") String topic, KafkaTemplate<String, People> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(People people){
        kafkaTemplate.send(topic, people).addCallback(
                success -> log.info("Mensagem enviada com sucesso!"),
                failure -> log.error("Falha ao enviar mensagem!")
        );
    }
}
