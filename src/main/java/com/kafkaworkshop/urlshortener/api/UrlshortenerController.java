package com.kafkaworkshop.urlshortener.api;

import com.kafkaworkshop.urlshortener.kafka.IdentifierFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UrlshortenerController {

    private static final Logger log = LoggerFactory.getLogger(UrlshortenerController.class);

    private final KafkaOperations<String, String> kafkaOperations;
    private final String compactedTopic;
    private final IdentifierFactory identifierFactory;
    private final KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService;

    public UrlshortenerController(KafkaOperations<String, String> kafkaOperations, String compactedTopic, IdentifierFactory identifierFactory, KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService) {
        this.kafkaOperations = kafkaOperations;
        this.compactedTopic = compactedTopic;
        this.identifierFactory = identifierFactory;
        this.kafkaStreamsInteractiveQueryService = kafkaStreamsInteractiveQueryService;
    }

    record GetLongUrlResponse() {

    }

    @GetMapping
    GetLongUrlResponse getLongUrlResponse() {
        kafkaStreamsInteractiveQueryService.getKafkaStreamsApplicationHostInfo()
    }


    record PostShortUrlResponse(String shortUrl) {

    }

    record PostShortUrlRequest(String longUrl) {

    }

    @PostMapping
    ResponseEntity<PostShortUrlResponse> postShortUrl(PostShortUrlRequest postShortUrlRequest) {
        try {
            String shortIdentifier = identifierFactory.createShortIdentifier();
            SendResult<String, String> stringStringSendResult = kafkaOperations.send(compactedTopic, shortIdentifier, postShortUrlRequest.longUrl).get();
            log.info("Created short url");
            return ResponseEntity.created(null).body(new PostShortUrlResponse(null));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
