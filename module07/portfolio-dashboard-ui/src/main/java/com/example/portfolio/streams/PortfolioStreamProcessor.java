package com.example.portfolio.streams;

import com.example.portfolio.model.TradeEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

@Component
@EnableKafkaStreams
public class PortfolioStreamProcessor {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public KStream<String, String> processTrades(StreamsBuilder streamsBuilder) {

        System.out.println("KAFKA STREAM STARTED");

        KStream<String, String> tradesStream =
                streamsBuilder.stream(
                        "portfolio-trades-topic",
                        Consumed.with(
                                Serdes.String(),
                                Serdes.String()
                        )
                );

        tradesStream.peek((key, value) -> {
            System.out.println("INPUT RECEIVED");
            System.out.println("KEY = " + key);
            System.out.println("VALUE = " + value);
        });

        KTable<String, Integer> portfolioTable =
                tradesStream
                        .groupByKey(
                                Grouped.with(
                                        Serdes.String(),
                                        Serdes.String()
                                )
                        )
                        .aggregate(
                                () -> 0,
                                (stockSymbol, tradeJson, currentTotal) -> {

                                    try {

                                        TradeEvent tradeEvent =
                                                objectMapper.readValue(
                                                        tradeJson,
                                                        TradeEvent.class);

                                        int newTotal =
                                                currentTotal + tradeEvent.getQuantity();

                                        System.out.println(
                                                stockSymbol +
                                                        " Total = " +
                                                        newTotal);

                                        return newTotal;

                                    } catch (Exception e) {

                                        e.printStackTrace();

                                        return currentTotal;
                                    }
                                },
                                Materialized.with(
                                        Serdes.String(),
                                        Serdes.Integer()
                                )
                        );

        portfolioTable
                .toStream()
                .peek((key, value) -> {

                    System.out.println("OUTPUT PRODUCED");

                    System.out.println(
                            key + " -> " + value);

                })
                .mapValues(String::valueOf)
                .to(
                        "portfolio-summary-topic",
                        Produced.with(
                                Serdes.String(),
                                Serdes.String()
                        )
                );

        return tradesStream;
    }
}