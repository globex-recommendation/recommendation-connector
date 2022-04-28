package org.globex.retail;

import java.util.List;

import io.vertx.core.json.JsonArray;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;

public class Routes extends RouteBuilder {


    @Override
    public void configure() throws Exception {
        from("kafka:{{kafka.product-scores.topic.name}}?groupId={{kafka.product-scores.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2PostgresqlProductScores")
                .unmarshal().json()
                .log(LoggingLevel.DEBUG, "Product recommendation event received: ${body}")
                .process(exchange -> {
                    Message in = exchange.getIn();
                    String category = in.getHeader(KafkaConstants.KEY, String.class);
                    JsonArray json = new JsonArray(in.getBody(List.class));
                    in.setBody(new StringBuilder()
                            .append("UPDATE featured_products SET ")
                            .append("items = '").append(json.encode()).append("' ")
                            .append("WHERE category = '").append(category).append("'"));
                })
                .log(LoggingLevel.DEBUG, "SQL statement: ${body}")
                .to("jdbc:catalog");
    }
}
