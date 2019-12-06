package com.capgemini.camel.metrics.publisher;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.impl.event.*;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.support.EventNotifierSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.codahale.metrics.MetricRegistry;

import static org.apache.commons.lang.StringUtils.isBlank;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Class to capture and output Route level metrics to Codahale.
 * <p>
 * Based on class from https://gist.github.com/aldrinleal/8262171
 */
@Component("camelMetricsUpdater")
public class CamelMetricsUpdater extends EventNotifierSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(CamelMetricsUpdater.class);

    private final MetricRegistry metrics;

    public CamelMetricsUpdater(@Autowired final MetricRegistry metrics) {
        this.metrics = metrics;
    }

    protected void onExchangeCompletedEvent(AbstractExchangeEvent event, String metricPrefix) {
        final long duration = ChronoUnit.MILLIS.between(event
                .getExchange()
                .getProperty(Exchange.CREATED_TIMESTAMP, Date.class)
                .toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalTime(),
            Instant.ofEpochMilli(System.currentTimeMillis())
                .atZone(ZoneId.systemDefault())
                .toLocalTime());
        metrics.timer(name(event.getClass(), metricPrefix)).update(duration, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isEnabled(CamelEvent event) {
        return true;
    }

    @Override
    protected void doStart() throws Exception {
        setIgnoreCamelContextEvents(true);
        setIgnoreExchangeEvents(false);
        setIgnoreExchangeCreatedEvent(true);
        setIgnoreExchangeRedeliveryEvents(true);
        setIgnoreExchangeSendingEvents(true);
        setIgnoreExchangeSentEvents(true);
        setIgnoreRouteEvents(true);
        setIgnoreServiceEvents(true);
    }

    @Override public void notify(CamelEvent event) throws Exception {
        boolean covered = false;

        if (event instanceof AbstractExchangeEvent) {
            final AbstractExchangeEvent ev = (AbstractExchangeEvent) event;

            final Exchange exchange = ev.getExchange();
            final String metricPrefix = exchange.getFromRouteId();

            // if we can't find the prefix for the metrics then don't capture any
            if (isBlank(metricPrefix)) {
                return;
            }

            if (ev instanceof ExchangeCompletedEvent || ev instanceof ExchangeFailedEvent || ev instanceof ExchangeRedeliveryEvent) {
                onExchangeCompletedEvent(ev, metricPrefix);
                covered = true;
            } else {
                metrics.meter(name(event.getClass(), metricPrefix)).mark();
            }
        }

        if (!covered) {
            LOGGER.debug("Not covered: Type {} ({})", event.getClass(), event);
        }
    }
}
