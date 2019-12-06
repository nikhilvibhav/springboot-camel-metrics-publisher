package com.capgemini.camel.metrics.publisher.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.bealetech.metrics.reporting.Statsd;
import com.bealetech.metrics.reporting.StatsdReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jetty9.InstrumentedQueuedThreadPool;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.contrib.codahalemetricspublisher.HystrixCodaHaleMetricsPublisher;
import com.netflix.hystrix.contrib.metrics.eventstream.HystrixMetricsStreamServlet;
import com.netflix.hystrix.strategy.HystrixPlugins;

import com.capgemini.camel.metrics.publisher.CamelMetricsUpdater;
import com.capgemini.camel.metrics.publisher.TomcatMetricsPublisher;
import com.capgemini.codahale.metrics.filter.ConfigurableCodaHaleMetricFilter;
import com.capgemini.codahale.metrics.filter.HttpResponseCodeMetricsFilter;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Shared configuration class to initialise the Codehale Metrics Registry, create and
 * register Metrics Publishers, initialise StatsD/Graphite reporting and create metrics
 * servlets
 *
 * @author Simon Irving
 */
@Configuration
public class MetricsServletConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsServletConfiguration.class);

    @Value("${statsd.host:127.0.0.1}")
    private String metricsReporterHost;

    @Value("${statsd.port:8125}")
    private int metricsReporterPort;

    @Value("${statsd.pollingPeriodInSeconds:5}")
    private int pollingPeriodInSeconds;

    @Value("${monitoring.servlets.path:/monitoring}")
    private String monitoringServletsPath;

    @Value("${metrics.prefix:/metrics}")
    private String metricsPrefix;

    @Value("${response.code.metrics.filter.url:/api/v1}")
    private String responseCodeMetricsFilterUrl;

    /**
     * Register the required attributes in the Servlet Context
     *
     * @return ServletContextListener A ServletContextListener which will register/deregister the required attributes
     * in the ServletContext at startup/shutdown respectively
     */
    @Bean
    protected ServletContextListener listener() {
        return new ServletContextListener() {
            @Override
            public void contextInitialized(final ServletContextEvent sce) {
                final MetricRegistry metricRegistry = codahaleMetricsRegistry();
                sce.getServletContext().setAttribute(MetricsServlet.METRICS_REGISTRY, metricRegistry);
                sce.getServletContext().setAttribute(HttpResponseCodeMetricsFilter.REGISTRY_ATTRIBUTE, metricRegistry);
                LOGGER.info("ServletContext initialized");
            }

            @Override
            public void contextDestroyed(final ServletContextEvent sce) {
                sce.getServletContext().removeAttribute(MetricsServlet.METRICS_REGISTRY);
                sce.getServletContext().removeAttribute(HttpResponseCodeMetricsFilter.REGISTRY_ATTRIBUTE);
                LOGGER.info("ServletContext destroyed");
            }
        };
    }

    /**
     * Setup the Codahale MetricRegistry bean and the Statsd/Graphite reporter
     *
     * @return the Codahale MetricRegistry bean
     */
    @Bean(name = "metricRegistry")
    public MetricRegistry codahaleMetricsRegistry() {
        final MetricRegistry registry = new MetricRegistry();
        registerJvmMetrics(registry);
        LOGGER.info("Codahale MetricRegistry created");

        // plug in the Hystrix Metrics to Codahale
        final HystrixCodaHaleMetricsPublisher hystrixCodaHaleMetricsPublisher = new HystrixCodaHaleMetricsPublisher(registry);
        HystrixPlugins.getInstance().registerMetricsPublisher(hystrixCodaHaleMetricsPublisher);

        LOGGER.info("Codahale Statsd Reporter for MetricRegistry started");
        return registry;
    }

    /**
     * Setup the Camel metrics updater bean
     *
     * @return the {@link CamelMetricsUpdater} bean
     */
    @Bean(name = "camelMetricsUpdater")
    public CamelMetricsUpdater camelMetricsRegistry(final MetricRegistry metricRegistry) {
        final CamelMetricsUpdater updater = new CamelMetricsUpdater(metricRegistry);
        LOGGER.info("CamelMetricsUpdater created");

        return updater;
    }

    /**
     * Initialise the embedded servlet container metrics reporting.
     *
     * @return An ApplicationListener which will register a MetricsPublisher when the Servlet container is
     * initialised
     */
    @Bean
    public ApplicationListener<WebServerInitializedEvent> webServerMetricsInitializer() {

        final MetricRegistry registry = codahaleMetricsRegistry();

        return webServerInitializedEvent -> {
            final WebServer webServer = webServerInitializedEvent.getWebServer();

            if (webServer == null) {
                LOGGER.error("The EmbeddedServletContainer obtained from the EmbeddedServletContainerInitializedEvent was null; metrics will not be published");
                return;
            }

            final int serverPort = webServer.getPort();

            LOGGER.info("Initialising MetricsPublisher on port {}", serverPort);

            TomcatMetricsPublisher.forRegistry(registry)
                .serverPortIs(serverPort)
                .start();

            // register the graphite reporter
            final Statsd statsd = new Statsd(metricsReporterHost, metricsReporterPort);
            final StatsdReporter statsdReporter = StatsdReporter.forRegistry(registry)
                .prefixedWith(metricsPrefix + serverPort)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .filter(new ConfigurableCodaHaleMetricFilter(DynamicPropertyFactory.getInstance()))
                .build(statsd);
            statsdReporter.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
        };
    }

    /**
     * Registers HystrixMetricsStream Servlet that handles Hystrix Metrics
     *
     * @return ServletRegistrationBean
     */
    @Bean
    public ServletRegistrationBean<HystrixMetricsStreamServlet> hystrixServletRegistrationBean() {
        final HystrixMetricsStreamServlet hystrixServlet = new HystrixMetricsStreamServlet();
        final ServletRegistrationBean<HystrixMetricsStreamServlet> servletRegistrationBean =
            new ServletRegistrationBean<>(hystrixServlet, monitoringServletsPath + "/hystrix.stream");
        LOGGER.info("HystrixMetricsStreamServlet registered");
        return servletRegistrationBean;
    }

    /**
     * Registers Codahale MetricsServlet that handles all the bespoke metrics.
     *
     * @return ServletRegistrationBean
     */
    @Bean
    public ServletRegistrationBean<MetricsServlet> codahaleMetricsServletRegistrationBean() {
        final MetricsServlet metricsServlet = new MetricsServlet();
        final ServletRegistrationBean<MetricsServlet> servletRegistrationBean =
            new ServletRegistrationBean<>(metricsServlet, monitoringServletsPath + "/codahale.metrics");
        LOGGER.info("Codahale MetricsServlet registered");
        return servletRegistrationBean;
    }

    /**
     * Registers an instrumented Servlet filter which has meters for status
     * codes, a counter for the number of active requests, and a timer for
     * request duration.
     *
     * @return FilterRegistrationBean
     * @see this https://metrics.dropwizard.io/4.1.1/manual/servlet.html
     */
    @Bean
    public FilterRegistrationBean<HttpResponseCodeMetricsFilter> responseCodeMetricsFilterRegistrationBean() {
        final FilterRegistrationBean<HttpResponseCodeMetricsFilter> filterRegistrationBean = new FilterRegistrationBean<>();
        final HttpResponseCodeMetricsFilter responseCodeMetricsFilter = new HttpResponseCodeMetricsFilter();
        filterRegistrationBean.setFilter(responseCodeMetricsFilter);

        final List<String> urlPatterns = new ArrayList<>();
        urlPatterns.add(responseCodeMetricsFilterUrl + "/*");
        filterRegistrationBean.setUrlPatterns(urlPatterns);

        LOGGER.info("HttpResponseCodeMetricsFilter bean registered");
        return filterRegistrationBean;
    }

    /**
     * Registers {@link com.codahale.metrics.servlets.ThreadDumpServlet} with the application. This provides
     * an endpoint to capture JVM thread dump
     *
     * @return
     */
    @Bean
    public ServletRegistrationBean<ThreadDumpServlet> threadDumpServletRegistrationBean() {
        final ThreadDumpServlet threadDumpServlet = new ThreadDumpServlet();
        final ServletRegistrationBean<ThreadDumpServlet> servletRegistrationBean = new ServletRegistrationBean<>
            (threadDumpServlet, monitoringServletsPath + "/threads");
        LOGGER.info("Codahale ThreadDumpServlet registered");
        return servletRegistrationBean;
    }

    private void registerJvmMetrics(MetricRegistry metricRegistry) {
        metricRegistry.register(name("jvm", "gc"), new GarbageCollectorMetricSet());
        metricRegistry.register(name("jvm", "memory"), new MemoryUsageGaugeSet());
        metricRegistry.register(name("jvm", "thread-states"), new ThreadStatesGaugeSet());
        //TODO: Add the remaining metric sets that are not available in the current version
    }
}
