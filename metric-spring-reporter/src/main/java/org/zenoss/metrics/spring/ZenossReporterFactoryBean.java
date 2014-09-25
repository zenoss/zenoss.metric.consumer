package org.zenoss.metrics.spring;

import com.codahale.metrics.MetricFilter;
import com.ryantenney.metrics.spring.reporter.AbstractScheduledReporterFactoryBean;
import org.zenoss.metrics.reporter.HttpPoster;
import org.zenoss.metrics.reporter.HttpPoster.Builder;
import org.zenoss.metrics.v3reporter.ZenossMetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ZenossReporterFactoryBean extends AbstractScheduledReporterFactoryBean<ZenossMetricsReporter> {

    // Required
    public static final String PERIOD = "period";

    //optional
    public static final String POSTURL = "post-url";
    public static final String CLOCK_REF = "clock-ref";
    public static final String PREFIX = "prefix";
    public static final String DURATION_UNIT = "duration-unit";
    public static final String RATE_UNIT = "rate-unit";
    public static final String FILTER_PATTERN = "filter";
    public static final String FILTER_REF = "filter-ref";
    public static final String PASSWORD = "password";
    public static final String USERNAME = "username";
    public static final String TAGS = "tags";

    private static final Logger LOG = LoggerFactory.getLogger(ZenossReporterFactoryBean.class);

    @Override

    protected long getPeriod() {
        return convertDurationString(getProperty(PERIOD));
    }

    @Override
    public Class<ZenossMetricsReporter> getObjectType() {
        return ZenossMetricsReporter.class;
    }

    @Override
    protected ZenossMetricsReporter createInstance() throws Exception {
        final ZenossMetricsReporter.Builder reporter = ZenossMetricsReporter.forRegistry(getMetricRegistry());

        if (hasProperty(PREFIX)) {
            reporter.prefixedWith(getProperty(PREFIX));
        }

        if (hasProperty(DURATION_UNIT)) {
            reporter.convertDurationsTo(getProperty(DURATION_UNIT, TimeUnit.class));
        }

        if (hasProperty(RATE_UNIT)) {
            reporter.convertRatesTo(getProperty(RATE_UNIT, TimeUnit.class));
        }

        if (hasProperty(FILTER_PATTERN)) {
            reporter.filter(metricFilterPattern(getProperty(FILTER_PATTERN)));
        } else if (hasProperty(FILTER_REF)) {
            reporter.filter(getPropertyRef(FILTER_REF, MetricFilter.class));
        }

        if (hasProperty(TAGS)) {
            String prop = getProperty(TAGS);
	    LOG.info("setting metric tags: {}", prop);
            String[] entries = prop.split(",");
            Map<String, String> tags = new HashMap<>();
            for (String entry : entries) {
                String[] keyval = entry.split(":");
                tags.put(keyval[0].trim(), keyval[1].trim());
            }
            reporter.setTags(tags);
        }

        String url = getProperty(POSTURL, "http://localhost:8080" + HttpPoster.METRIC_API);

	LOG.info("setting metric url: {}", url);
        HttpPoster.Builder poster = new Builder(new java.net.URL(url));
        if (hasProperty(USERNAME)) {
	    LOG.info("setting metric user: {}", getProperty(USERNAME));
            poster.setUsername(getProperty(USERNAME));
        }

	if (hasProperty(PASSWORD)) {
            LOG.info("setting metric password: ********");
            poster.setPassword(getProperty(PASSWORD));
        }

        return reporter.build(poster.build());
    }
}
