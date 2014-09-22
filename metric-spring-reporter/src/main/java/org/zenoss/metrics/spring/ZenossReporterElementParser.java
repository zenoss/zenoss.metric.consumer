package org.zenoss.metrics.spring;

import com.ryantenney.metrics.spring.reporter.AbstractReporterElementParser;

import static org.zenoss.metrics.spring.ZenossReporterFactoryBean.*;

public class ZenossReporterElementParser extends AbstractReporterElementParser {
    @Override
    public String getType() {
        return "zenoss";
    }

    @Override
    protected Class<?> getBeanClass() {
        return ZenossReporterFactoryBean.class;
    }

    @Override
    protected void validate(ValidationContext c) {
        c.require(PERIOD, DURATION_STRING_REGEX, "Period is required and must be in the form '\\d+(ns|us|ms|s|m|h|d)'");
        c.optional(POSTURL);
        c.optional(PREFIX);
        c.optional(CLOCK_REF);
        c.optional(RATE_UNIT, TIMEUNIT_STRING_REGEX, "Rate unit must be one of the enum constants from java.util.concurrent.TimeUnit");
        c.optional(DURATION_UNIT, TIMEUNIT_STRING_REGEX, "Duration unit must be one of the enum constants from java.util.concurrent.TimeUnit");
        c.optional(FILTER_PATTERN);
        c.optional(FILTER_REF);
        if (c.has(FILTER_PATTERN) && c.has(FILTER_REF)) {
            c.reject(FILTER_REF, "Reporter element not specify both the 'filter' and 'filter-ref' attributes");
        }

        c.rejectUnmatchedProperties();
    }

}
