package org.zenoss.app.consumer.metric.remote;

import com.google.common.base.Strings;
import org.zenoss.app.consumer.metric.data.Metric;

import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;
import java.util.List;

/**
 * Shared utilities for Metric WebSockets and WebResources.
 */
public final class Utils {

    /**
     * Find and inject all parameters in the servlet request matching the provided prefix into each metric.
     *
     * @param request      The http servlet request
     * @param metrics      The metrics to tag
     * @param tagsPrefixes The prefixes to find int he servlet request
     */
    public static void tagMetrics(HttpServletRequest request, List<Metric> metrics, List<String> tagsPrefixes) {
        if (tagsPrefixes == null || tagsPrefixes.isEmpty()) {
            return;
        }

        Enumeration<String> parameters = request.getParameterNames();
        while (parameters.hasMoreElements()) {
            String parameter = parameters.nextElement();
            for (String prefix : tagsPrefixes) {
                if (parameter.startsWith(prefix)) {
                    injectTag(parameter, request.getParameter(parameter), metrics);
                }
            }
        }
    }

    /**
     * Add tag into each metric.  No tag's added if the value's empty.
     *
     * @param name    tag name
     * @param value   tag value
     * @param metrics the metrics to tag
     */
    public static void injectTag(String name, String value, List<Metric> metrics) {
        if (!Strings.isNullOrEmpty(value)) {
            for (Metric metric : metrics) {
                metric.addTag(name, value);
            }
        }
    }
}
