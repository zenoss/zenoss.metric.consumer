package org.zenoss.app.consumer.metric.remote;

import com.google.common.base.Strings;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Metric;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;

/**
 * Shared utilities for Metric WebSockets and WebResources.
 */
public final class Utils {

    /**
     * Best guess at the IP address of the remote end of the request.
     */
    public static String remoteAddress(HttpServletRequest request) {
        String forwardedFor = request.getHeader("X-Forwarded-For");
        return (forwardedFor != null) ? forwardedFor : request.getRemoteAddr();
    }

    /**
     * Find and inject all parameters in the servlet request matching the provided prefix into each metric.
     *
     * @param request      The http servlet request
     * @param metrics      The metrics to tag
     * @param tagPrefixes The prefixes to find int he servlet request
     */
    public static void tagMetrics(HttpServletRequest request, List<Metric> metrics, List<String> tagPrefixes) {
        if (tagPrefixes == null || tagPrefixes.isEmpty()) {
            return;
        }

        Enumeration<String> parameters = request.getParameterNames();
        while (parameters.hasMoreElements()) {
            String parameter = parameters.nextElement();
            for (String prefix : tagPrefixes) {
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
    public static void injectTag(String name, String value, Collection<Metric> metrics) {
        if (!Strings.isNullOrEmpty(value)) {
            for (Metric metric : metrics) {
                metric.addTag(name, value);
            }
        }
    }
}
