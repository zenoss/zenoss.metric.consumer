package org.zenoss.app.consumer.metric.remote;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Metric;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

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

    /**
     * Filter tags in each metric using a white list. No white listing performed when
     * the list's null.  An empty white list removes all tags.
     *
     * @param metrics the metrics to tag
     * @param whiteList tags to white list
     */
    public static void filterMetricTags(List<Metric> metrics, List<String> whiteList) {
        if (whiteList != null) {
            for ( Metric m : metrics) {
                Map<String, String> tags = filterTags( m.getTags(), whiteList);
                m.setTags( tags);
            }
        }
    }

    /**
     * Create a new tag map from an existing tag map using a white list to filter tags.  When
     * white list is null the old tags are the new tags.
     *
     * @param tags the tags to filter
     * @param whiteList tags to white list
     */
    public static Map<String, String> filterTags(  Map<String,String> tags, List<String> whiteList) {
        if (whiteList != null) {
            final Map<String, String> newTags = Maps.newHashMap();
            for( Map.Entry<String, String> entry : tags.entrySet()) {
                final String key = entry.getKey();
                if (whiteList.contains( key)) {
                    newTags.put( key, entry.getValue());
                }
            }
            return newTags;
        }
        return tags;
    }
}
