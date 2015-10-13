/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.remote;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Metric;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

/**
 * Shared utilities for Metric WebSockets and WebResources.
 */
public final class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * Best guess at the IP address of the remote end of the request.
     */
    public static String remoteAddress(HttpServletRequest request) {
        String forwardedFor = request.getHeader("X-Forwarded-For");
        if (forwardedFor != null) {
            log.debug("X-Forwarded-For: {}", forwardedFor);
            return forwardedFor;
        }
        String xZAuthToken = request.getHeader("X-ZAuth-Token");
        if (xZAuthToken != null) {
            log.debug("X-ZAuth-Token: {}", xZAuthToken);
            return xZAuthToken;
        }
        return request.getRemoteAddr();
    }

    /**
     * Find and inject all parameters in the servlet request matching the provided prefix into each metric.
     *
     * @param request     The http servlet request
     * @param metrics     The metrics to tag
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
     * Filter tags in each metric using a white list of specific tags, and a white list of tag prefixes.  Tags matching
     * the explicit whitelist have priority, and are placed in the resulting map first.
     *
     * If both white lists are null, all tags are preserved.  If one white list is null, then it is ignored.
     *
     * @param metrics           the metrics to tag
     * @param whiteList         tags to white list
     * @param whiteListPrefixes tag prefixes to white list
     */
    public static void filterMetricTags(List<Metric> metrics, List<String> whiteList, List<String> whiteListPrefixes) {
        if (!(whiteList == null && whiteListPrefixes == null)) {
            for (Metric m : metrics) {
                Map<String, String> tags = filterTags(m.getTags(), whiteList, whiteListPrefixes);
                m.setTags(tags);
            }
        }
    }

    /**
     * Create a new tag map from an existing tag map using an explicit white list, and a white list prefix list.
     * If both white lists are null, the old tags are the new tags.  If one white list is null, then it is ignored.
     *
     * The current implementation returns an ordered Map with the explicit white list matches appearing first in the map.
     *
     * @param tags              the tags to filter
     * @param whiteList         tags to white list
     * @param whiteListPrefixes tag prefixes to white list
     */
    public static Map<String, String> filterTags(Map<String, String> tags, List<String> whiteList, List<String> whiteListPrefixes) {
        // return original tags if white lists are null
        if (whiteList == null && whiteListPrefixes == null) {
            return tags;
        }

        final Map<String, String> tagsCopy = Maps.newHashMap(tags);
        final Map<String, String> newTags = Maps.newLinkedHashMap();
        String key;

        // First add explicitly white listed tags
        if (whiteList != null) {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                key = entry.getKey();
                if (whiteList.contains(key)) {
                    newTags.put(key, entry.getValue());
                    tagsCopy.remove(key);
                }
            }
        }
        // Now loop through again and check remaining tags against the prefix white list.  Only look @ the copy
        // of the tags at this point, in case any were removed by the explicit white list.
        if (whiteListPrefixes != null) {
            for (Map.Entry<String, String> entry : tagsCopy.entrySet()) {
                key = entry.getKey();
                for (String prefix : whiteListPrefixes) {
                    if (key.startsWith(prefix)) {
                        newTags.put(key, entry.getValue());
                        break;
                    }
                }
            }

        }
        return newTags;
    }

}
