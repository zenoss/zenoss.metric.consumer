/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss under the directory where your Zenoss product is installed.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.remote;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.*;
/**
 *
 * @author cschellenger
 */
public class TaskIntParserTest {
 
    /*
     * Verify that if the parameters are empty, the default value is used.
     */
    @Test
    public void testParseNoValues() {
        TaskIntParser parser = new TaskIntParser();
        ImmutableCollection<String> emptyList = ImmutableList.of();
        assertEquals(1, parser.parse(emptyList, 1));
        assertEquals(-1, parser.parse(emptyList, -1));
    }
    
    /*
     * Verify that if a single value is provided, that value is parsed 
     * and returned.
     */
    @Test
    public void testParseOneValues() {
        TaskIntParser parser = new TaskIntParser();
        ImmutableCollection<String> singletonList = ImmutableList.of("123");
        assertEquals(123, parser.parse(singletonList, 1));
        assertEquals(123, parser.parse(singletonList, -1));
    }
    
    /*
     * Verify that if multiple values are provided, the first one is parsed 
     * and returned.
     */
    @Test
    public void testParseMultipleValues() {
        TaskIntParser parser = new TaskIntParser();
        ImmutableCollection<String> multiList = ImmutableList.of("234","456");
        assertEquals(234, parser.parse(multiList, 1));
        assertEquals(234, parser.parse(multiList, -1));
    }
}
