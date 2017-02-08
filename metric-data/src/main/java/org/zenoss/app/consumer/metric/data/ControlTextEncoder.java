/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2017, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */

package org.zenoss.app.consumer.metric.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.websocket.Encoder;
import javax.websocket.EndpointConfig;

public class ControlTextEncoder implements Encoder.Text<Control>{
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(EndpointConfig ec) { }

    @Override
    public void destroy() { }

    @Override
    public String encode(Control control) {
        String retval;
        try {
            retval = objectMapper.writeValueAsString(control);
        } catch (JsonProcessingException e) {
            retval = "{}";
        }
        return retval;
    }
}
