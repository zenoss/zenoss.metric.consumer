package org.zenoss.metrics.reporter;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

import junit.framework.Assert;
import org.apache.http.client.HttpResponseException;
import org.junit.Rule;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.metrics.reporter.HttpPoster.Builder;

import java.io.IOException;

public class HttpPosterTest {

    private static final String URL_PATH = "/test/path";
    private static final int MOCK_PORT = 9843;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(MOCK_PORT);

    @Test
    public void simplePost() throws IOException {
        stubFor(post(urlEqualTo(URL_PATH))
                .withHeader("Accept", matching("application/json.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("<response>Some content</response>")));

        HttpPoster poster = new Builder("localhost", MOCK_PORT, false)
                .setApi(URL_PATH)
                .build();
        poster.start();

        MetricBatch batch = new MetricBatch(8);
        batch.addMetric(new Metric("mname", 8, 9999));

        poster.post(batch);
        poster.shutdown();
        verify(postRequestedFor(urlEqualTo(URL_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"metrics\":[{\"metric\":\"mname\",\"timestamp\":8,\"value\":9999.0,\"tags\":{}}]}"))
        );
    }

    @Test
    public void authPost() throws IOException {
        stubFor(post(urlEqualTo(URL_PATH))
                .withHeader("Accept", matching("application/json.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/xml")
                        .withHeader("Set-Cookie", "cookieName=cookieValue")
                        .withBody("<response>Some content</response>")));

        HttpPoster poster = new Builder("localhost", MOCK_PORT, false)
                .setApi(URL_PATH)
                .setUsername("test_user")
                .setPassword("test_pass")
                .build();
        poster.start();

        MetricBatch batch = new MetricBatch(8);
        batch.addMetric(new Metric("mname", 8, 9999));

        poster.post(batch);

        verify(postRequestedFor(urlEqualTo(URL_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withHeader("Authorization", equalTo("Basic dGVzdF91c2VyOnRlc3RfcGFzcw=="))
                .withRequestBody(equalTo("{\"metrics\":[{\"metric\":\"mname\",\"timestamp\":8,\"value\":9999.0,\"tags\":{}}]}"))
        );

        //verify cookie and auth header resent and basic auth not sent
        poster.post(batch);

        verify(postRequestedFor(urlEqualTo(URL_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withHeader("Cookie", equalTo("cookieName=cookieValue"))
                .withoutHeader("Authorization")
                .withRequestBody(equalTo("{\"metrics\":[{\"metric\":\"mname\",\"timestamp\":8,\"value\":9999.0,\"tags\":{}}]}"))
        );

        //send 401 to verify re auth
        stubFor(post(urlEqualTo(URL_PATH))
                .willReturn(aResponse()
                        .withStatus(401)));

        try {
            poster.post(batch);
            Assert.fail("expected unauthorized");
        } catch (HttpResponseException e) {
            Assert.assertEquals(e.getMessage(), "Unauthorized");
        }
        //setup for reauthentication with different cookie
        stubFor(post(urlEqualTo(URL_PATH))
                .withHeader("Accept", matching("application/json.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/xml")
                        .withHeader("Set-Cookie", "cookieName=newCookie")
                        .withBody("<response>Some content</response>")));
        poster.post(batch);

        //verify auth sent and no cookies
        verify(postRequestedFor(urlEqualTo(URL_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withHeader("Authorization", equalTo("Basic dGVzdF91c2VyOnRlc3RfcGFzcw=="))
                .withoutHeader("Cookie")
                .withRequestBody(equalTo("{\"metrics\":[{\"metric\":\"mname\",\"timestamp\":8,\"value\":9999.0,\"tags\":{}}]}"))
        );

        //verify no auth and new cookie sent
        poster.post(batch);

        verify(postRequestedFor(urlEqualTo(URL_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withHeader("Cookie", notMatching("cookieName=cookieValue"))
                .withHeader("Cookie", equalTo("cookieName=newCookie"))
                .withoutHeader("Authorization")
                .withRequestBody(equalTo("{\"metrics\":[{\"metric\":\"mname\",\"timestamp\":8,\"value\":9999.0,\"tags\":{}}]}"))
        );
        poster.shutdown();


    }
}
