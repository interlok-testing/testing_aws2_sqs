package com.adaptris.elastic.test;

import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.CustomEndpoint;
import com.adaptris.aws2.sqs.AsyncSQSClientFactory;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.AdaptrisMessageImp;
import com.adaptris.core.management.BootstrapProperties;
import com.adaptris.core.varsub.VariableSubstitutionPreProcessor;
import com.adaptris.interlok.resolver.Resolver;
import com.adaptris.testing.DockerComposeFunctionalTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import java.io.File;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Properties;

import static io.restassured.RestAssured.*;

public class DefaultFunctionalTest extends DockerComposeFunctionalTest {
    protected static String INTERLOK_SERVICE_NAME = "interlok-1";
    protected static String LOCALSTACK_SERVICE_NAME = "localstack-1";
    protected static int INTERLOK_PORT = 8081;
    protected static int LOCALSTACK_PORT = 4566;
    protected static WaitStrategy defaultWaitStrategy = Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90));

    protected String awsRegion;
    protected String queue1Url;
    protected String queue2Url;

    @Override
    protected void customiseVariablesIfExists(Properties props) {
        var varsub = new VariableSubstitutionPreProcessor(new BootstrapProperties(resolveBootstrapLocation()));
        varsub.setProperties(bootstrapProperties);
        try {
            awsRegion = varsub.process(props.getProperty("aws2.account.region"));
            queue1Url = varsub.process(props.getProperty("aws2.sqs.queue.1.url"));
            queue2Url = varsub.process(props.getProperty("aws2.sqs.queue.2.url"));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected ComposeContainer setupContainers() {
        return new ComposeContainer(new File("docker-compose.yaml"))
                .withExposedService(INTERLOK_SERVICE_NAME, INTERLOK_PORT, defaultWaitStrategy)
                .withExposedService(LOCALSTACK_SERVICE_NAME, LOCALSTACK_PORT, defaultWaitStrategy);
    }

    protected String getSqsEndpoint(String path) {
        InetSocketAddress address = getHostAddressForService(LOCALSTACK_SERVICE_NAME, LOCALSTACK_PORT);
        if (!path.startsWith("/")) path = "/" + path;
        return "http://" + address.getHostString() + ":" + address.getPort() + path;
    }

    protected String getInterlokApiEndpoint(String path) {
        InetSocketAddress address = getHostAddressForService(INTERLOK_SERVICE_NAME, INTERLOK_PORT);
        if (!path.startsWith("/")) path = "/" + path;
        return "http://" + address.getHostString() + ":" + address.getPort() + path;}



    @Test
    public void test_sqs_split() throws Exception {
        // index json document
        given().body("""
            <messages>
                <message>this</message>
                <message>is</message>
                <message>a</message>
                <message>sentence</message>
            </messages>""")
        .when().get(getInterlokApiEndpoint(String.format("/aws2/sqs")))
                .then().statusCode(200);

        // poller interval is set to 5 seconds
        Thread.sleep(5000);

        try (var sdk = buildSdkClient()) {
            // if there is at least 1 message in queue2, then everything is working
            var approxMessagesQueue2 = Integer.parseInt(sdk.getQueueAttributes(GetQueueAttributesRequest.builder().queueUrl(queue2Url).attributeNames(QueueAttributeName.ALL).build()).get().attributesAsStrings().get("ApproximateNumberOfMessages"));
            Assertions.assertTrue(approxMessagesQueue2 > 0);
        }
    }

    private SqsAsyncClient buildSdkClient() {
        var auth = new AWSKeysAuthentication();
        auth.setAccessKey("test");
        auth.setSecretKey("test");
        var endpoint = new CustomEndpoint();
        endpoint.setServiceEndpoint(getSqsEndpoint("_aws"));
        endpoint.setSigningRegion(awsRegion);
        var sdk = new AsyncSQSClientFactory().createClient(() -> {
            try {
                return auth.getAWSCredentials();
            } catch (Exception ex) {
                return null;
            }

        }, ClientOverrideConfiguration.builder().build(), endpoint);
        return (SqsAsyncClient) sdk;
    }

}
