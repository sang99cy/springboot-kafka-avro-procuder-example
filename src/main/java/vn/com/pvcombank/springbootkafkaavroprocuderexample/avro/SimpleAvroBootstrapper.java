package vn.com.pvcombank.springbootkafkaavroprocuderexample.avro;

import io.apicurio.registry.client.RegistryRestClient;
import io.apicurio.registry.client.RegistryRestClientFactory;
import io.apicurio.registry.rest.beans.ArtifactMetaData;
import io.apicurio.registry.rest.beans.IfExistsType;
import io.apicurio.registry.types.ArtifactType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class SimpleAvroBootstrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAvroBootstrapper.class);

    private static RegistryRestClient client;
    static {
        // Create a Service Registry client
        String registryUrl = "http://localhost:8081/api";
        client = RegistryRestClientFactory.create(registryUrl);
    }

    public static final void main(String [] args) throws Exception {
        try {
            LOGGER.info("\n\n--------------\nBootstrapping the Avro Schema demo.\n--------------\n");
            String topicName = SimpleAvroAppConstants.TOPIC_NAME;

            // Register the Avro Schema schema in the Apicurio registry.
            String artifactId = topicName;
            try {
                createSchemaInServiceRegistry(artifactId, SimpleAvroAppConstants.SCHEMA);
            } catch (Exception e) {
                if (is409Error(e)) {
                    LOGGER.warn("\n\n--------------\nWARNING: Schema already existed in registry!\n--------------\n");
                    return;
                } else {
                    throw e;
                }
            }

            LOGGER.info("\n\n--------------\nBootstrapping complete.\n--------------\n");
        } finally {
        }
    }

    /**
     * Create the artifact in the registry (or update it if it already exists).
     * @param artifactId
     * @param schema
     * @throws Exception
     */
    private static void createSchemaInServiceRegistry(String artifactId, String schema) throws Exception {

        LOGGER.info("---------------------------------------------------------");
        LOGGER.info("=====> Creating artifact in the registry for Avro Schema with ID: {}", artifactId);
        try {
            ByteArrayInputStream content = new ByteArrayInputStream(schema.getBytes(StandardCharsets.UTF_8));
            ArtifactMetaData metaData = client.createArtifact(artifactId, ArtifactType.AVRO, IfExistsType.RETURN, content);
            LOGGER.info("=====> Successfully created Avro Schema artifact in Service Registry: {}", metaData);
            LOGGER.info("---------------------------------------------------------");
        } catch (Exception t) {
            throw t;
        }
    }

    private static boolean is409Error(Exception e) {
        if (e.getCause() instanceof WebApplicationException) {
            WebApplicationException wae = (WebApplicationException) e.getCause();
            if (wae.getResponse().getStatus() == 409) {
                return true;
            }
        }
        return false;
    }

}
