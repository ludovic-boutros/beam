/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.elasticsearch.utils;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.rnorth.ducttape.unreliables.Unreliables.retryUntilSuccess;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.jetbrains.annotations.NotNull;
import org.rnorth.ducttape.TimeoutException;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.MountableFile;

/**
 * Represents an elasticsearch docker instance which exposes by default port 9200 and 9300. <br>
 * The original source is taken from the original project and uses SSL. <br>
 * The docker image is by default fetched from docker.elastic.co/elasticsearch/elasticsearch.
 */
public class ElasticsearchWithSSLContainer extends GenericContainer<ElasticsearchWithSSLContainer> {

  /** Elasticsearch Default HTTP port. */
  private static final int ELASTICSEARCH_DEFAULT_PORT = 9200;

  /** Elasticsearch Default Transport port. */
  private static final int ELASTICSEARCH_DEFAULT_TCP_PORT = 9300;

  /** Elasticsearch container default user. */
  public static final String ELASTICSEARCH_DEFAULT_USER = "elastic";

  public static final String TRUST_AND_KEY_STORE_PASSWORD = "changeme";
  public static final String TRUST_AND_KEY_STORE = "src/test/resources/certs/truststore.jks";

  /**
   * Create an Elasticsearch Container by passing the full docker image name.
   *
   * @param dockerImageName Full docker image name, like:
   *     docker.elastic.co/elasticsearch/elasticsearch:7.5.1
   */
  public ElasticsearchWithSSLContainer(String dockerImageName, @NotNull String password) {
    super(dockerImageName);
    logger().info("Starting an elasticsearch container using [{}]", dockerImageName);
    withNetworkAliases("elasticsearch-" + Base58.randomString(6));

    withEnv("discovery.type", "single-node");

    withPassword(password);
    withSSL();

    addExposedPorts(ELASTICSEARCH_DEFAULT_PORT, ELASTICSEARCH_DEFAULT_TCP_PORT);
    setWaitStrategy(
        new ESWaitStrategy()
            .withBasicCredentials(ELASTICSEARCH_DEFAULT_USER, password)
            .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
            .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private void withPassword(@NotNull String password) {
    withEnv("ELASTIC_PASSWORD", password);
  }

  private void withSSL() {
    MountableFile certs = MountableFile.forClasspathResource("certs");
    withCopyFileToContainer(certs, "/usr/share/elasticsearch/config/certs");
    withEnv("xpack.security.enabled", "true");
    withEnv("xpack.security.http.ssl.enabled", "true");
    withEnv("xpack.security.http.ssl.key", "/usr/share/elasticsearch/config/certs/es01/es01.key");
    withEnv(
        "xpack.security.http.ssl.certificate_authorities",
        "/usr/share/elasticsearch/config/certs/ca/ca.crt");
    withEnv(
        "xpack.security.http.ssl.certificate",
        "/usr/share/elasticsearch/config/certs/es01/es01.crt");
  }

  private static class ESWaitStrategy
      extends org.testcontainers.containers.wait.strategy.AbstractWaitStrategy {
    private static final org.slf4j.Logger log =
        org.slf4j.LoggerFactory.getLogger(HttpWaitStrategy.class);
    /** Authorization HTTP header. */
    private static final String HEADER_AUTHORIZATION = "Authorization";
    /** Basic Authorization scheme prefix. */
    private static final String AUTH_BASIC = "Basic ";

    private Set<Integer> statusCodes = new HashSet<>();
    private String username;
    private String password;
    private Predicate<Integer> statusCodePredicate = null;

    /**
     * Waits for the status code to pass the given predicate.
     *
     * @param statusCodePredicate The predicate to test the response against
     * @return this
     */
    private ESWaitStrategy forStatusCodeMatching(Predicate<Integer> statusCodePredicate) {
      this.statusCodePredicate = statusCodePredicate;
      return this;
    }

    /**
     * Authenticate with HTTP Basic Authorization credentials.
     *
     * @param username the username
     * @param password the password
     * @return this
     */
    private ESWaitStrategy withBasicCredentials(String username, String password) {
      this.username = username;
      this.password = password;
      return this;
    }

    @Override
    protected void waitUntilReady() {
      final String containerName = waitStrategyTarget.getContainerInfo().getName();
      final int livenessCheckPort = waitStrategyTarget.getMappedPort(ELASTICSEARCH_DEFAULT_PORT);
      final String uri = buildLivenessUri(livenessCheckPort).toString();

      SSLSocketFactory sslsocketfactory = setSslEnv();

      log.info(
          "{}: Waiting for {} seconds for URL: {}",
          containerName,
          startupTimeout.getSeconds(),
          uri);
      // try to connect to the URL
      try {
        retryUntilSuccess(
            (int) startupTimeout.getSeconds(),
            TimeUnit.SECONDS,
            () -> {
              getRateLimiter()
                  .doWhenReady(
                      () -> {
                        try {
                          final HttpsURLConnection connection =
                              (HttpsURLConnection) new URL(uri).openConnection();
                          // authenticate
                          if (!Strings.isNullOrEmpty(username)) {
                            connection.setRequestProperty(
                                HEADER_AUTHORIZATION, buildAuthString(username, password));
                            connection.setUseCaches(false);
                          }
                          connection.setRequestMethod("GET");
                          connection.setSSLSocketFactory(sslsocketfactory);
                          connection.connect();
                          log.trace("Get response code {}", connection.getResponseCode());
                          if (!statusCodePredicate.test(connection.getResponseCode())) {
                            throw new RuntimeException(
                                String.format(
                                    "HTTP response code was: %s", connection.getResponseCode()));
                          }
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      });
              return true;
            });
      } catch (TimeoutException e) {
        throw new ContainerLaunchException(
            String.format(
                "Timed out waiting for URL to be accessible (%s should return HTTP %s)",
                uri, statusCodes.isEmpty() ? HttpURLConnection.HTTP_OK : statusCodes));
      }
    }

    private SSLSocketFactory setSslEnv() {
      System.setProperty("javax.net.ssl.trustStore", TRUST_AND_KEY_STORE);
      System.setProperty("javax.net.ssl.trustStorePassword", TRUST_AND_KEY_STORE_PASSWORD);
      System.setProperty("javax.net.ssl.keyStore", TRUST_AND_KEY_STORE);
      System.setProperty("javax.net.ssl.keyStorePassword", TRUST_AND_KEY_STORE_PASSWORD);

      return (SSLSocketFactory) SSLSocketFactory.getDefault();
    }

    /**
     * Build the URI on which to check if the container is ready.
     *
     * @param livenessCheckPort the liveness port
     * @return the liveness URI
     */
    private URI buildLivenessUri(int livenessCheckPort) {
      final String scheme = "https://";
      final String host = waitStrategyTarget.getContainerIpAddress();
      final String portSuffix = ":" + livenessCheckPort;
      final String path = "/";
      return URI.create(scheme + host + portSuffix + path);
    }

    /**
     * @param username the username
     * @param password the password
     * @return a basic authentication string for the given credentials
     */
    private String buildAuthString(String username, String password) {
      return AUTH_BASIC
          + BaseEncoding.base64()
              .encode((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }
  }
}
