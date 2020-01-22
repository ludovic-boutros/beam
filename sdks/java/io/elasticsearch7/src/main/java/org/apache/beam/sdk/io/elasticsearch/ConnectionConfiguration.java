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
package org.apache.beam.sdk.io.elasticsearch;

import com.google.auto.value.AutoValue;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.security.KeyStore;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/** A POJO describing a connection configuration to Elasticsearch. */
@AutoValue
public abstract class ConnectionConfiguration implements Serializable {

  public abstract List<String> getAddresses();

  @Nullable
  public abstract String getUsername();

  @Nullable
  public abstract String getPassword();

  @Nullable
  public abstract String getKeystorePath();

  @Nullable
  public abstract String getKeystorePassword();

  @Nullable
  public abstract String getIndex();

  @Nullable
  public abstract Integer getSocketAndRetryTimeout();

  @Nullable
  public abstract Integer getConnectTimeout();

  public static Builder builder() {
    return new AutoValue_ConnectionConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setAddresses(List<String> addresses);

    public abstract Builder setUsername(String username);

    public abstract Builder setPassword(String password);

    public abstract Builder setKeystorePath(String keystorePath);

    public abstract Builder setKeystorePassword(String password);

    public abstract Builder setIndex(String index);

    public abstract Builder setSocketAndRetryTimeout(Integer maxRetryTimeout);

    public abstract Builder setConnectTimeout(Integer connectTimeout);

    public abstract ConnectionConfiguration build();
  }

  void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("address", getAddresses().toString()));
    builder.add(DisplayData.item("index", getIndex()));
    builder.addIfNotNull(DisplayData.item("username", getUsername()));
    builder.addIfNotNull(DisplayData.item("keystore.path", getKeystorePath()));
    builder.addIfNotNull(DisplayData.item("socketAndRetryTimeout", getSocketAndRetryTimeout()));
    builder.addIfNotNull(DisplayData.item("connectTimeout", getConnectTimeout()));
  }

  RestHighLevelClient createClient() throws IOException {
    HttpHost[] hosts = new HttpHost[getAddresses().size()];
    int i = 0;
    for (String address : getAddresses()) {
      URL url = new URL(address);
      hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
      i++;
    }

    RestClientBuilder restClientBuilder = RestClient.builder(hosts);
    final CredentialsProvider credentialsProvider;
    if (getUsername() != null) {
      credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(getUsername(), getPassword()));
    } else {
      credentialsProvider = null;
    }

    final SSLContext sslContext;
    final SSLIOSessionStrategy sessionStrategy;

    if (getKeystorePath() != null && !getKeystorePath().isEmpty()) {
      try {
        KeyStore keyStore = KeyStore.getInstance("jks");
        try (InputStream is = new FileInputStream(new File(getKeystorePath()))) {
          String keystorePassword = getKeystorePassword();
          keyStore.load(is, (keystorePassword == null) ? null : keystorePassword.toCharArray());
        }
        sslContext =
            SSLContexts.custom().loadTrustMaterial(keyStore, new TrustSelfSignedStrategy()).build();

        sessionStrategy = new SSLIOSessionStrategy(sslContext);
      } catch (Exception e) {
        throw new IOException("Can't load the client certificate from the keystore", e);
      }
    } else {
      sslContext = null;
      sessionStrategy = null;
    }

    restClientBuilder.setHttpClientConfigCallback(
        httpClientBuilder -> {
          if (credentialsProvider != null) {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          }

          if (sslContext != null) {
            httpClientBuilder.setSSLContext(sslContext).setSSLStrategy(sessionStrategy);
          }

          return httpClientBuilder;
        });

    restClientBuilder.setRequestConfigCallback(
        requestConfigBuilder -> {
          if (getConnectTimeout() != null) {
            requestConfigBuilder.setConnectTimeout(getConnectTimeout());
          }
          if (getSocketAndRetryTimeout() != null) {
            requestConfigBuilder.setSocketTimeout(getSocketAndRetryTimeout());
          }
          return requestConfigBuilder;
        });

    return new RestHighLevelClient(restClientBuilder);
  }
}
