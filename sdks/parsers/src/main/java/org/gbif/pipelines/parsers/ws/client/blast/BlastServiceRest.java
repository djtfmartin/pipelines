package org.gbif.pipelines.parsers.ws.client.blast;

import java.util.concurrent.TimeUnit;

import org.gbif.pipelines.parsers.config.WsConfig;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class BlastServiceRest {
  private final BlastService service;
  private static volatile BlastServiceRest instance;
  private static final Object MUTEX = new Object();

  private BlastServiceRest(WsConfig wsConfig) {

    // create client
    OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(wsConfig.getTimeout(), TimeUnit.SECONDS)
            .readTimeout(wsConfig.getTimeout(), TimeUnit.SECONDS)
            .build();

    // create service
    Retrofit retrofit =
        new Retrofit.Builder()
            .client(client)
            .baseUrl(wsConfig.getBasePath())
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    service = retrofit.create(BlastService.class);
  }

  public static BlastServiceRest getInstance(WsConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new BlastServiceRest(config);
        }
      }
    }
    return instance;
  }

  public BlastService getService() {
    return service;
  }

}
