package io.vertx.guides.wiki;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;
import io.vertx.guides.wiki.varticles.DbConnectionVerticle;
import io.vertx.guides.wiki.varticles.HttpServerVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> promise) {
    Promise<String> dbVerticlePromise = Promise.promise();
    vertx.deployVerticle(new DbConnectionVerticle(), dbVerticlePromise);
    dbVerticlePromise
        .future()
        .compose(
            id -> {
              Promise<String> httpVerticlePromise = Promise.promise();
              vertx.deployVerticle(new HttpServerVerticle(), httpVerticlePromise);
              return httpVerticlePromise.future();
            })
        .onComplete(
            result -> {
              if (result.succeeded()) {
                logger.info("Deployment DOne");
                promise.complete();
              } else {
                logger.error("Deployment failed");
                promise.fail(result.cause());
              }
            });
  }
}
