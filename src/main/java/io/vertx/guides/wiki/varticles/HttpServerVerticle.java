package io.vertx.guides.wiki.varticles;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class HttpServerVerticle extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(HttpServerVerticle.class);
  private static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
  private static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";
  private String defaultWikiQueue = "wikidb.queue";
  private FreeMarkerTemplateEngine templateEngine;
  private static final String EMPTY_PAGE_MARKDOWN =
      "# A new page\n" + "\n" + "Feel-free to write in Markdown!\n";

  @Override
  public void start(Promise<Void> promise) {
    defaultWikiQueue = config().getString(CONFIG_WIKIDB_QUEUE, defaultWikiQueue);

    HttpServer httpServer = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeletionHandler);

    templateEngine = FreeMarkerTemplateEngine.create(vertx);
    int port = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    httpServer
        .requestHandler(router)
        .listen(
            port,
            ar -> {
              if (ar.succeeded()) {
                logger.info("Http varticle deployed in port {}", port);
                promise.complete();
              } else {
                logger.error("Http server creation failed", ar.cause());
                promise.fail(ar.cause());
              }
            });
  }

  private void indexHandler(RoutingContext routingContext) {
    DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-all-pages");
    vertx
        .eventBus()
        .request(
            defaultWikiQueue,
            new JsonObject(),
            options,
            reply -> {
              if (reply.succeeded()) {
                JsonObject body = (JsonObject) reply.result().body();
                routingContext.put("title", body.getString("title"));
                routingContext.put("pages", body.getJsonArray("pages").getList());

                templateEngine.render(
                    routingContext.data(),
                    "templates/index.ftl",
                    ar -> {
                      if (ar.succeeded()) {
                        routingContext.response().putHeader("Content-Type", "text/html");
                        routingContext.response().end(ar.result());
                      } else {
                        routingContext.fail(ar.cause());
                      }
                    });
              } else {
                routingContext.fail(reply.cause());
              }
            });
  }

  private void pageRenderingHandler(RoutingContext routingContext) {
    String page = routingContext.request().getParam("page");
    DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-wiki-page");
    JsonObject jsonObject = new JsonObject().put("page", page);
    vertx
        .eventBus()
        .request(
            defaultWikiQueue,
            jsonObject,
            options,
            reply -> {
              if (reply.succeeded()) {
                JsonObject object = (JsonObject) reply.result().body();
                boolean found = object.getBoolean("found");
                String content = object.getString("rawContent", EMPTY_PAGE_MARKDOWN);

                routingContext.put("title", page);
                routingContext.put("id", object.getInteger("id", -1));
                routingContext.put("newPage", found ? "no" : "yes");
                routingContext.put("rawContent", content);
                routingContext.put("content", Processor.process(content));
                routingContext.put("timestamp", new Date().toString());

                templateEngine.render(
                    routingContext.data(),
                    "templates/page.ftl",
                    renderResult -> {
                      if (renderResult.succeeded()) {
                        routingContext.response().putHeader("Content-Type", "text/html");
                        routingContext.response().end(renderResult.result());
                      } else {
                        routingContext.fail(renderResult.cause());
                      }
                    });

              } else {
                routingContext.fail(reply.cause());
              }
            });
  }

  private void pageCreateHandler(RoutingContext routingContext) {
    String page = routingContext.request().getParam("name");
    String location = "/wiki/" + page;
    if (null == page || page.isEmpty()) {
      location = "/";
    }

    routingContext.response().setStatusCode(303);
    routingContext.response().putHeader("Location", location);
    routingContext.response().end();
  }

  private void pageUpdateHandler(RoutingContext routingContext) {
    String id = routingContext.request().getParam("id");
    String title = routingContext.request().getParam("title");
    String markdown = routingContext.request().getParam("markdown");
    boolean newPage = "Yes".equalsIgnoreCase(routingContext.request().getParam("newPage"));
    JsonObject object = new JsonObject();
      DeliveryOptions options = new DeliveryOptions();
    if (newPage) {
      object.put("title", title);
      object.put("markdown", markdown);
      options.addHeader("action", "insert-wiki-page");
      logger.info("This is an insert operation");
    } else {
      object.put("markdown", markdown);
      object.put("id", id);
      options.addHeader("action", "update-wiki-page");
        logger.info("This is an update operation");
    }
    vertx
        .eventBus()
        .request(
            defaultWikiQueue,
            object,
            options,
            reply -> {
              if (reply.succeeded()) {
                routingContext.response().setStatusCode(303);
                routingContext.response().putHeader("Location", "/wiki/" + title);
                routingContext.response().end();
              } else {
                routingContext.fail(reply.cause());
              }
            });
  }

  private void pageDeletionHandler(RoutingContext routingContext) {
    String id = routingContext.request().getParam("id");
    String title = routingContext.request().getParam("title");
    String location = "/";
    JsonObject object = new JsonObject().put("id", id);
    DeliveryOptions options = new DeliveryOptions().addHeader("action", "delete-wiki-page");
    vertx
        .eventBus()
        .request(
            defaultWikiQueue,
            object,
            options,
            reply -> {
              routingContext.response().setStatusCode(303);
              if (reply.succeeded()) {
                routingContext.response().putHeader("Location", location);

              } else {
                String newLocation = location + "wiki/" + title;
                routingContext.response().putHeader("Location", newLocation);
              }
              routingContext.response().end();
            });
  }
}
