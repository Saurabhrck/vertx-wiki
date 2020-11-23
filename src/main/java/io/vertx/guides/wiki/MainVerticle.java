package io.vertx.guides.wiki;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

  private JDBCClient dbClient;
  private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);
  private FreeMarkerTemplateEngine templateEngine;
  private static final String SQL_CREATE_PAGES_TABLE =
      "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
  private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
  private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
  private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
  private static final String SQL_ALL_PAGES = "select Name from Pages";
  private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";
  private static final String EMPTY_PAGE_MARKDOWN =
      "# A new page\n" + "\n" + "Feel-free to write in Markdown!\n";

  @Override
  public void start(Promise<Void> promise) {
    Future<Void> result = createDbConnection().compose(v -> createHttpClient());
    result.onComplete(
        ar -> {
          if (ar.succeeded()) {
            logger.info("Success");
            promise.complete();
          } else {
            logger.error("failed");
            promise.fail(ar.cause());
          }
        });
  }

  private Future<Void> createDbConnection() {
    Promise<Void> promise = Promise.promise();
    dbClient =
        JDBCClient.createShared(
            vertx,
            new JsonObject()
                .put("url", "jdbc:hsqldb:file:db/wiki")
                .put("driver_class", "org.hsqldb.jdbcDriver")
                .put("max_pool_size", 30));

    dbClient.getConnection(
        ar -> {
          if (ar.failed()) {
            logger.error("Connection failed with DB", ar.cause());
          } else {
            SQLConnection connection = ar.result();
            connection.execute(
                SQL_CREATE_PAGES_TABLE,
                create -> {
                  connection.close();
                  if (create.failed()) {
                    logger.error("Could not create DB connection");
                    promise.fail(create.cause());
                  } else {
                    logger.info("DB verticle deployed");
                    promise.complete();
                  }
                });
          }
        });
    return promise.future();
  }

  private Future<Void> createHttpClient() {
    Promise<Void> promise = Promise.promise();
    HttpServer httpServer = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeletionHandler);

    templateEngine = FreeMarkerTemplateEngine.create(vertx);
    httpServer
        .requestHandler(router)
        .listen(
            8888,
            ar -> {
              if (ar.succeeded()) {
                logger.info("Http varticle deployed in port 8888");
                promise.complete();
              } else {
                logger.error("Http server creation failed", ar.cause());
                promise.fail(ar.cause());
              }
            });

    return promise.future();
  }

  private void indexHandler(RoutingContext routingContext) {
    dbClient.getConnection(
        result -> {
          if (result.succeeded()) {
            SQLConnection connection = result.result();
            connection.query(
                SQL_ALL_PAGES,
                query -> {
                  connection.close();
                  if (query.succeeded()) {
                    List<String> pages =
                        query.result().getResults().stream()
                            .map(json -> json.getString(0))
                            .sorted()
                            .collect(Collectors.toList());
                    routingContext.put("title", "Wiki Home");
                    routingContext.put("pages", pages);

                    templateEngine.render(
                        routingContext.data(),
                        "templates/index.ftl",
                        ar -> {
                          if (ar.succeeded()) {
                            routingContext.response().putHeader("Content-Type", "text/html");
                            routingContext.response().end(ar.result());
                          }
                        });

                  } else {
                    routingContext.fail(query.cause());
                  }
                });
          } else {
            routingContext.fail(result.cause());
          }
        });
  }

  private void pageRenderingHandler(RoutingContext routingContext) {
    String page = routingContext.request().getParam("page");
    dbClient.getConnection(
        result -> {
          if (result.succeeded()) {
            SQLConnection connection = result.result();
            connection.queryWithParams(
                SQL_GET_PAGE,
                new JsonArray().add(page),
                fetch -> {
                  connection.close();
                  if (fetch.succeeded()) {
                    JsonArray row =
                        fetch.result().getResults().stream()
                            .findFirst()
                            .orElseGet(() -> new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));
                    Integer id = row.getInteger(0);
                    String content = row.getString(1);

                    routingContext.put("title", page);
                    routingContext.put("id", id);
                    routingContext.put(
                        "newPage", fetch.result().getResults().size() == 0 ? "yes" : "no");
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
                    routingContext.fail(fetch.cause());
                  }
                });
          } else {
            routingContext.fail(result.cause());
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

    dbClient.getConnection(
        car -> {
          if (car.succeeded()) {
            SQLConnection connection = car.result();
            String sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;
            JsonArray jsonArray = new JsonArray();
            if (newPage) {
              jsonArray.add(title).add(markdown);
            } else {
              jsonArray.add(markdown).add(id);
            }
            connection.updateWithParams(
                sql,
                jsonArray,
                result -> {
                  connection.close();
                  if (result.succeeded()) {
                    routingContext.response().setStatusCode(303);
                    routingContext.response().putHeader("Location", "/wiki/" + title);
                    routingContext.response().end();
                  } else {
                    routingContext.fail(result.cause());
                  }
                });
          } else {
            routingContext.fail(car.cause());
          }
        });
  }

  private void pageDeletionHandler(RoutingContext routingContext) {
    String id = routingContext.request().getParam("id");
    String title = routingContext.request().getParam("title");
    String location = "/";

    dbClient.getConnection(
        car -> {
          if (car.succeeded()) {
            SQLConnection connection = car.result();
            JsonArray jsonArray = new JsonArray().add(id);
            connection.updateWithParams(
                SQL_DELETE_PAGE,
                jsonArray,
                ar -> {
                  connection.close();
                  if (ar.succeeded()) {
                    routingContext.response().setStatusCode(303);
                    routingContext.response().putHeader("Location", location);
                  } else {
                    String newLocation = location + "wiki/" + title;
                    routingContext.response().setStatusCode(303);
                    routingContext.response().putHeader("Location", newLocation);
                  }
                  routingContext.response().end();
                });
          } else {
            routingContext.fail(car.cause());
          }
        });
  }
}
