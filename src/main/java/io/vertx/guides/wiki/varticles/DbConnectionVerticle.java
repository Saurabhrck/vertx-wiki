package io.vertx.guides.wiki.varticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class DbConnectionVerticle extends AbstractVerticle {

  private JDBCClient dbClient;
  private static final Logger logger = LoggerFactory.getLogger(DbConnectionVerticle.class);
  private static final String CONFIG_WIKI_JDBC_URL = "wiki.jdbc.url";
  private static final String CONFIG_WIKI_JDBC_DRIVER = "wiki.jdbc.driver";
  private static final String CONFIG_WIKI_JDBC_MAX_POOL_SIZE = "wiki.jdbc.max.pool.size";
  private static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";
  private static final String CONFIG_VERTX_DEFAULT_SQL_FILE = "wikidb.sqlqueries.resource.file";

  private enum SqlQuery {
    CREATE_PAGES_TABLE,
    GET_WIKI_PAGE,
    INSERT_WIKI_PAGE,
    UPDATE_WIKI_PAGE,
    GET_ALL_PAGES,
    DELETE_WIKI_PAGE
  }

  public enum ErrorCodes {
    NO_ACTION_SPECIFIED,
    BAD_ACTION,
    DB_ERROR
  }

  private Map<SqlQuery, String> queryMap = new HashMap<>();

  private Future<Void> updateQuery() throws IOException {
    Promise promise = Promise.promise();
    String queryFile = config().getString(CONFIG_VERTX_DEFAULT_SQL_FILE);
    InputStream inputStream;
    if (null != queryFile) {
      inputStream = new FileInputStream(queryFile);
    } else {
      inputStream = getClass().getResourceAsStream("/db-queries.properties");
    }

    Properties queriesProps = new Properties();
    queriesProps.load(inputStream);
    inputStream.close();

    queryMap.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-table"));
    queryMap.put(SqlQuery.GET_WIKI_PAGE, queriesProps.getProperty("get-wiki-page"));
    queryMap.put(SqlQuery.INSERT_WIKI_PAGE, queriesProps.getProperty("insert-wiki-page"));
    queryMap.put(SqlQuery.UPDATE_WIKI_PAGE, queriesProps.getProperty("update-wiki-page"));
    queryMap.put(SqlQuery.GET_ALL_PAGES, queriesProps.getProperty("get-all-pages"));
    queryMap.put(SqlQuery.DELETE_WIKI_PAGE, queriesProps.getProperty("delete-wiki-page"));
    promise.complete("DB loaded properly");

    return promise.future();
  }

  @Override
  public void start(Promise<Void> promise) throws Exception {
    WorkerExecutor executor = vertx.createSharedWorkerExecutor("my-worker-pool");
    executor.executeBlocking(
        workerPromise -> {
          String result = null;
          try {
            result = updateQuery().result().toString();
          } catch (IOException e) {
            workerPromise.fail("Exception occurred, cause - " + e.getCause().toString());
          }
          workerPromise.complete(result);
        },
        res -> {
          logger.info("The result is: " + res.result());
        });

    dbClient =
        JDBCClient.createShared(
            vertx,
            new JsonObject()
                .put("url", config().getString(CONFIG_WIKI_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
                .put(
                    "driver_class",
                    config().getString(CONFIG_WIKI_JDBC_DRIVER, "org.hsqldb.jdbcDriver"))
                .put("max_pool_size", config().getInteger(CONFIG_WIKI_JDBC_MAX_POOL_SIZE, 30)));

    dbClient.getConnection(
        ar -> {
          if (ar.failed()) {
            logger.error("Could not open a database connection", ar.cause());
            promise.fail(ar.cause());
          } else {
            SQLConnection connection = ar.result();
            connection.execute(
                queryMap.get(SqlQuery.CREATE_PAGES_TABLE),
                create -> { // <2>
                  connection.close();
                  if (create.failed()) {
                    logger.error("Database preparation error", create.cause());
                    promise.fail(create.cause());
                  } else {
                    vertx
                        .eventBus()
                        .consumer(
                            config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"),
                            this::onMesage); // <3>
                    promise.complete();
                  }
                });
          }
        });
  }

  private void onMesage(Message<JsonObject> tMessage) {
    if (!tMessage.headers().contains("action")) {
      logger.error(
          "No action is specified with header {} and body {}",
          tMessage.headers(),
          tMessage.body().encodePrettily());
      tMessage.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action specified");
      return;
    }

    String action = tMessage.headers().get("action");
    switch (action) {
      case "get-all-pages":
        getAllPages(tMessage);
        break;
      case "get-wiki-page":
        getWikiPage(tMessage);
        break;
      case "insert-wiki-page":
        insertPage(tMessage);
        break;
      case "update-wiki-page":
        updatePage(tMessage);
        break;
      case "delete-wiki-page":
        deletePage(tMessage);
        break;
      default:
        tMessage.fail(
            ErrorCodes.BAD_ACTION.ordinal(), "Provided action " + action + " is not permitted");
    }
  }

  private void getAllPages(Message<JsonObject> tMessage) {
    dbClient.query(
        queryMap.get(SqlQuery.GET_ALL_PAGES),
        result -> {
          if (result.succeeded()) {
            List<String> pages =
                result.result().getResults().stream()
                    .map(json -> json.getString(0))
                    .sorted()
                    .collect(Collectors.toList());
            JsonObject jsonObject = new JsonObject().put("title", "Wiki Home").put("pages", pages);
            tMessage.reply(jsonObject);
          } else {
            handleFailMessage(tMessage, result.cause());
          }
        });
  }

  private void getWikiPage(Message<JsonObject> tMessage) {
    String page = tMessage.body().getString("page");
    JsonArray jsonArray = new JsonArray().add(page);
    dbClient.queryWithParams(
        queryMap.get(SqlQuery.GET_WIKI_PAGE),
        jsonArray,
        result -> {
          if (result.succeeded()) {
            ResultSet resultSet = result.result();
            JsonObject jsonObject = new JsonObject();
            if (resultSet.getNumRows() == 0) {
              jsonObject.put("found", false);
            } else {
              jsonObject.put("found", true);
              JsonArray row = resultSet.getResults().get(0);
              jsonObject.put("id", row.getInteger(0));
              jsonObject.put("rawContent", row.getString(1));
            }
            tMessage.reply(jsonObject);
          } else {
            handleFailMessage(tMessage, result.cause());
          }
        });
  }

  private void insertPage(Message<JsonObject> tMessage) {
    String title = tMessage.body().getString("title");
    String content = tMessage.body().getString("markdown");
    JsonArray inputArray = new JsonArray().add(title).add(content);
    dbClient.updateWithParams(
        queryMap.get(SqlQuery.INSERT_WIKI_PAGE),
        inputArray,
        result -> {
          if (result.succeeded()) {
            logger.info("Data successfully inserted with title {}", title);
            tMessage.reply("OK");
          } else {
            handleFailMessage(tMessage, result.cause());
          }
        });
  }

  private void updatePage(Message<JsonObject> tMessage) {
    String content = tMessage.body().getString("markdown");
    String id = tMessage.body().getString("id");
    JsonArray inputArray = new JsonArray().add(content).add(id);
    dbClient.updateWithParams(
        queryMap.get(SqlQuery.UPDATE_WIKI_PAGE),
        inputArray,
        result -> {
          if (result.succeeded()) {
            System.out.println(inputArray.getString(0));
            logger.info("Data successfully updated with id {}", id);
            tMessage.reply("ok");
          } else {
            handleFailMessage(tMessage, result.cause());
          }
        });
  }

  private void deletePage(Message<JsonObject> tMessage) {
    String id = tMessage.body().getString("id");
    dbClient.queryWithParams(
        queryMap.get(SqlQuery.DELETE_WIKI_PAGE),
        new JsonArray().add(id),
        result -> {
          if (result.succeeded()) {
            tMessage.reply("ok");
          } else {
            handleFailMessage(tMessage, result.cause());
          }
        });
  }

  private void handleFailMessage(Message<JsonObject> tMessage, Throwable cause) {
    logger.error(
        "DB operation failed with header {} body {}",
        tMessage.headers(),
        tMessage.body().encodePrettily());
    tMessage.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
  }
}
