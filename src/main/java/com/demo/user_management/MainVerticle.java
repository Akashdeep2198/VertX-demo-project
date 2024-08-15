package com.demo.user_management;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class MainVerticle extends AbstractVerticle {

  private PgPool client;

  @Override
  public void start(Promise<Void> startPromise) {
    PgConnectOptions connectOptions = new PgConnectOptions()
      .setPort(5432)
      .setHost("localhost")
      .setDatabase("user_details")
      .setUser("postgres")
      .setPassword("root");

    PoolOptions poolOptions = new PoolOptions().setMaxSize(5);

    client = PgPool.pool(vertx, connectOptions, poolOptions);

    createTable();

    Router router = Router.router(vertx);

    router.route().handler(BodyHandler.create());

    router.post("/api/users/add-user").handler(this::addUser);
    router.get("/api/users").handler(this::getUsers);
    router.put("/api/users/:id").handler(this::updateUser);

    // Start the HTTP server
    vertx.createHttpServer()
      .requestHandler(router)
      .listen(8888, result -> {
        if (result.succeeded()) {
          startPromise.complete();
          System.out.println("HTTP server started on port 8888");
        } else {
          startPromise.fail(result.cause());
        }
      });
  }

  public void createTable() {
    client.getConnection(ar -> {
      if (ar.succeeded()) {
        SqlConnection connection = ar.result();
        String createTableSQL = "CREATE TABLE IF NOT EXISTS user_info (" +
          "id UUID PRIMARY KEY, " +
          "name VARCHAR(64) NOT NULL, " +
          "email VARCHAR(64) UNIQUE NOT NULL, " +
          "gender VARCHAR(6) CHECK (gender IN ('MALE', 'FEMALE')), " +
          "status VARCHAR(9) CHECK (status IN ('ACTIVE', 'INACTIVE')), " +
          "timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP" +
          ")";
        connection.query(createTableSQL).execute(res -> {
          connection.close();
          if (res.succeeded()) {
            System.out.println("Table created successfully");
          } else {
            System.out.println("Failed to create table: " + res.cause().getMessage());
          }
        });
      } else {
        System.out.println("Failed to connect to database: " + ar.cause().getMessage());
      }
    });
  }


  private void addUser(RoutingContext context) {
    JsonObject jsonBody = context.getBodyAsJson();
    UUID id = UUID.randomUUID();
    String name = jsonBody.getString("name");
    String email = jsonBody.getString("email");
    String gender = jsonBody.getString("gender");
    String status = jsonBody.getString("status");

    client
      .preparedQuery("INSERT INTO user_info (id, name, email, gender, status) VALUES ($1, $2, $3, $4, $5)")
      .execute(Tuple.of(id, name, email, gender, status), res -> {
        if (res.succeeded()) {
          context.response().setStatusCode(201).end("User created");
        } else {
          context.response().setStatusCode(500).end("Failed to create user: " + res.cause().getMessage());
        }
      });
  }

  private void getUsers(RoutingContext context) {
    String gender = context.request().getParam("gender");
    String status = context.request().getParam("status");

    // Build the query dynamically based on which parameters are present
    StringBuilder queryBuilder = new StringBuilder("SELECT * FROM user_info WHERE 1=1");

    List<Object> paramsList = new ArrayList<>();

    if (gender != null) {
      queryBuilder.append(" AND gender = $").append(paramsList.size() + 1);
      paramsList.add(gender);
    }
    if (status != null) {
      queryBuilder.append(" AND status = $").append(paramsList.size() + 1);
      paramsList.add(status);
    }

    String query = queryBuilder.toString();
    Tuple params = Tuple.tuple(Arrays.asList(paramsList.toArray()));

    client
      .preparedQuery(query)
      .execute(params, res -> {
        if (res.succeeded()) {
          JsonArray results = new JsonArray();
          res.result().forEach(row -> {
            JsonObject obj = new JsonObject()
              .put("id", row.getUUID("id"))
              .put("name", row.getString("name"))
              .put("email", row.getString("email"))
              .put("gender", row.getString("gender"))
              .put("status", row.getString("status"))
              .put("timestamp", row.getOffsetDateTime("timestamp").toString());
            results.add(obj);
          });
          context.response().putHeader("Content-Type", "application/json").end(results.encodePrettily());
        } else {
          context.response().setStatusCode(500).end("Failed to retrieve users: " + res.cause().getMessage());
        }
      });
  }

  private void updateUser(RoutingContext context) {
    UUID id = UUID.fromString(context.request().getParam("id"));
    JsonObject jsonBody = context.getBodyAsJson();
    String name = jsonBody.getString("name");
    String email = jsonBody.getString("email");
    String gender = jsonBody.getString("gender");
    String status = jsonBody.getString("status");

    client
      .preparedQuery("UPDATE user_info SET name = $1, email = $2, gender = $3, status = $4 WHERE id = $5")
      .execute(Tuple.of(name, email, gender, status, id), res -> {
        if (res.succeeded()) {
          context.response().setStatusCode(200).end("User updated");
        } else {
          context.response().setStatusCode(500).end("Failed to update user: " + res.cause().getMessage());
        }
      });
  }
}
