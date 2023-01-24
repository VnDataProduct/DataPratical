package org.vndataproduct.httplog001;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class MainStarter {

   public void handleRequest(RoutingContext context) {
      //Get response data from context
      HttpServerResponse response = context.response();
      response.setStatusCode(200).end();
      try {
         //This will get body as Buffer then cast it to JsonObject
         JsonObject dataBody = context.body().asJsonObject();
         System.out.println("Data Received: " + dataBody.toString());
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public static void main(String[] args) {
      //Each application should have one vertx instance. We can create multiple threading on one instance later.
      Vertx vertx = Vertx.vertx();
      //Create a MainStarter object from the class, so we can call the method handleRequest from the object.
      MainStarter handlerObj = new MainStarter();
      //Create a router from vertx instance.
      Router router = Router.router(vertx);
      router.post("/accept_tracking")
            .handler(BodyHandler.create())
            .handler(handlerObj::handleRequest);
      //
      HttpServer httpServer = vertx.createHttpServer();
      httpServer.requestHandler(router).listen(8080);
   }
}
