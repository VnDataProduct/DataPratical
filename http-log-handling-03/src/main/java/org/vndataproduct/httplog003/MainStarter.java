package org.vndataproduct.httplog003;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.redis.client.*;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MainStarter {

   //RedisAPI redisAPI = null;
   //public void setRedisAPI(RedisAPI redisAPI) { this.redisAPI = redisAPI; }
   RedisConnection redisConn = null;
   public void setRedisConn(RedisConnection redisConn) {
      this.redisConn = redisConn;
   }

   DateFormat hDF = new SimpleDateFormat("yyyy-MM-dd-HH");
   DateFormat fileDF = new SimpleDateFormat(
         "'logs/register_event/year='yyyy'/month='MM'/day='dd'/hour='HH'/register_event_log_'yyyy-MM-dd-HH'.tsv'");

   public void handleRequest(RoutingContext context) {
      //Get response data from context
      HttpServerResponse response = context.response();
      response.setStatusCode(200).end();
      try {
         //This will get body as Buffer then cast it to JsonObject
         JsonObject dataBody = context.body().asJsonObject();
         System.out.println("Data Received: " + dataBody.toString());
         //If we let above methods use their own time, it is possible that data will be discrepancy
         Date receiveTime = new Date();
         this.sumToRedis(receiveTime, dataBody);
         this.writeLog(context.vertx(), receiveTime, dataBody);
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void sumToRedis(Date receiveTime, JsonObject row) {
      boolean isSuccess = row.getString("result").equals("success");
      String hKey = hDF.format(receiveTime);
      List<Request> requests = new ArrayList<>();
      if (isSuccess) {
         requests.add(Request.cmd(Command.HINCRBY, hKey, "success_device:" + row.getString("device"), 1));
         requests.add(Request.cmd(Command.HINCRBY, hKey, "success_browser:" + row.getString("browser"), 1));
         requests.add(Request.cmd(Command.HINCRBY, hKey, "success_time", row.getInteger("duration")));
         if (row.getString("accountId") != null)
            requests.add(Request.cmd(Command.HINCRBY, hKey, "success_reg_user", 1));
      } else {
         requests.add(Request.cmd(Command.HINCRBY, hKey, "error_device:" + row.getString("device"), 1));
         requests.add(Request.cmd(Command.HINCRBY, hKey, "error_browser:" + row.getString("browser"), 1));
         requests.add(Request.cmd(Command.HINCRBY, hKey, "error_time", row.getInteger("duration")));
         requests.add(Request.cmd(Command.HINCRBY, hKey, "error_cause:" + row.getString("message"), 1));
         if (row.getString("accountId") != null)
            requests.add(Request.cmd(Command.HINCRBY, hKey, "error_reg_user", 1));
      }
      this.redisConn.batch(requests);
   }

   //Since FileSystem instance need to be created from Vertx instance, we have to put in method
   private void writeLog(Vertx vertx, Date receiveTime, JsonObject row) {
      FileSystem fs = vertx.fileSystem();
      StringBuilder sb = new StringBuilder();
      sb.append(row.getString("device")).append("\t");
      sb.append(row.getString("browser")).append("\t");
      sb.append(row.getString("result")).append("\t");
      sb.append(row.getString("message")).append("\t");
      sb.append(row.getInteger("duration")).append("\t");
      sb.append(row.getString("accountId", ""));
      String fileName = fileDF.format(receiveTime);
      //Because log files are writting to directories now, so we have to create it whenever file is not created yet
      if(!fs.existsBlocking(fileName)) {
         fs.mkdirsBlocking(fileName.substring(0, fileName.lastIndexOf("/")));
         fs.createFileBlocking(fileName);
      }
      fs.writeFile(fileName, Buffer.buffer(sb.toString()));
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
      //Initialize Redis instance
      RedisOptions options = new RedisOptions()
            .setConnectionString("redis://localhost:6379");
      Future<RedisConnection> connect = Redis.createClient(vertx, options).connect();
      connect.onSuccess(res -> {
         //handlerObj.setRedisAPI(RedisAPI.api(res));
         handlerObj.setRedisConn(res);
         //We need to make sure Redis connection is established successfully before init the HTTP service
         HttpServer httpServer = vertx.createHttpServer();
         httpServer.requestHandler(router).listen(8080);
      });
   }
}
