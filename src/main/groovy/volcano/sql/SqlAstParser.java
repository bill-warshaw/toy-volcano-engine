package volcano.sql;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;

import volcano.db.Database;
import volcano.operator.Operator;

public class SqlAstParser {

  private final HttpClient httpClient;
  private final String address;

  public SqlAstParser(String host, int port) {
    this.httpClient = HttpClients.createDefault();
    this.address = String.format("http://%s:%s/ast", host, port);
  }

  public Operator parse(String sql, Database db) throws Exception {
    Map<String,Object> ast = getAst(sql);
    String type = (String)ast.get("type");
    if (type.equalsIgnoreCase("select")) {
      return new SqlSelectNode(ast, db).toOperator(db);
    } else {
      throw new IllegalArgumentException("expected top-level select, but was " + type);
    }
  }

  private Map<String,Object> getAst(String sql) throws Exception {
    Gson gson = new Gson();
    HttpPost post = new HttpPost(address);
    post.addHeader("content-type", "application/json");
    Map<String,String> bodyMap = new HashMap<>();
    bodyMap.put("sql", sql);
    StringEntity bodyEntity = new StringEntity(gson.toJson(bodyMap), Charsets.UTF_8);
    post.setEntity(bodyEntity);
    HttpResponse response = httpClient.execute(post);
    if (response.getStatusLine().getStatusCode() > 201) {
      throw new IllegalStateException(String.format("Error contacting SQL parsing service (%d): %s",
          response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase()));
    }
    return gson.fromJson(EntityUtils.toString(response.getEntity()), Map.class);
  }
}
