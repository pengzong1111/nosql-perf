package edu.indiana.d2i.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class GettingStarted2 {

	public static void main(String[] args) {
		Cluster cluster = Cluster.builder().addContactPoint("crow.soic.indiana.edu").withRetryPolicy(DefaultRetryPolicy.INSTANCE).withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build())).build();
		Session session = cluster.connect("demo");
		
		PreparedStatement statement = session.prepare("INSERT INTO users " + "(lastname, age, city, email, firstname)" + "VALUES(?,?,?,?,?);");
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind("Jones", 35, "Austin", "bob@example.com", "Bob"));
		session.execute(boundStatement.bind("Bill", 35, "Bloomington", "bill@example.com", "Cliton"));
		///////////////
		
		Statement select = QueryBuilder.select().all().from("demo", "users");//.where(QueryBuilder.eq("lastname", "Jones"));
		ResultSet results = session.execute(select);
		for(Row row : results) {
			System.out.format("%s %d \n", row.getString("firstname"), row.getInt("age"));
		}
		
		Statement delete = QueryBuilder.delete().from("users").where(QueryBuilder.eq("lastname", "Jones"));
		results = session.execute(delete);
		for(Row row : results) {
			System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"), row.getString("city"), row.getString("email"), row.getString("firstname"));
		}
		
		cluster.close();

	}

}
