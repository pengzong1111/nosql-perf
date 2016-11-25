package edu.indiana.d2i.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class GettingStarted {

	public static void main(String[] args) {
		Cluster cluster = Cluster.builder().addContactPoint("crow.soic.indiana.edu").build();
		Session session = cluster.connect("demo");
		System.out.println(session.getClass().getCanonicalName());
	    session.execute("CREATE TABLE volumes ("
	    		+ "volumeID text, "
	    		+ "sequence text, "
	    		+ "byteCount int, "
	    		+ "characterCount int, "
	    		+ "contents text, "
	    		+ "MD5 text, "
	    		+ "SHA1 text, "
	    		+ "pageNumberLabel text, "
	    		+ "PRIMARY KEY (volumeID, sequence))");
	    
		// Insert one record into the users table
		session.execute("INSERT INTO users (lastname, age, city, email, firstname, phone) VALUES ('Xmen', 37, 'Austin', 'bob@example.com', 'Jim', '1234567')");
		ResultSet results = session.execute("SELECT * FROM users WHERE lastname='Xmen'");
		for (Row row : results) {
			System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
		}
		
		// Update the same user with a new age
		session.execute("update users set age = 39 where lastname = 'Xmen'");
		// Select and show the change
		results = session.execute("select * from users where lastname='Xmen'");
		//Statement statement 
		for (Row row : results) {
			System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
		}
		
		// Delete the user from the users table
		session.execute("DELETE FROM users WHERE lastname = 'Jones'");
		// Show that the user is gone
		results = session.execute("SELECT * FROM users");
		for (Row row : results) {
		System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"),  row.getString("city"), row.getString("email"), row.getString("firstname"));
		}
		
		// Clean up the connection by closing it
		cluster.close();
	}

}
