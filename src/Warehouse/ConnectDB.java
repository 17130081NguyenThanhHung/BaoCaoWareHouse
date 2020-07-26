package Warehouse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.mysql.jdbc.PreparedStatement;

public class ConnectDB {
	PreparedStatement statement = null;
	Connection connection = null;
	void connectDatabase(String jdbcURL,String username ,String password) throws SQLException {
		connection = DriverManager.getConnection(jdbcURL, username, password);
		System.out.println("Connect success");
	}
	public static void main(String[] args) throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306";
		String username = "root";
		String password = "";
		ConnectDB cn= new ConnectDB();
		cn.connectDatabase(jdbcURL, username, password);
	}
}
