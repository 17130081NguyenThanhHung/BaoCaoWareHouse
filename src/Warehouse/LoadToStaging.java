package Warehouse;

import java.sql.SQLException;
import java.sql.Statement;

public class LoadToStaging {
	private void loadToStagging(String jdbcURL, String username, String password) throws SQLException {
		ConnectDB connect = new ConnectDB();
		connect.connectDatabase(jdbcURL, username, password);
		Statement stmt = connect.connection.createStatement();
		String sql="select filenamesrc, IDlog from log where statusend='ER'";
		stmt.execute(sql);
		
		String sql1="use datacontrol";
		stmt.executeUpdate(sql1);
		String sqlER="update log set statusend='TR' where IDlog=''";
		stmt.executeUpdate(sqlER);		
		
	}

}