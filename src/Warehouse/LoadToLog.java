package Warehouse;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import Warehouse.ConnectDB;
public class LoadToLog {
	 
	public static void main(String[] args) throws SQLException {
		try{
		String myDriver = "com.mysql.jdbc.Driver";
		String myUrl = "jdbc:mysql://localhost/warehouse";
		Class.forName(myDriver);
		Connection conn = DriverManager.getConnection(myUrl, "root", "");
		
		String sql="INSERT INTO log (name,status,typeFile,path)"+ "values(?,?,?,?);";
		
		 File dir = new File("D:\\DataWareHouse");
	     File[] children = dir.listFiles();
	     for (File file : children) {
	    	 PreparedStatement preparedStmt = conn.prepareStatement(sql);
	    	 preparedStmt.setString (1, file.getName());
	    	 preparedStmt.setString (2, "ER");
	    	 if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".xlsx")) {
	    		 preparedStmt.setString(3, "xlsx");
				} else if (file.getName().substring(file.getName().lastIndexOf("."))
						.equals(".txt")) {
					preparedStmt.setString(3, "txt");
				} else {
					preparedStmt.setString(3, "kb");
				}
	         preparedStmt.setString (4, file.getAbsolutePath());
	         preparedStmt.execute();
	     }
	     
	     }
	     catch (Exception e){
	     }
		 System.err.println("Load to log success!");
	}
}
