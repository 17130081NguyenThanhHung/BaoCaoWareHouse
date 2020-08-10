import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class loadToStaging {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:3306/control";

	// Ten nguoi dung va mat khau cua CSDL
	static final String USER = "root";
	static final String PASS = "";
	static Connection conn = null;
	static CallableStatement stmt = null;
	static ArrayList<String> listER = new ArrayList<String>();
	static Map<String, String> map = new HashMap<>();
	static SendEmail sendMail;
	private static String USER_NAME = "Datawarehousethaysong2020@gmail.com";
	private static String PASSWORD = "0964024229";
	private static String RECIPIENT = "17130081@st.hcmuaf.edu.vn, nguyenthanhhungb6@gmail.com";
	static String subject = "Thong bao ";
	static String body = " thanh cong";
	static String[] listEmail = { RECIPIENT };
	static String table_target = "";
	static String db_target = "";
	static String db_config = "";

	static void connectDb() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		System.out.println("Dang ket noi toi co so du lieu ...");
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
	}

	private static void getConfigData(int id) throws SQLException {
		// lay thong tin data trong table dataconfig
		String sql = "SELECT * FROM control.config WHERE config.id =" + id + ";";
		stmt = conn.prepareCall(sql);
		int id_config = id;
		stmt.setInt(1, id_config);
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			db_target = rs.getString(12);
			System.out.println(db_target);
			table_target = rs.getString(13);// table staging
			db_config = rs.getString(15);
		}

	}

	public static boolean writeTolog() {
		int rs = 0;
		// tao bien temp de kiem tra ghi vao db thanh cong hay khong
		boolean temp = false;
		PreparedStatement pr = null;
		// cau sql ghi vao bang log trong db
		String sql = "INSERT INTO log(id_log, name, status, typeFile, path)" + "values(?,?,?,?,?);";
		File dir = new File("E:\\GroupDW");
		// tao danh sach cac file vua tai ve
		File[] file = dir.listFiles();
		for (int i = 0; i < file.length; i++) {
			try {
				// connect voi db
				pr = conn.prepareCall(sql);
				// ghi vao cot duong dan
				pr.setString(1, file[i].getAbsolutePath());
				// ghi vao cot trang thai
				pr.setString(2, "ER");
				// loai file
				if (file[i].getName().substring(file[i].getName().lastIndexOf(".")).equals(".xlsx")) {
					pr.setString(3, "xlsx");
				} else if (file[i].getName().substring(file[i].getName().lastIndexOf(".")).equals(".txt")) {
					pr.setString(3, "txt");
				} else if (file[i].getName().substring(file[i].getName().lastIndexOf(".")).equals(".csv")) {
					pr.setString(3, "csv");
				} else if (file[i].getName().substring(file[i].getName().lastIndexOf(".")).equals(".osheet")) {
					pr.setString(3, "osheet");
				}

				rs = pr.executeUpdate();
				// ghi thanh cong thi gan temp = true
				temp = true;
			} catch (Exception e) {
				e.printStackTrace();
				return temp;
			}
		}
		return temp;
	}

	static void getFileER(int id_config) throws ClassNotFoundException, SQLException {

		String sql = "{call control.getFile_local (?)}";
		stmt = conn.prepareCall(sql);
		// Dau tien gan ket tham so IN

		stmt.setInt(1, id_config);

		// Su dung phuong thuc execute de chay stored procedure.
		System.out.println("Thuc thi stored procedure ...");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			// gan gia tri key value bang idlog va file name log
			String key = rs.getString(1);
			String value = rs.getString(2);
			map.put(key, value);
			System.out.println(key + "  " + value);

		}
	}

	static void loadToStaging() throws SQLException {
		// su dung db sinh vien
		String useDB = "use " + db_target;
		System.out.println(useDB);

		System.out.println(table_target);
		stmt.executeUpdate(useDB);
		System.out.println("Bat dau load");
		// duyet cai map lay ra các file ER o phan tren
		for (Map.Entry<String, String> entry : map.entrySet()) {
			String target = "sinhvien_stagging";
			String k = entry.getKey();
			String v = entry.getValue();
			System.out.println("Key: " + k + ", Value: " + v);
			// load file vao db
			// dung load file ko the bo vao procedure
			String load_stagging = "LOAD DATA  INFILE '" + v + "' " + "INTO TABLE " + target + ""
					+ " FIELDS TERMINATED BY '\t' " + "ENCLOSED BY '' " + "LINES TERMINATED BY '\r\n';";

			System.out.println("Dang load dong:  " + v);
			stmt.executeUpdate(load_stagging);
			System.out.println("load ok");

		}
		sendMail = new SendEmail();
		sendMail.sendFromGMail(USER_NAME, PASSWORD, listEmail, subject, body);
		System.out.println("gui email ok");
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		connectDb();
		// int id_config = Integer.parseInt(args[0]);
		getConfigData(id_config);
		getFileER(id_config);
		loadToStagging();
		writeTolog();
		System.out.println("Done!");

	}
}
