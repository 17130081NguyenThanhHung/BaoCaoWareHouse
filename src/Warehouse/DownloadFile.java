package Warehouse;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import java.util.*;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class DownloadFile {

	String condition;

	public void DownloadFile(String condition) {
		this.condition = condition;
	}

	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}
	public static String scpDownload(String hostname, int port, String user, String pw, String remotePath,
			String localPath) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("hello ");
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return "";
		}
		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw(user, pw);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return "";
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return "";
		}
		// Download tat ca cac file bat dau bang "sinhvien"
		scp.put_SyncMustMatch("sinhvien_chieu*.*");
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return "";
		}

		ssh.Disconnect();
		return "";

	}

// phuong thuc load to log
	public static boolean log() {
		boolean check = false;

		try {
			// connect database
			String myDriver = "com.mysql.jdbc.Driver";
			String myUrl = "jdbc:mysql://localhost/warehouse";
			Class.forName(myDriver);
			Connection conn = DriverManager.getConnection(myUrl, "root", "");

			// insert to table log
			String sql = "INSERT INTO log (name,status,typeFile,path)" + "values(?,?,?,?);";
			File dir = new File("D:\\DataWareHouse");
			File[] children = dir.listFiles();
			for (File file : children) {
				PreparedStatement preparedStmt = conn.prepareStatement(sql);
//				preparedStmt.setString(5, file.());
				// tên file
				preparedStmt.setString(1, file.getName());
				// trạng thái
				preparedStmt.setString(2, "ER");
				// type file
				if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".xlsx")) {
					preparedStmt.setString(3, "xlsx");
				} else if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".txt")) {
					preparedStmt.setString(3, "txt");
				} else if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".csv")) {
					preparedStmt.setString(3, "csv");
				} else if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".osheet")) {
					preparedStmt.setString(3, "osheet");
				} else {
					preparedStmt.setString(3, "kb");
				}
				// file path
				preparedStmt.setString(4, file.getAbsolutePath());

				preparedStmt.executeUpdate();
				check = true;
			}
			conn.close();
		} catch (Exception e) {
			return check;
		}
		System.out.println("Load đến log thành công!");
		return check;
	}

// phương thức send mail
	public static boolean sendMail(String to, String subject, String bodyMail) {
		Properties props = new Properties();
		// Cấu hình mail server
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");
		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication("Datawarehousethaysong2020@gmail.com", "0964024229");
			}
		});
		// Used to debug SMTP issues
//		session.setDebug(true);
		try {
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);
			// Set From: header field of the header.
			message.setHeader("Content-Type", "text/plain; charset=UTF-8");
			// Set To: header field of the header.
			message.setFrom(new InternetAddress("Datawarehousethaysong2020@gmail.com"));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
			// Set Subject: header field
			message.setSubject(subject, "UTF-8");
			// Now set the actual message
			message.setText(bodyMail, "UTF-8");
			// Send message
			Transport.send(message);
		} catch (MessagingException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

// phương thức send mail
	public static void sendMailLog() {
		if (log() != true) {// nếu điều kiện không đúng thì sẽ thực hiện sendMail báo lỗi
			sendMail("17130081@st.hcmuaf.edu.vn", "DATA WAREHOUSE Nhóm 10 - 2020", "Download file khong thanh cong! ");
			System.out.println("Gửi mail thông báo: Download bị lỗi!");
		} else {// nếu điều kiện đúng thì sẽ thực hiện sendMail báo thành công
			sendMail("17130081@st.hcmuaf.edu.vn", "DATA WAREHOUSE - 2020", "Downoad file thanh cong! ");
			System.out.println("Gửi mail thông báo: Download thành công!");
		}
	}

	public static void main(String argv[]) {
//		String hostname = "drive.ecepvn.org";
//		int port = 2227;
//		String user = "guest_access";
//		String pw = "123456";
//		// source: Địa chỉ file trên Nas của Thầy
//		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
//		// Thư mục download file trên Nas về
//		String localPath = "D:\\DataWareHouse";
////		scpDownload(hostname, port, user, pw, remotePath, localPath);
////		log();0
		sendMailLog();
	}

}
