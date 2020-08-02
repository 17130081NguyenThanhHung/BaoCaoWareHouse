package Warehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;


public class LoadFile {

	String condition;

	public void LoadFile(String condition) {
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
		//Load form Nas useing SCP
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
		// Download all files starting "sinhvien"
		scp.put_SyncMustMatch("sinhvien_chieu*.*");
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return "";
		}
		System.out.println("Success!");
		ssh.Disconnect();
		return "";
	}

	public static void main(String argv[]) {
		String hostname = "drive.ecepvn.org";
		int port = 2227;
		String user = "guest_access";
		String pw = "123456";
		//Source
		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
		// Directory in local
		String localPath = "D:\\DataWareHouse"; 
		scpDownload(hostname, port, user, pw, remotePath, localPath);

	}

}
