import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbc {
	private static String driver = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection connect = DriverManager.getConnection("jdbc:hive2://111.222.333.444:10000/hive_db;",
				"hive_user", "password");

		Statement state = connect.createStatement();
		String tableName = "test";
		state.executeQuery("drop table " + tableName);
		System.out.println("table droped");
		ResultSet res = state.executeQuery("create table " + tableName + " (key int, value string)");

	}
}
