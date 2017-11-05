import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class HiveUDF extends UDF {

	private Text result = new Text();
	public Text evaluate(Text str) {
		if (str == null) {
			return null;
		}
		try {
			result.set(Sha256.hash256(str.toString()));
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Exception : Some problem in hashing ..");
			e.printStackTrace();
		}
		return result;
	}
}

