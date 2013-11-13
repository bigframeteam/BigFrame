
package bigframe.util.dataloader.hana;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import bigframe.bigif.WorkflowInputFormat;

public class HanaGraphLoader extends HanaDataLoader {

	public HanaGraphLoader(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean createTable() {
		
		String create_graph = "create column table twitter_graph( " +
				"	friend_id int, " +
				"	follower_id int " +
				")";
		
		try {
			Statement stmt = connection.createStatement();
			stmt.execute(create_graph);
		} catch (SQLException e) {
			System.out.println("Please make sure to drop tables before load the data. (twitter_graph))");
			return false;
		}
		return true;
	}

	@Override
	public boolean load() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean preProcess(String srcHdfsPath) {
		// TODO Auto-generated method stub
		return false;
	}

}
