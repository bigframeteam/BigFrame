package bigframe.queries.BusinessIntelligence.relational.exploratory.hadoop;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import bigframe.queries.BaseTablePath;
import bigframe.queries.HiveRunnable;
import bigframe.queries.BusinessIntelligence.relational.exploratory.Q1_HiveDialect;
import bigframe.util.Constants;

public class Q1 extends Q1_HiveDialect implements HiveRunnable {
	
	
	public Q1(BaseTablePath basePath) {
		super(basePath);
		runningEngine = Constants.HIVE;
	}
	
	@Override
	public void printDescription() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepareTables(Connection connection) {
		try {
			Statement stmt = connection.createStatement();
			
			prepareBaseTable(stmt);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run(Connection connection) {
		// TODO Auto-generated method stub
		try {
			Statement stmt = connection.createStatement();
			
			runBenchQuery(stmt);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
