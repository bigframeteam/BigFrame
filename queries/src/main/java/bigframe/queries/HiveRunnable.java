package bigframe.queries;

import java.sql.Connection;

public interface HiveRunnable {
	
	public void prepareTables(Connection connection);
	
	public void run(Connection connection);
}
