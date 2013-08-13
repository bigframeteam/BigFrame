package bigframe.queries;

public class BaseTablePath {
	
	private String relational_path = "";
	private String graph_path = "";
	private String nested_path = "";
	
	public BaseTablePath() {
		
	}
	
	public void setRelationalPath(String r_path) {
		relational_path = r_path;
	}
	
	public void setGraphPath(String g_path) {
		graph_path = g_path;
	}
	
	public void setNestedPath(String n_path) {
		nested_path = n_path;
	}
	
	public String getRelationalPath() {
		return relational_path;
	}
	
	public String getGraphPath() {
		return graph_path;
	}
	
	public String getNestedPath() {
		return nested_path;
	}
	
}
