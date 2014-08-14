/**
 * 
 */
package bigframe.workflows.events;

/**
 * @author mayuresh
 *
 */
public class QueryCompletedEvent implements BigFrameListenerEvent {

	String queryString;
	String engine;

	public QueryCompletedEvent(String queryString, String engine) {
		this.queryString = queryString;
		this.engine = engine;
	}

	/**
	 * @return the queryString
	 */
	public String getQueryString() {
		return queryString;
	}

	/**
	 * @param queryString the queryString to set
	 */
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	/**
	 * @return the engine
	 */
	public String getEngine() {
		return engine;
	}

	/**
	 * @param engine the engine to set
	 */
	public void setEngine(String engine) {
		this.engine = engine;
	}
}
