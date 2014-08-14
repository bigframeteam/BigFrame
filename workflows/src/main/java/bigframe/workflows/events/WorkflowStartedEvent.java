/**
 * 
 */
package bigframe.workflows.events;

/**
 * @author mayuresh
 *
 */
public class WorkflowStartedEvent implements BigFrameListenerEvent {

	String name;
	String userName;

	public WorkflowStartedEvent(String name) {
		this(name, "biguser");
	}
	
	public WorkflowStartedEvent(String name, String userName) {
		this.name = name;
		this.userName = userName;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @param userName the userName to set
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}
}
