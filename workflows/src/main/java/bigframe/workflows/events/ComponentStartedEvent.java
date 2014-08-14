/**
 * 
 */
package bigframe.workflows.events;

/**
 * @author mayuresh
 *
 */
public class ComponentStartedEvent implements BigFrameListenerEvent {

	String name;
	String engine;

	public ComponentStartedEvent(String name, String engine) {
		this.name = name;
		this.engine = engine;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
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
