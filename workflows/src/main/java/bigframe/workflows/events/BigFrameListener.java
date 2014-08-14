/**
 * 
 */
package bigframe.workflows.events;

/**
 * Any listener to BigFrame events need to implement this interface
 * @author mayuresh
 *
 */
public interface BigFrameListener {

	/**
	 * Submitted when any workflow starts execution
	 * @param event
	 */
	public void onWorkflowStarted(WorkflowStartedEvent event);

	/**
	 * Submitted when any workflow finishes execution
	 * @param event
	 */
	public void onWorkflowCompleted(WorkflowCompletedEvent event);

	/**
	 * Submitted when any component of workflow begins execution
	 * @param event
	 */
	public void onComponentStarted(ComponentStartedEvent event);

	/**
	 * Submitted when any component of workflow finishes execution
	 * @param event
	 */
	public void onComponentCompleted(ComponentCompletedEvent event);

	/**
	 * Submitted when any query within a component begins execution
	 * @param event
	 */
	public void onQueryStarted(QueryStartedEvent event);

	/**
	 * Submitted when any query within a component finishes execution
	 * @param event
	 */
	public void onQueryCompleted(QueryCompletedEvent event);
}
