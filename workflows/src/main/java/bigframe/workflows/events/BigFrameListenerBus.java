/**
 * 
 */
package bigframe.workflows.events;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mayuresh
 *
 */
public class BigFrameListenerBus {

	List<BigFrameListener> listeners = new ArrayList<BigFrameListener>();
	
	public synchronized void addListener(BigFrameListener listener) {
		listeners.add(listener);
	}
	
	public synchronized void postToAll(BigFrameListenerEvent event) {
		for(BigFrameListener listener: listeners) {
			if(event.getClass().getName().equals(
					WorkflowStartedEvent.class.getName())) {
				listener.onWorkflowStarted((WorkflowStartedEvent) event);
				continue;
			}
			if(event.getClass().getName().equals(
					WorkflowCompletedEvent.class.getName())) {
				listener.onWorkflowCompleted((WorkflowCompletedEvent) event);
				continue;
			}
			if(event.getClass().getName().equals(
					ComponentStartedEvent.class.getName())) {
				listener.onComponentStarted((ComponentStartedEvent) event);
				continue;
			}
			if(event.getClass().getName().equals(
					ComponentCompletedEvent.class.getName())) {
				listener.onComponentCompleted((ComponentCompletedEvent) event);
				continue;
			}
			if(event.getClass().getName().equals(
					QueryStartedEvent.class.getName())) {
				listener.onQueryStarted((QueryStartedEvent) event);
				continue;
			}
			if(event.getClass().getName().equals(
					QueryCompletedEvent.class.getName())) {
				listener.onQueryCompleted((QueryCompletedEvent) event);
				continue;
			}
		}
	}
}
