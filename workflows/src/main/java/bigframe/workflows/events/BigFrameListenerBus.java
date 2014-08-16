/**
 * 
 */
package bigframe.workflows.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * @author mayuresh
 *
 */
public class BigFrameListenerBus {

	static final Integer EVENT_QUEUE_CAPACITY = 10000;
	LinkedBlockingQueue<BigFrameListenerEvent> eventQueue = 
			new LinkedBlockingQueue<BigFrameListenerEvent>(EVENT_QUEUE_CAPACITY);
	Boolean started = false;
	// A counter that represents the number of events produced and consumed in the queue
	Semaphore eventLock = new Semaphore(0);
	Thread listenerThread = new Thread(new Runnable() {
		
		@Override
		public void run() {
			while(true) {
				try {
					eventLock.acquire();
					synchronized (BigFrameListenerBus.this) {
						BigFrameListenerEvent event = eventQueue.poll();
						if(event.getClass().getName().equals(
								ListenerShutdownEvent.class.getName())) {
							return;
						}
						postToAll(event);
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}, "BigFrame Listener");

	List<BigFrameListener> listeners = Collections.synchronizedList(
			new ArrayList<BigFrameListener>());
	
	public void addListener(BigFrameListener listener) {
		listeners.add(listener);
	}

	public void start() {
		if(started) {
			System.out.println("Already running!");
			return;
		}
		started = true;
		listenerThread.start();
	}
	
	public void stop() {
		if(!started) {
			System.out.println("Not running!");
			return;
		}
		post(new ListenerShutdownEvent());
		try {
			listenerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void post(BigFrameListenerEvent event) {
		if(eventQueue.offer(event)) {
			eventLock.release();
		} else {
			System.out.println("Queue full!");
		}
	}

	void postToAll(BigFrameListenerEvent event) {
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
