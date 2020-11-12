package cs451;

import cs451.PerfectLink.Pair;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class FIFO
{
	private final ConcurrentHashMap<Integer, PriorityBlockingQueue<Pair<Integer, String>>> queueMap; // <HostId, Queue<MsgIdx, Msg>>
	private final ConcurrentHashMap<Integer, AtomicInteger> nextMsgIdxMap; // <HostId, MsgIdx>
	private final UniformReliableBroadcast urb;
	
	private final Consumer<List<String>> deliverCallback;
	
	public FIFO(List<Host> hosts,
	            int id,
	            Consumer<List<String>> deliverCallback,
	            int threadPoolSize) throws SocketException, UnknownHostException
	{
		queueMap = new ConcurrentHashMap<>();
		nextMsgIdxMap = new ConcurrentHashMap<>();
		
		hosts.forEach(host -> {
			queueMap.put(host.getId(), new PriorityBlockingQueue<>(1, Comparator.comparingInt(Pair::_1)));
			nextMsgIdxMap.put(host.getId(), new AtomicInteger(1));
		});
		
		List<Host> targetHosts = hosts;
		Host self = targetHosts.stream().filter(host -> host.getId() == id).findFirst().get();
		targetHosts.remove(self);
		
		urb = new UniformReliableBroadcast(self, targetHosts, this::deliver, threadPoolSize);
		
		this.deliverCallback = deliverCallback;
	}
	
	// TODO move to thread, signal each time a deliver happens
	// TODO also keep a boolean indicating if a signal was sent during last execution of the thread
	// TODO (in order to avoid that the last deliver is ignored because the thread is running for the second-to-last)
	private void deliver(String msg)
	{
//		System.out.printf("[%s]%n", msg);
		synchronized (FIFO.class)
		{
			String[] msgParts = msg.split(" ");
			int hostId = Integer.parseInt(msgParts[0]);
			int msgIdx = Integer.parseInt(msgParts[1]);
			
			PriorityBlockingQueue<Pair<Integer, String>> queue = queueMap.get(hostId);
			queue.add(new Pair<>(msgIdx, msg));
			
			List<String> toDeliverList = new ArrayList<>();
		
		
			while (!queue.isEmpty() && queue.peek()._1().equals(nextMsgIdxMap.get(hostId).get()))
			{
				toDeliverList.add(queue.poll()._2());
				nextMsgIdxMap.get(hostId).getAndIncrement();
			}
		
		
		// TODO with the thread this should perform a little better?
		// TODO (no two possible polls from two threads, the second of which may fail)
//		while (!queue.isEmpty())
//		{
//			synchronized (FIFO.class)
//			{
//				if (!queue.peek()._1().equals(nextMsgIdxMap.get(hostId)))
//					break;
//
//				toDeliverList.add(queue.poll()._2());
//				nextMsgIdxMap.put(hostId, msgIdx + 1);
//			}
//		}
		
			deliverCallback.accept(toDeliverList);
		}
	}
	
	public void broadcast(String msg)
	{
		urb.broadcast(msg);
	}
	
	public void close()
	{
		urb.close();
	}
}
