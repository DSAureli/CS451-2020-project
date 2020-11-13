package cs451;

import cs451.Helper.Pair;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class FIFO
{
	private final ConcurrentHashMap<Integer, PriorityBlockingQueue<Pair<Integer, String>>> pendingQueueMap; // <HostId, Queue<MsgIdx, Msg>>
	private final ConcurrentHashMap<Integer, AtomicInteger> nextMsgIdxMap; // <HostId, MsgIdx>
	
	private final Thread deliverThread;
	
	private final UniformReliableBroadcast urb;
	private final Consumer<List<String>> deliverCallback;
	
	public FIFO(List<Host> hosts, int id, Consumer<List<String>> deliverCallback, int threadPoolSize) throws SocketException, UnknownHostException
	{
		pendingQueueMap = new ConcurrentHashMap<>();
		nextMsgIdxMap = new ConcurrentHashMap<>();
		
		for (Host host: hosts)
		{
			pendingQueueMap.put(host.getId(), new PriorityBlockingQueue<>(1, Comparator.comparingInt(Pair::_1)));
			nextMsgIdxMap.put(host.getId(), new AtomicInteger(1));
		}
		
		List<Host> targetHosts = hosts;
		Host self = targetHosts.stream().filter(host -> host.getId() == id).findFirst().get();
		targetHosts.remove(self);
		
		this.deliverThread = new Thread(new DeliverThread());
		this.deliverThread.start();
		
		this.urb = new UniformReliableBroadcast(self, targetHosts, this::deliver, threadPoolSize);
		this.deliverCallback = deliverCallback;
	}
	
	private final BlockingQueue<String> pendingDeliverQueue = new PriorityBlockingQueue<>();
	
	private class DeliverThread implements Runnable
	{
		@Override
		public void run()
		{
			while (!Thread.interrupted())
			{
				// Retrieve all pending deliveries, blocking if none available
				
//				System.out.printf("[FIFO.DeliverThread] Waiting%n");
				
				List<String> retrievedList = new LinkedList<>();
				try
				{
					retrievedList.add(pendingDeliverQueue.take());
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					Thread.currentThread().interrupt();
				}
				pendingDeliverQueue.drainTo(retrievedList);
				
//				System.out.printf("[FIFO.DeliverThread] retrievedList: %s%n", retrievedList);
				
				// Pending queues that get changed and thus require a check for deliveries
				Set<Integer> toCheckPendingQueueIdSet = new HashSet<>();
				
				for (String msg: retrievedList)
				{
					String[] msgParts = msg.split(" ");
					int hostId = Integer.parseInt(msgParts[0]);
					int msgIdx = Integer.parseInt(msgParts[1]);
					
//					System.out.printf("[FIFO.DeliverThread] toCheckPendingQueueIdSet.add hostId: %s%n", hostId);
					toCheckPendingQueueIdSet.add(hostId);
					
					PriorityBlockingQueue<Pair<Integer, String>> pendingQueue = pendingQueueMap.get(hostId);
					pendingQueue.add(new Pair<>(msgIdx, msg));
				}
				
//				System.out.printf("[FIFO.DeliverThread] toCheckPendingQueueIdSet: %s%n", toCheckPendingQueueIdSet);
				
				// All messages to be delivered by FIFO
				List<String> toDeliverList = new ArrayList<>();
				
				for (Integer hostId: toCheckPendingQueueIdSet)
				{
					PriorityBlockingQueue<Pair<Integer, String>> pendingQueue = pendingQueueMap.get(hostId);
					
					while (!pendingQueue.isEmpty() && pendingQueue.peek()._1().equals(nextMsgIdxMap.get(hostId).get()))
					{
						toDeliverList.add(pendingQueue.poll()._2());
						nextMsgIdxMap.get(hostId).getAndIncrement();
					}
				}
				
				deliverCallback.accept(toDeliverList);
			}
		}
	}
	
	private void deliver(String msg)
	{
//		System.out.printf("[FIFO.deliver] added message to pendingDeliverQueue%n");
		pendingDeliverQueue.add(msg);
	}
	
	// TODO careful not to decrement current window size less than 0
	
	public void broadcast(String msg)
	{
		urb.broadcast(msg);
	}
	
	public void close()
	{
		urb.close();
		deliverThread.interrupt();
	}
}
