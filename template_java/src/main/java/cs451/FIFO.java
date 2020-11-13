package cs451;

import cs451.Helper.Pair;
import cs451.Helper.Tuple3;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class FIFO
{
	private final int id;
	
	private final ConcurrentHashMap<Integer, PriorityBlockingQueue<Pair<Integer, String>>> pendingQueueMap; // <HostId, Queue<MsgIdx, Msg>> // TODO no need for concurrent data structure
	private final ConcurrentHashMap<Integer, AtomicInteger> nextMsgIdxMap; // <HostId, MsgIdx>
	
	private final Thread deliverThread;
	
	private final UniformReliableBroadcast urb;
	private final Consumer<List<String>> deliverCallback;
	
	public FIFO(List<Host> hosts, int id, Consumer<List<String>> deliverCallback, int threadPoolSize) throws SocketException, UnknownHostException
	{
		this.id = id;
		this.pendingQueueMap = new ConcurrentHashMap<>();
		this.nextMsgIdxMap = new ConcurrentHashMap<>();
		
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
	
	private final BlockingQueue<Tuple3<Integer, Integer, String>> pendingDeliverQueue = new LinkedBlockingQueue<>(); // <HostId, MsgIdx, Msg>
	
	private class DeliverThread implements Runnable
	{
		@Override
		public void run()
		{
			while (!Thread.interrupted())
			{
				// Retrieve all pending deliveries, blocking if none available
				
//				System.out.printf("[FIFO.DeliverThread] Waiting%n");
				
				List<Tuple3<Integer, Integer, String>> retrievedList = new LinkedList<>();
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
				
				for (Tuple3<Integer, Integer, String> tuple: retrievedList)
				{
					int hostId = tuple._1();
					int msgIdx = tuple._2();
					
//					System.out.printf("[FIFO.DeliverThread] toCheckPendingQueueIdSet.add hostId: %s%n", hostId);
					toCheckPendingQueueIdSet.add(hostId);
					
					PriorityBlockingQueue<Pair<Integer, String>> pendingQueue = pendingQueueMap.get(hostId);
					pendingQueue.add(new Pair<>(msgIdx, tuple._3()));
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
	
	private static final int windowSize = 1000;
	private final Lock windowLock = new ReentrantLock();
	private final Condition windowCondition = windowLock.newCondition();
	private final AtomicInteger broadcastingCount = new AtomicInteger(windowSize);
	
	private void deliver(String msg)
	{
//		System.out.printf("[FIFO.deliver] added message to pendingDeliverQueue%n");
//		System.out.printf("[deliver] msg: %s, broadcastingCount: %d%n", msg, broadcastingCount.get());
		
		String[] msgParts = msg.split(" ");
		int hostId = Integer.parseInt(msgParts[0]);
		int msgIdx = Integer.parseInt(msgParts[1]);
		
		try
		{
			pendingDeliverQueue.put(new Tuple3<>(hostId, msgIdx, msg));
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		if (hostId == id)
		{
			windowLock.lock();
			broadcastingCount.getAndIncrement();
			windowCondition.signal();
			windowLock.unlock();
		}
	}
	
	public void broadcast(String msg) throws InterruptedException
	{
//		System.out.printf("[broadcast] broadcastingCount: %d%n", broadcastingCount.get());
		
		windowLock.lock();
		if (broadcastingCount.get() < 1)
			windowCondition.await();
		broadcastingCount.getAndDecrement();
		windowLock.unlock();
		
		urb.broadcast(msg);
	}
	
	public void close()
	{
		urb.close();
		deliverThread.interrupt();
	}
}
