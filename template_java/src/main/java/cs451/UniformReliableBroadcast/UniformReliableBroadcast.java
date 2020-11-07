package cs451.UniformReliableBroadcast;

import cs451.Host;

import java.io.NotSerializableException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class UniformReliableBroadcast
{
	// Majority-Ack Uniform Reliable Broadcast
	
	private final Set<String> deliveredSet = ConcurrentHashMap.newKeySet();
	private final Set<String> forwardedSet = ConcurrentHashMap.newKeySet();
	private final ConcurrentHashMap<String, Set<Integer>> ackMap = new ConcurrentHashMap<>();
	
	private BestEffortBroadcast bestEffortBroadcast;
	private int hostsCount;
	private int id;
	Consumer<String> deliverCallback;
	
	public UniformReliableBroadcast(Host self, List<Host> targetHosts, Consumer<String> deliverCallback) throws SocketException, UnknownHostException
	{
		this.bestEffortBroadcast = new BestEffortBroadcast(self, targetHosts, this::deliver);
		this.hostsCount = targetHosts.size() + 1;
		this.id = self.getId();
		this.deliverCallback = deliverCallback;
//		System.out.printf("Hosts count: %s%n", hostsCount);
	}
	
	public void broadcast(String msg)
	{
		forwardedSet.add(msg);
		
		URBMessage urbMessage = new URBMessage(id, msg);
		bestEffortBroadcast.broadcast(urbMessage.toString());
	}
	
	private void deliver(String urbMessageString)
	{
		// Deserialize message
		URBMessage urbMessage;
		try
		{
			urbMessage = URBMessage.fromString(urbMessageString);
		}
		catch (NotSerializableException e)
		{
			e.printStackTrace();
			return;
		}
		
		String msg = urbMessage.getMessage();
//		System.out.printf("hash[msg]: %s%n", msg.hashCode());
//		System.out.print("chars[msg]: ");
//		msg.chars().forEach(ch -> System.out.printf("%s", Integer.toHexString(ch)));
//		System.out.printf("%n");
//		System.out.printf("ackMap[%s] = %s%n", msg, ackMap.get(msg));
		
		// Add sender to ack[msg]

//		synchronized (UniformReliableBroadcast.class)
//		{
////			System.out.printf("ackMap[%s] = %s%n", msg, ackMap.get(msg));
//			if (!ackMap.containsKey(msg))
//			{
////				System.out.printf("Init map for: %s%n", msg);
//				ackMap.put(msg, ConcurrentHashMap.newKeySet());
//			}
//		}
		ackMap.putIfAbsent(msg, ConcurrentHashMap.newKeySet());
		ackMap.get(msg).add(urbMessage.getSender());
//		System.out.printf("ackMap[%s] = %s%n", msg, ackMap.get(msg));
		
		// Forward message
		if (!forwardedSet.contains(msg))
		{
//			System.out.printf("Forwarding: %s%n", urbMessage.toString());
			forwardedSet.add(msg);
			
			URBMessage fwdURBMessage = new URBMessage(id, msg);
			bestEffortBroadcast.broadcast(fwdURBMessage.toString());
		}

//		System.out.printf("Checking: %s %s%n", msg, ackMap.get(msg));
		
		// Check for delivery
		if (ackMap.get(msg).size() > hostsCount / 2 && !deliveredSet.contains(msg))
		{
//			System.out.printf("Good check%n");
			deliveredSet.add(msg);
			deliverCallback.accept(msg);
		}

//		System.out.printf("ackMap[%s] = %s%n", msg, ackMap.get(msg));
	}
}