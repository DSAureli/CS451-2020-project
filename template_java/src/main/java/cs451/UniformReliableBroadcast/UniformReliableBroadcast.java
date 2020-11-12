package cs451.UniformReliableBroadcast;

import cs451.Host;

import java.io.NotSerializableException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class UniformReliableBroadcast
{
	// Majority-Ack Uniform Reliable Broadcast
	
	private final Set<String> deliveredSet = ConcurrentHashMap.newKeySet();
	private final Set<String> forwardedSet = ConcurrentHashMap.newKeySet();
	private final ConcurrentHashMap<String, Set<Integer>> ackMap = new ConcurrentHashMap<>();
	
	private final BestEffortBroadcast bestEffortBroadcast;
	private final int hostsCount;
	private final int id;
	Consumer<String> deliverCallback;
	
	public UniformReliableBroadcast(Host self,
	                                List<Host> targetHosts,
	                                Consumer<String> deliverCallback,
	                                ExecutorService recvThreadPool) throws SocketException, UnknownHostException
	{
		this.bestEffortBroadcast = new BestEffortBroadcast(self, targetHosts, this::deliver, recvThreadPool);
		this.hostsCount = targetHosts.size() + 1;
		this.id = self.getId();
		this.deliverCallback = deliverCallback;
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
		
		// Add sender to ack[msg]
		ackMap.putIfAbsent(msg, ConcurrentHashMap.newKeySet());
		ackMap.get(msg).add(urbMessage.getSender());
		
		// Forward message
		if (!forwardedSet.contains(msg))
		{
			forwardedSet.add(msg);
			
			URBMessage fwdURBMessage = new URBMessage(id, msg);
			bestEffortBroadcast.broadcast(fwdURBMessage.toString());
		}
		
		// Check for delivery
		if (ackMap.get(msg).size() > hostsCount / 2 && !deliveredSet.contains(msg))
		{
			deliveredSet.add(msg);
			deliverCallback.accept(msg);
		}
	}
}
	
	public void close()
	{
		bestEffortBroadcast.close();
	}
}