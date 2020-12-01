package cs451.PerfectLink;

import java.net.InetAddress;
import java.util.Objects;
import java.util.StringJoiner;

// TODO divide behaviours into two subclasses
public class PLRequest
{
	private final long schedTimestamp;
	
	private final long seqNum;
	private final PLMessage.PLMessageType type;
	private final InetAddress address;
	private final int port;
	
	private String data;
	private long msgSendTimestamp;
	
	public long getSchedTimestamp()
	{
		return schedTimestamp;
	}
	
	public long getSeqNum()
	{
		return seqNum;
	}
	
	public PLMessage.PLMessageType getType()
	{
		return type;
	}
	
	public InetAddress getAddress()
	{
		return address;
	}
	
	public int getPort()
	{
		return port;
	}
	
	public String getData()
	{
		return data;
	}
	
	private PLRequest(long timestamp, long seqNum, PLMessage.PLMessageType type, InetAddress address, int port, String data, long msgSendTimestamp)
	{
		this.schedTimestamp = timestamp;
		this.seqNum = seqNum;
		this.type = type;
		this.address = address;
		this.port = port;
		this.data = data;
		this.msgSendTimestamp = msgSendTimestamp;
	}
	
	public static PLRequest newNormalRequest(long timestamp, long seqNum, InetAddress address, int port, String data)
	{
		return new PLRequest(timestamp, seqNum, PLMessage.PLMessageType.Normal, address, port, data, 0);
	}
	
	public static PLRequest newACKRequest(long timestamp, long seqNum, InetAddress address, int port, long msgSendTimestamp)
	{
		return new PLRequest(timestamp, seqNum, PLMessage.PLMessageType.ACK, address, port, null, msgSendTimestamp);
	}

	public PLRequest(PLRequest request, long newTimestamp)
	{
		this.schedTimestamp = newTimestamp;
		this.seqNum = request.seqNum;
		this.type = request.type;
		this.address = request.address;
		this.port = request.port;
		this.data = request.data;
		this.msgSendTimestamp = request.msgSendTimestamp;
	}
	
	public PLMessage toNormalPLMessage(int recvPort, long msgSendTimestamp)
	{
		return new PLMessage(type, seqNum, msgSendTimestamp, recvPort, data == null ? 0 : data.length(), data);
	}
	
	public PLMessage toACKPLMessage(int recvPort)
	{
		return new PLMessage(type, seqNum, msgSendTimestamp, recvPort, data == null ? 0 : data.length(), data);
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		PLRequest plRequest = (PLRequest) o;
		return schedTimestamp == plRequest.schedTimestamp &&
			seqNum == plRequest.seqNum &&
			port == plRequest.port &&
			type == plRequest.type &&
			Objects.equals(address, plRequest.address) &&
			Objects.equals(data, plRequest.data);
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(schedTimestamp, seqNum, type, address, port, data);
	}
	
	@Override
	public String toString()
	{
		return new StringJoiner(", ", PLRequest.class.getSimpleName() + "[", "]")
			.add("timestamp=" + schedTimestamp)
			.add("seqNum=" + seqNum)
			.add("type=" + type)
			.add("address=" + address)
			.add("port=" + port)
			.add(data == null ? "data=null" : ("data='" + data + "'"))
			.toString();
	}
}
