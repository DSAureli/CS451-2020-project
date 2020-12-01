package cs451.PerfectLink;

import java.net.InetAddress;
import java.util.Objects;
import java.util.StringJoiner;

public abstract class PLRequest
{
	protected final long schedTimestamp;
	protected final long seqNum;
	protected final InetAddress address;
	protected final int port;
	
	public long getSchedTimestamp()
	{
		return schedTimestamp;
	}
	
	public long getSeqNum()
	{
		return seqNum;
	}
	
	public InetAddress getAddress()
	{
		return address;
	}
	
	public int getPort()
	{
		return port;
	}
	
	protected PLRequest(long timestamp, long seqNum, InetAddress address, int port)
	{
		this.schedTimestamp = timestamp;
		this.seqNum = seqNum;
		this.address = address;
		this.port = port;
	}
	
	protected PLRequest(PLRequest request, long newTimestamp)
	{
		this.schedTimestamp = newTimestamp;
		this.seqNum = request.seqNum;
		this.address = request.address;
		this.port = request.port;
	}
	
	public abstract PLMessage toPLMessage(int recvPort);
	
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		PLRequest that = (PLRequest) o;
		return schedTimestamp == that.schedTimestamp &&
			seqNum == that.seqNum &&
			port == that.port &&
			Objects.equals(address, that.address);
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(schedTimestamp, seqNum, address, port);
	}
	
	@Override
	public String toString()
	{
		return new StringJoiner(", ", PLRequest.class.getSimpleName() + "[", "]")
			.add("schedTimestamp=" + schedTimestamp)
			.add("seqNum=" + seqNum)
			.add("address=" + address)
			.add("port=" + port)
			.toString();
	}
}
