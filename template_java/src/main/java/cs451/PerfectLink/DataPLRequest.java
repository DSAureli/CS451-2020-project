package cs451.PerfectLink;

import java.net.InetAddress;
import java.util.Objects;
import java.util.StringJoiner;

public class DataPLRequest extends PLRequest
{
	private final String data;
	
	public DataPLRequest(long timestamp, long seqNum, InetAddress address, int port, String data)
	{
		super(timestamp, seqNum, address, port);
		this.data = data;
	}
	
	public DataPLRequest(DataPLRequest request, long newTimestamp)
	{
		super(request, newTimestamp);
		this.data = request.data;
	}
	
	@Override
	public PLMessage toPLMessage(int recvPort)
	{
		return new PLMessage(PLMessage.PLMessageType.Data, seqNum,
		                     System.currentTimeMillis(), recvPort,
		                     data == null ? 0 : data.length(), data);
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		if (!super.equals(o))
			return false;
		DataPLRequest that = (DataPLRequest) o;
		return Objects.equals(data, that.data);
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(super.hashCode(), data);
	}
	
	@Override
	public String toString()
	{
		return new StringJoiner(", ", DataPLRequest.class.getSimpleName() + "[", "]")
			.add("schedTimestamp=" + schedTimestamp)
			.add("seqNum=" + seqNum)
			.add("address=" + address)
			.add("port=" + port)
			.add("data='" + data + "'")
			.toString();
	}
}
