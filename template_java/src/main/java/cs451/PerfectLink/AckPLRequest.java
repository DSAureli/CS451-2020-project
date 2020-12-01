package cs451.PerfectLink;

import java.net.InetAddress;
import java.util.Objects;
import java.util.StringJoiner;

public class AckPLRequest extends PLRequest
{
	private final long msgSendTimestamp;
	
	protected AckPLRequest(long timestamp, long seqNum, InetAddress address, int port, long msgSendTimestamp)
	{
		super(timestamp, seqNum, address, port);
		this.msgSendTimestamp = msgSendTimestamp;
	}
	
	protected AckPLRequest(AckPLRequest request, long newTimestamp)
	{
		super(request, newTimestamp);
		this.msgSendTimestamp = request.msgSendTimestamp;
	}
	
	@Override
	public PLMessage toPLMessage(int recvPort)
	{
		return new PLMessage(PLMessage.PLMessageType.Ack, seqNum,
		                     msgSendTimestamp, recvPort,
		                     0, null);
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
		AckPLRequest that = (AckPLRequest) o;
		return msgSendTimestamp == that.msgSendTimestamp;
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(super.hashCode(), msgSendTimestamp);
	}
	
	@Override
	public String toString()
	{
		return new StringJoiner(", ", AckPLRequest.class.getSimpleName() + "[", "]")
			.add("schedTimestamp=" + schedTimestamp)
			.add("seqNum=" + seqNum)
			.add("address=" + address)
			.add("port=" + port)
			.add("msgSendTimestamp=" + msgSendTimestamp)
			.toString();
	}
}
