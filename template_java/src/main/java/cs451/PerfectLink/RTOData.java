package cs451.PerfectLink;

import java.util.StringJoiner;

public class RTOData
{
	// Variables for TCP's Retransmission Timer [IETF RFC 6298]
	private final boolean firstRTT;
	private final double SRTT;
	private final double RTTVAR;
	private final long RTO;
	
	public boolean isFirstRTT()
	{
		return firstRTT;
	}
	
	public double getSRTT()
	{
		return SRTT;
	}
	
	public double getRTTVAR()
	{
		return RTTVAR;
	}
	
	public long getRTO()
	{
		return RTO;
	}
	
	public RTOData(boolean firstRTT, double SRTT, double RTTVAR, long RTO)
	{
		this.firstRTT = firstRTT;
		this.SRTT = SRTT;
		this.RTTVAR = RTTVAR;
		this.RTO = RTO;
	}
	
	@Override
	public String toString()
	{
		return new StringJoiner(", ", RTOData.class.getSimpleName() + "[", "]")
			.add("firstRTT=" + firstRTT)
			.add("SRTT=" + SRTT)
			.add("RTTVAR=" + RTTVAR)
			.add("RTO=" + RTO)
			.toString();
	}
}
