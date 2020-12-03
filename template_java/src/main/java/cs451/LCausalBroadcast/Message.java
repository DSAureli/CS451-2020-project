package cs451.LCausalBroadcast;

public class Message
{
	private final int host;
	private final int idx;
	
	public int getHost()
	{
		return host;
	}
	
	public int getIdx()
	{
		return idx;
	}
	
	public Message(int host, int idx)
	{
		this.host = host;
		this.idx = idx;
	}
	
	@Override
	public String toString()
	{
		return String.format("%s %s", host, idx);
	}
}
