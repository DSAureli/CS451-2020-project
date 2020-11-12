package cs451.PerfectLink;

import java.util.Objects;

// TODO move up
public class Pair<T1,T2>
{
	protected final T1 _1;
	protected final T2 _2;
	
	public T1 _1()
	{
		return _1;
	}
	
	public T2 _2()
	{
		return _2;
	}
	
	public Pair(T1 _1, T2 _2)
	{
		this._1 = _1;
		this._2 = _2;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Pair<?,?> pair = (Pair<?,?>) o;
		return Objects.equals(_1, pair._1) &&
			Objects.equals(_2, pair._2);
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(_1, _2);
	}
	
	@Override
	public String toString()
	{
		return "Pair{" +
			"_1=" + _1 +
			", _2=" + _2 +
			'}';
	}
}

