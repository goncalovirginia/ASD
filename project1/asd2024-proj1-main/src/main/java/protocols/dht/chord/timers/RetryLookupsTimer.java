package protocols.dht.chord.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class RetryLookupsTimer extends ProtoTimer {

	public static final short TIMER_ID = 501;

	public RetryLookupsTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}
