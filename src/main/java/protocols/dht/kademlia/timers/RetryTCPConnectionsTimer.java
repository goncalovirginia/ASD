package protocols.dht.kademlia.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class RetryTCPConnectionsTimer extends ProtoTimer {

	public static final short TIMER_ID = 501;

	public RetryTCPConnectionsTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}
