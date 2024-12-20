package protocols.statemachine.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ConnectionRetryTimer extends ProtoTimer {
	public static final short TIMER_ID = 141;


	public ConnectionRetryTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}