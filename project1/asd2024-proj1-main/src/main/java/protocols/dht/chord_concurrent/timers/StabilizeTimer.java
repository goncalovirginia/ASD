package protocols.dht.chord_concurrent.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class StabilizeTimer extends ProtoTimer {

	public static final short TIMER_ID = 502;

	public StabilizeTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}

}
