package protocols.point2point.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class HelperTimer extends ProtoTimer {

	public static final short TIMER_ID = 405;

	public HelperTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}

}
