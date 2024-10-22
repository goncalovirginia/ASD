package protocols.point2point.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ResendMessagesTimer extends ProtoTimer {

	public static final short TIMER_ID = 401;

	public ResendMessagesTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}

}
