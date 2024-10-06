package protocols.apps.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class SendMessageTimer extends ProtoTimer {
	public static final short TIMER_ID = 301;

	public SendMessageTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}
