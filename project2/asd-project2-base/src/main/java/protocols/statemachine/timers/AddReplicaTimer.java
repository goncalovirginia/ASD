package protocols.statemachine.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class AddReplicaTimer extends ProtoTimer {
	public static final short TIMER_ID = 143;


	public AddReplicaTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}