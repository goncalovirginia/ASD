package protocols.statemachine.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class LeaderCandidateTimer extends ProtoTimer {
	public static final short TIMER_ID = 142;


	public LeaderCandidateTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}