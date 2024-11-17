package protocols.agreement.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class PrepareRequest extends ProtoRequest {

	public static final short REQUEST_ID = 121;

	private final int instance;

	public PrepareRequest(int instance) {
		super(REQUEST_ID);
		this.instance = instance;
	}

	public int getInstance() {
		return instance;
	}


	@Override
	public String toString() {
		return "PrepareRequest{" +
				"instance=" + instance +
				'}';
	}
}
