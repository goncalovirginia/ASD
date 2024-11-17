package protocols.agreement.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import org.apache.commons.codec.binary.Hex;

import java.util.UUID;

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
