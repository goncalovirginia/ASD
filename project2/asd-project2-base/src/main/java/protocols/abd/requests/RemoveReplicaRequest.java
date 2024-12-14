package protocols.abd.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class RemoveReplicaRequest extends ProtoRequest {

    public static final short REQUEST_ID = 512;

    private final Host replica;

    public RemoveReplicaRequest(Host replica) {
        super(REQUEST_ID);
        this.replica = replica;
    }
    public Host getReplica() {
    	return replica;
    }
   

    @Override
    public String toString() {
        return "RemoveReplica{" +
                "replica=" + replica +
                '}';
    }
}
