package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

public class ChangeMembershipMessage extends ProtoMessage {

    public final static short MSG_ID = 192;

    private final Host newReplica;
    private final int instance;
    private final boolean ok;

    public ChangeMembershipMessage(Host newReplica, int instance, boolean ok) {
        super(MSG_ID);
        this.newReplica = newReplica;
        this.instance = instance;
        this.ok = ok;
    }

    public Host getNewReplica() {
        return newReplica;
    }

    public int getInstance() {
        return instance;
    }

    public boolean isOK() {
        return ok;
    }

    @Override
    public String toString() {
        return "ChangeMembershipMessage{" +
                "newReplica=" + newReplica +
                ", instance=" + instance +
                '}';
    }

    public static ISerializer<ChangeMembershipMessage> serializer = new ISerializer<ChangeMembershipMessage>() {
        @Override
        public void serialize(ChangeMembershipMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.newReplica, out);
            out.writeInt(msg.instance);
            out.writeBoolean(msg.ok);
        }

        @Override
        public ChangeMembershipMessage deserialize(ByteBuf in) throws IOException {
            Host nReplica = Host.serializer.deserialize(in);
            int instance = in.readInt();
            boolean ok = in.readBoolean();
            return new ChangeMembershipMessage(nReplica, instance, ok);
        }
    };

}
