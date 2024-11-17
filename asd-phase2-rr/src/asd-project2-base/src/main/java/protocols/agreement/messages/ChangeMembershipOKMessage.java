package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ChangeMembershipOKMessage extends ProtoMessage {

    public final static short MSG_ID = 196;

    private final Host newReplica;
    private final int instance;

    public ChangeMembershipOKMessage(Host newReplica, int instance) {
        super(MSG_ID);
        this.newReplica = newReplica;
        this.instance = instance;
    }

    public Host getNewReplica() {
        return newReplica;
    }

    public int getInstance() {
        return instance;
    }


    @Override
    public String toString() {
        return "ChangeMembershipOKMessage{" +
                "newReplica=" + newReplica +
                ", instance=" + instance +
                '}';
    }

    public static ISerializer<ChangeMembershipOKMessage> serializer = new ISerializer<ChangeMembershipOKMessage>() {
        @Override
        public void serialize(ChangeMembershipOKMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.newReplica, out);
            out.writeInt(msg.instance);
        }

        @Override
        public ChangeMembershipOKMessage deserialize(ByteBuf in) throws IOException {
            Host nReplica = Host.serializer.deserialize(in);
            int instance = in.readInt();
            return new ChangeMembershipOKMessage(nReplica, instance);
        }
    };

}
