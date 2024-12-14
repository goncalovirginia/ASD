package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class AddReplicaMessage extends ProtoMessage {

    public final static short MSG_ID = 525;

    private final Host newReplica;
    private final Host contact;
    private final int instance;

    public AddReplicaMessage(Host newReplica, int instance, Host contact) {
        super(MSG_ID);
        this.newReplica = newReplica;
        this.instance = instance;
        this.contact = contact;
    }

    public Host getNewReplica() {
        return newReplica;
    }

    public Host getContact() {
        return contact;
    }

    public int getInstance() {
        return instance;
    }

    @Override
    public String toString() {
        return "AddReplicaMessage{" +
                "newReplica=" + newReplica +
                ", instance=" + instance +
                ", contact=" + contact +
                '}';                
    }

    public static ISerializer<AddReplicaMessage> serializer = new ISerializer<AddReplicaMessage>() {
        @Override
        public void serialize(AddReplicaMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.newReplica, out);
            out.writeInt(msg.instance);
            Host.serializer.serialize(msg.contact, out);
        }

        @Override
        public AddReplicaMessage deserialize(ByteBuf in) throws IOException {
            Host nReplica = Host.serializer.deserialize(in);
            int c = in.readInt();
            Host cReplica = Host.serializer.deserialize(in);

            return new AddReplicaMessage(nReplica, c, cReplica);
        }
    };

}
