package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class AcceptMessage extends ProtoMessage {

    public final static short MSG_ID = 198;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final Host replica;
    private final int replicaInstance;

    private final boolean changingMembership;
    private final boolean adding;
    private final int lastChosen;

    public AcceptMessage(int instance, UUID opId, byte[] op, int last) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.changingMembership = false;
        this.replica = null;
        replicaInstance = -1;
        this.adding = false;
        this.lastChosen = last;
    }

    public AcceptMessage(int instance, Host host, int replicaInstance, boolean adding) {
        super(MSG_ID);
        this.instance = instance;
        this.op = new byte[0];
        this.opId = UUID.randomUUID();
        this.changingMembership = true;
        this.replica = host;
        this.replicaInstance = replicaInstance;
        this.adding = adding;
        this.lastChosen = 0;
    }
    
    public int getInstance() {
        return instance;
    }

    public int getLastChosen() {
        return lastChosen;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }

    public Host getReplica() {
        return replica;
    }

    public int getReplicaInstance() {
        return replicaInstance;
    }

    public boolean isAddOrRemoving() {
        return changingMembership;
    }

    public boolean isAdding() {
        return adding;
    }

    public boolean isRemoving() {
        return (!adding && changingMembership);
    }

    @Override
    public String toString() {
        if (!changingMembership) {
            return "AcceptMessage{" +
                    "opId=" + opId +
                    ", instance=" + instance +
                    ", op=" + Hex.encodeHexString(op) +
                    '}';
        } else {
            return "AcceptAddRemoveMessage{" +
                    ", instance=" + instance +
                    ", newReplica=" + replica +
                    '}';
        }
    }

    public static ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instance);
            out.writeBoolean(msg.changingMembership);

            if (msg.changingMembership) {
                Host.serializer.serialize(msg.replica, out);
                out.writeInt(msg.replicaInstance);
                out.writeBoolean(msg.adding);
            } else {
                out.writeLong(msg.opId.getMostSignificantBits());
                out.writeLong(msg.opId.getLeastSignificantBits());
                out.writeInt(msg.op.length);
                out.writeBytes(msg.op);
                out.writeInt(msg.lastChosen);
            }   
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            boolean isReplicaOp = in.readBoolean();

            if (isReplicaOp) {
                Host nReplica = Host.serializer.deserialize(in);
                int nReplicaInstance = in.readInt();
                boolean add = in.readBoolean();
                return new AcceptMessage(instance, nReplica, nReplicaInstance, add);    
            } else {
                long highBytes = in.readLong();
                long lowBytes = in.readLong();
                UUID opId = new UUID(highBytes, lowBytes);
                byte[] op = new byte[in.readInt()];
                in.readBytes(op);
                int last = in.readInt();
                return new AcceptMessage(instance, opId, op, last);
            } 
        }
    };

}
