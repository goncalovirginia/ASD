package protocols.agreement;

import protocols.agreement.messages.AcceptMessage;
import protocols.agreement.messages.AcceptOKMessage;
import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.messages.ChangeMembershipMessage;
import protocols.agreement.messages.ChangeMembershipOKMessage;
import protocols.agreement.messages.LogReadMessage;
import protocols.agreement.messages.LogWriteMessage;
import protocols.agreement.messages.PrepareMessage;
import protocols.agreement.messages.PrepareOKMessage;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.MembershipChangedNotification;
import protocols.agreement.notifications.NewLeaderNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.PrepareRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.lang3.tuple.Pair;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;
import java.io.IOException;
import java.util.*;

public class PaxosClassic extends GenericProtocol {

    private static class AgreementInstanceState {
        private int acceptOkCount;
        private boolean decided;
        public AgreementInstanceState() {
            this.acceptOkCount = 0;
            this.decided = false;
        }

        public int getAcceptokCount() {
            return acceptOkCount;
        }
        public void resetAcceptOk() {
            acceptOkCount = 0;
        }
        public void incrementAcceptCount() {
            acceptOkCount ++;
        }
        public boolean decided() {
            return decided;
        }
        public void decide() {
            decided = true;
        }
    }

    private static final Logger logger = LogManager.getLogger(PaxosClassic.class);

    public final static short PROTOCOL_ID = 600;
    public final static String PROTOCOL_NAME = "ClassicPaxos";

    private Host myself;
    private int joinedInstance;
    private int prepare_ok_count;
    private List<Pair<UUID, byte[]>> prepareOkMessages;
    private int highest_prepare;
    private List<Host> membership;
    private int toBeDecidedIndex;
    private Map<Integer, AgreementInstanceState> instanceStateMap; 
    private Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages;
    private Map<Integer, Pair<UUID, byte[]>> acceptedMessages;

    private Map<Integer, Pair<Host, Boolean>> addReplicaInstances;

    public PaxosClassic(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;
        prepare_ok_count = 0;
        highest_prepare = 0;
        toBeDecidedIndex = 1; //

        toBeDecidedMessages = new TreeMap<>();
        acceptedMessages = new TreeMap<>();
        addReplicaInstances = new TreeMap<>();

        prepareOkMessages = new LinkedList<>();
        instanceStateMap = new HashMap<>();
        /*--------------------- Register Timer Handlers ----------------------------- */

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(PrepareRequest.REQUEST_ID, this::uponPrepareRequest);
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for events from the application or agreement
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOKMessage.MSG_ID, PrepareOKMessage.serializer);
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptOKMessage.MSG_ID, AcceptOKMessage.serializer);


        registerMessageSerializer(cId, LogReadMessage.MSG_ID, LogReadMessage.serializer);
        registerMessageSerializer(cId, LogWriteMessage.MSG_ID, LogWriteMessage.serializer);
        registerMessageSerializer(cId, ChangeMembershipMessage.MSG_ID, ChangeMembershipMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
              registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
              registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
              registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
              registerMessageHandler(cId, AcceptOKMessage.MSG_ID, this::uponAcceptOKMessage, this::uponMsgFail);

              registerMessageHandler(cId, LogReadMessage.MSG_ID, this::uponLogCatchUpRead, this::uponMsgFail);
              registerMessageHandler(cId, LogWriteMessage.MSG_ID, this::uponLogCatchUpWrite, this::uponMsgFail);

              registerMessageHandler(cId, ChangeMembershipMessage.MSG_ID, this::uponChangeMembershipMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    private void uponPrepareRequest(PrepareRequest request, short sourceProto) {
        prepare_ok_count = 0; 
        highest_prepare++;
        PrepareMessage msg = new PrepareMessage(highest_prepare, request.getInstance(), false);
        membership.forEach(h -> sendMessage(msg, h));  
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        if (msg.getSequenceNumber() < highest_prepare) return;

        highest_prepare = msg.getSequenceNumber();

        List<Pair<UUID, byte[]>> relevantMessages = new LinkedList<>();
        if (!myself.equals(host)) {
            relevantMessages.addAll((((TreeMap<Integer, Pair<UUID, byte[]>>) acceptedMessages)
                    .tailMap((msg.getInstance()), true)
                    .values()));
                
            triggerNotification(new NewLeaderNotification(host));
            toBeDecidedMessages = new TreeMap<>();
        }

        PrepareOKMessage prepareOK = new PrepareOKMessage(highest_prepare, relevantMessages);
        sendMessage(prepareOK, host);
    }

    private void uponPrepareOKMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        if (msg.getSequenceNumber() < highest_prepare) return;

        prepare_ok_count ++;

        if (msg.getPrepareOKMsgs().size() > prepareOkMessages.size())
            prepareOkMessages = msg.getPrepareOKMsgs();
                
        if (prepare_ok_count >= (membership.size() / 2) + 1) {
            prepare_ok_count = -1;
            triggerNotification(new NewLeaderNotification(myself, prepareOkMessages));
            prepareOkMessages = new LinkedList<>();
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {} with membership {}", notification.getJoinInstance(), membership); 
        toBeDecidedIndex = notification.getJoinInstance();
        // b4 first instance, there is no need for log catch up -- != null is a safety check
        if (notification.getContact() != null && toBeDecidedIndex > 1) 
            sendMessage(new LogReadMessage(toBeDecidedIndex), notification.getContact());
        else joinedInstance = toBeDecidedIndex;
    }

    private void uponLogCatchUpRead(LogReadMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Received Log Read: " + msg);

        Map<Integer, Pair<UUID, byte[]>> accMsgs = (((TreeMap<Integer, Pair<UUID, byte[]>>) acceptedMessages)
                                                    .tailMap(msg.getInstance(), true));

        sendMessage(new LogWriteMessage(msg.getInstance(), highest_prepare, accMsgs, toBeDecidedMessages, addReplicaInstances), host);
    }

    private void uponLogCatchUpWrite(LogWriteMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Received Log Read: " + msg);

        toBeDecidedIndex = msg.getInstance();
        joinedInstance = toBeDecidedIndex;
        highest_prepare = msg.getHighestPrepare();

        acceptedMessages = msg.getAcceptedMessages();
        toBeDecidedMessages.putAll(msg.getToBeDecidedMessages());
        addReplicaInstances = msg.getAddReplicaInstances();
         
        acceptedMessages.forEach( (k, v) -> { 
            if(!addReplicaInstances.containsKey(k))
                triggerNotification(new DecidedNotification(k, v.getLeft(), v.getRight()));
            else {
                Pair<Host, Boolean> r = addReplicaInstances.get(k);
                triggerNotification(new MembershipChangedNotification(r.getLeft(), r.getRight(), k));   
            }
            toBeDecidedIndex ++;
        }); 
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received Add Replica Request: " + request);
        membership.forEach(h -> sendMessage(
            new ChangeMembershipMessage(request.getReplica(), request.getInstance(), highest_prepare, false, true), h));
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.info("Received Remove Replica Request: " + request);
        membership.forEach(h -> {
            if(!h.equals(request.getReplica())) 
                sendMessage(new ChangeMembershipMessage(request.getReplica(), request.getInstance(), highest_prepare, false, false), h);
        });
    }
    
    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Received Propose Request: instance {} opId {}", request.getInstance(), request.getOpId());
        AcceptMessage msg = new AcceptMessage(request.getInstance(), highest_prepare, request.getOpId(), request.getOperation());
        membership.forEach(h -> sendMessage(msg, h));          
    }

    private void uponChangeMembershipMessage(ChangeMembershipMessage msg, Host host, short sourceProto, int channelId) {   
        if (msg.getSequenceNumber() < highest_prepare) return;

        highest_prepare = msg.getSequenceNumber();

        if(!msg.isAdding()) membership.remove(msg.getReplica());
        
        instanceStateMap.put(msg.getInstance(), new AgreementInstanceState());
        AcceptOKMessage m = new AcceptOKMessage(msg.getInstance(), msg.getSequenceNumber(), UUID.randomUUID(), new byte[0]);
        toBeDecidedMessages.put(msg.getInstance(), Pair.of(m.getOpId(), m.getOp()));
        addReplicaInstances.put(msg.getInstance(), Pair.of(msg.getReplica(), msg.isAdding()));
        membership.forEach(h -> sendMessage(m, h));
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        if (msg.getSequenceNumber() < highest_prepare) return;

        highest_prepare = msg.getSequenceNumber();
        toBeDecidedMessages.put(msg.getInstance(), Pair.of(msg.getOpId(), msg.getOp()));

        if (joinedInstance >= 0) {
            instanceStateMap.put(msg.getInstance(), new AgreementInstanceState()); 
            membership.forEach(h -> sendMessage(new AcceptOKMessage(msg), h));  
        } 
    }

    private void uponAcceptOKMessage(AcceptOKMessage msg, Host host, short sourceProto, int channelId) {
        AgreementInstanceState state = instanceStateMap.get(msg.getInstance());
        if (state == null) return;
        
        if (msg.getSequenceNumber() > highest_prepare)
            state.resetAcceptOk();
            
        state.incrementAcceptCount();
        if (state.getAcceptokCount() >= (membership.size() / 2) + 1 && !state.decided()) {
            state.decide();
            
            for(; toBeDecidedIndex <= msg.getInstance(); toBeDecidedIndex++) {
                Pair<UUID, byte[]> pair = toBeDecidedMessages.remove(toBeDecidedIndex);

                if(pair == null) break;

                acceptedMessages.put(toBeDecidedIndex, pair);
                if(!addReplicaInstances.containsKey(toBeDecidedIndex))
                    triggerNotification(new DecidedNotification(toBeDecidedIndex, pair.getLeft(), pair.getRight()));
                else {
                    Pair<Host, Boolean> r = addReplicaInstances.get(toBeDecidedIndex);
                    if (r.getRight()) membership.add(r.getLeft()); 

                    triggerNotification(new MembershipChangedNotification(r.getLeft(), r.getRight(), toBeDecidedIndex));   
                }
            }   
        }
    }
    
    

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.debug("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
