package protocols.agreement;

import protocols.agreement.messages.AcceptMessage;
import protocols.agreement.messages.AcceptOKMessage;
import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.messages.ChangeMembershipMessage;
import protocols.agreement.messages.ChangeMembershipOKMessage;
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

import protocols.abd.utils.Tag;

import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;

import java.io.IOException;
import java.util.*;

public class ClassicPaxos extends GenericProtocol {

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

    private static final Logger logger = LogManager.getLogger(ClassicPaxos.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 600;
    public final static String PROTOCOL_NAME = "ClassicPaxos";

    private Host myself;
    private int joinedInstance;
    private int prepare_ok_count;
    private List<Pair<UUID, byte[]>> prepareOkMessages;
    private Tag highest_prepare;
    private List<Host> membership;
    private int toBeDecidedIndex;
    private Map<Integer, AgreementInstanceState> instanceStateMap; 
    private Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages;
    private Map<Integer, Pair<UUID, byte[]>> acceptedMessages;

    private Map<Integer, Pair<Host, Boolean>> addReplicaInstances;

    public ClassicPaxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;
        prepare_ok_count = 0;
        highest_prepare = new Tag(-1, -1);
        toBeDecidedIndex = 0; //

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

        registerMessageSerializer(cId, ChangeMembershipMessage.MSG_ID, ChangeMembershipMessage.serializer);
        registerMessageSerializer(cId, ChangeMembershipOKMessage.MSG_ID, ChangeMembershipOKMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
              registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
              registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
              registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
              registerMessageHandler(cId, AcceptOKMessage.MSG_ID, this::uponAcceptOKMessage, this::uponMsgFail);

              registerMessageHandler(cId, ChangeMembershipMessage.MSG_ID, this::uponChangeMembershipMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    //highest joinedInstance wins
    private void uponPrepareRequest(PrepareRequest request, short sourceProto) {
        prepare_ok_count = 0; 
        Tag newTag = new Tag(request.getInstance() + 1, joinedInstance);
        PrepareMessage msg = new PrepareMessage(newTag, false);
        membership.forEach(h -> sendMessage(msg, h));  
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            if(msg.getSeqNumber().greaterOrEqualThan(highest_prepare)) {
                highest_prepare = msg.getSeqNumber();

                List<Pair<UUID, byte[]>> relevantMessages = new LinkedList<>();
                if(!myself.equals(host)) {
                    int n = highest_prepare.getOpSeq() -1;
                    relevantMessages.addAll((((TreeMap<Integer, Pair<UUID, byte[]>>) acceptedMessages)
                                            .tailMap((n), true)
                                            .values()));
                    
                    triggerNotification(new NewLeaderNotification(host));
                    toBeDecidedMessages = new TreeMap<>();
                }   

                PrepareOKMessage prepareOK = new PrepareOKMessage(highest_prepare, relevantMessages);
                sendMessage(prepareOK, host);
            }
        }
    }

    //Prepare_OK messages with accepted values for any instance >= n.
    private void uponPrepareOKMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        if (msg.getSeqNumber().greaterOrEqualThan(highest_prepare)) {
            prepare_ok_count ++;
            if (msg.getPrepareOKMsgs().size() > prepareOkMessages.size())
                prepareOkMessages = msg.getPrepareOKMsgs();
                
            if (prepare_ok_count >= (membership.size() / 2) + 1) {
                prepare_ok_count = -1;

                triggerNotification(new NewLeaderNotification(myself, prepareOkMessages));
                prepareOkMessages = new LinkedList<>();
            }
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  process {}, membership {}", toBeDecidedIndex, joinedInstance, membership); 

        toBeDecidedMessages.forEach((k, v) -> {
            if(k <= joinedInstance) {
                toBeDecidedIndex ++;
                acceptedMessages.put(k, v);
                if (!addReplicaInstances.containsKey(k)) 
                    triggerNotification(new DecidedNotification(k, v.getLeft(), v.getRight()));
                else {
                    Pair<Host, Boolean> h = addReplicaInstances.get(k);
                    if (!h.getLeft().equals(myself))
                        triggerNotification(new MembershipChangedNotification(h.getLeft(), h.getRight(), k));
                }   
            } else 
                membership.forEach(h -> sendMessage(new AcceptOKMessage(k, v.getLeft(), v.getRight(),k-1), h)); 
        });

    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received Add Replica Request: " + request);
        
        membership.forEach(h -> sendMessage(
            new ChangeMembershipMessage(request.getReplica(), request.getInstance(), false, true), h));
    }

    //TODO
    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received Remove Replica Request: " + request);
        membership.remove(request.getReplica()); 
        if(joinedInstance > request.getInstance())
            joinedInstance--;

        instanceStateMap.put(0, new AgreementInstanceState());
        membership.forEach(h -> 
            sendMessage(new ChangeMembershipMessage(request.getReplica(), request.getInstance(), false, false), h));
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.info("Received Propose Request: instance {} opId {}", request.getInstance(), request.getOpId());

        AcceptMessage msg = new AcceptMessage(request.getInstance(), highest_prepare, request.getOpId(), request.getOperation(), request.getInstance() - 1);
        membership.forEach(h -> sendMessage(msg, h));          
    }

    private void uponChangeMembershipMessage(ChangeMembershipMessage msg, Host host, short sourceProto, int channelId) {   
        if (msg.isOK()) {
            toBeDecidedIndex = msg.getInstance();
            if(addReplicaInstances.size() < msg.AddReplicaInstancesSize())
                addReplicaInstances = msg.getAddReplicaInstances();

            toBeDecidedMessages.putAll(msg.getToBeDecidedMessages());
            return;
        }
        
        membership.add(msg.getReplica());
        triggerNotification(new MembershipChangedNotification(msg.getReplica(), true, msg.getInstance())); 

        instanceStateMap.put(msg.getInstance(), new AgreementInstanceState());
        AcceptOKMessage m = new AcceptOKMessage(msg.getInstance(), UUID.randomUUID(), new byte[0], msg.getInstance() -1);
        toBeDecidedMessages.put(msg.getInstance(), Pair.of(m.getOpId(), m.getOp()));
        addReplicaInstances.put(msg.getInstance(), Pair.of(msg.getReplica(), true));
        membership.forEach(h -> sendMessage(m, h));
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        if( !(msg.getSeqNumber()).greaterOrEqualThan(highest_prepare))
            return;
        highest_prepare = msg.getSeqNumber();

        toBeDecidedMessages.put(msg.getInstance(), Pair.of(msg.getOpId(), msg.getOp()));
        if(joinedInstance >= 0) {
            instanceStateMap.put(msg.getInstance(), new AgreementInstanceState()); 
            membership.forEach(h -> sendMessage(
                new AcceptOKMessage(msg.getInstance(), msg.getOpId(), msg.getOp(), msg.getLastChosen()), h));  
        } 
    }

    private void uponAcceptOKMessage(AcceptOKMessage msg, Host host, short sourceProto, int channelId) {
        AgreementInstanceState state = instanceStateMap.get(msg.getInstance());
        if (state == null) return;
        
        /* if(msg.getSeqNumber().greaterThan(highest_prepare)
            state.resetAcceptOk(); */
            
        state.incrementAcceptCount();
        if (state.getAcceptokCount() >= (membership.size() / 2) + 1 && !state.decided()) {
            state.decide();
            
            for(; toBeDecidedIndex <= msg.getLastFound(); toBeDecidedIndex++) {
                Pair<UUID, byte[]> pair = toBeDecidedMessages.remove(toBeDecidedIndex);
                if(pair != null) {
                    acceptedMessages.put(toBeDecidedIndex, pair);

                    if(!addReplicaInstances.containsKey(toBeDecidedIndex)) {
                        triggerNotification(
                            new DecidedNotification(toBeDecidedIndex, pair.getLeft(), pair.getRight()));
                    } else {
                        int n = toBeDecidedIndex -1;
                        Map<Integer, Pair<UUID, byte[]>> relevantMessages = new TreeMap<>();
                        relevantMessages.putAll(((TreeMap<Integer, Pair<UUID, byte[]>>) acceptedMessages).tailMap(n, true));
                        relevantMessages.putAll(((TreeMap<Integer, Pair<UUID, byte[]>>) toBeDecidedMessages).tailMap(n, true));
                        Pair<Host, Boolean> newReplica = addReplicaInstances.get(toBeDecidedIndex);                     
                        sendMessage(new ChangeMembershipMessage(
                            newReplica.getLeft(), n, true, true, relevantMessages, addReplicaInstances), newReplica.getLeft());

                    }
                } 
            }   
        }
    }
    
    

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.debug("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
