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

import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;

import java.io.IOException;
import java.util.*;

public class PaxosAgreement extends GenericProtocol {

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

    private static final Logger logger = LogManager.getLogger(PaxosAgreement.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Agreement";

    private Host myself;
    private Host newLeader;
    private int joinedInstance;
    private int prepare_ok_count;
    private int highest_prepare;
    private int proposer_seq_number;
    private List<Host> membership;

    private int lastChosen;
    private int lastToBeDecided;

    private Map<Integer, AgreementInstanceState> instanceStateMap; 

    private Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages;
    private Map<Integer, Pair<UUID, byte[]>> executedMessages;


    public PaxosAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;
        prepare_ok_count = 0;
        
        highest_prepare = -1;
        proposer_seq_number = -1;
        
        
        lastChosen = 0; //
        lastToBeDecided = 1; //

        toBeDecidedMessages = new TreeMap<>();
        executedMessages = new TreeMap<>();


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
              registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
              registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
              registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
              registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
              registerMessageHandler(cId, AcceptOKMessage.MSG_ID, this::uponAcceptOKMessage, this::uponMsgFail);

              registerMessageHandler(cId, ChangeMembershipMessage.MSG_ID, this::uponChangeMembershipMessage, this::uponMsgFail);
              registerMessageHandler(cId, ChangeMembershipOKMessage.MSG_ID, this::uponChangeMembershipOKMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    //TO DELETE AFTER DEALING WITH COMMENTS
    private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            //Obviously your agreement protocols will not decide things as soon as you receive the first message
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    //highest joinedInstance wins
    private void uponPrepareRequest(PrepareRequest request, short sourceProto) {
        prepare_ok_count = 0; //this probably needs to be an actual set, for the edge case mentioned in the slides, we'll see
        proposer_seq_number = request.getInstance() + joinedInstance;
        PrepareMessage msg = new PrepareMessage(proposer_seq_number);
        membership.forEach(h -> sendMessage(msg, h));  
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            if(msg.getInstance() > highest_prepare) {
                highest_prepare = msg.getInstance();
                PrepareOKMessage prepareOK = new PrepareOKMessage(msg.getInstance());
                sendMessage(prepareOK, host);

                if(host.equals(newLeader)) {
                    triggerNotification(new NewLeaderNotification(host));
                    highest_prepare--;
                }
                newLeader = host;
            }
        } else {
            //TODO: uponBroadcast above comments
        }
    }

    private void uponPrepareOKMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        if (proposer_seq_number == msg.getInstance() && proposer_seq_number >= highest_prepare) {
            prepare_ok_count ++;
            if (prepare_ok_count >= (membership.size() / 2) + 1) {
                prepare_ok_count = -1;

                triggerNotification(new NewLeaderNotification(myself));
                membership.forEach(h -> {
                    if (!h.equals(myself))
                        sendMessage(new PrepareMessage(proposer_seq_number+1), h);
                });
            }
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        //The joining instances are sequential, the initial membership is 1,2,3,etc...
        //so in the joining proccess, we should take that into account.
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  processSequence: {}, membership: {}", lastToBeDecided, joinedInstance, membership); 
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        instanceStateMap.put(0, new AgreementInstanceState()); //an instance that bothers no one.
        logger.debug("Received Add Replica Request: " + request);
        
        sendMessage(new ChangeMembershipMessage(request.getReplica(), request.getInstance(), true), request.getReplica());

        membership.forEach(h -> 
            sendMessage(new ChangeMembershipMessage(request.getReplica(), request.getInstance(), false), h));
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        instanceStateMap.putIfAbsent(request.getInstance(), new AgreementInstanceState());
        AcceptMessage msg = new AcceptMessage(request.getInstance(), request.getOpId(), request.getOperation(), lastChosen);
        membership.forEach(h -> sendMessage(msg, h));          
    }

    //TODO - INCORPORATE THIS IN ACCEPT ? MAYBE NOT BECAUSE MESSAGES ARE TOO DIFFERENT? NO OPID/DATA?
    //MAYBE NOT, BUT DO REUSE THIS FOR REMOVE
    private void uponChangeMembershipMessage(ChangeMembershipMessage msg, Host host, short sourceProto, int channelId) {        
        if (msg.isOK()) {
            if(msg.getNewReplica().equals(myself)) {
                lastToBeDecided = msg.getInstance();
                return;
            }

            membership.add(msg.getNewReplica());
            triggerNotification(new MembershipChangedNotification(msg.getNewReplica(), true, channelId));
            return;
        }
        
        ChangeMembershipOKMessage acceptOK = new ChangeMembershipOKMessage(msg.getNewReplica(), msg.getInstance());
        sendMessage(acceptOK, host);
    }

    private void uponChangeMembershipOKMessage(ChangeMembershipOKMessage msg, Host host, short sourceProto, int channelId) {        
        AgreementInstanceState state = instanceStateMap.get(0);
        if (state != null) {
            state.incrementAcceptCount();
            if (state.getAcceptokCount() >= (membership.size() / 2) + 1 && !state.decided()) {
                state.decide();

                membership.forEach(h ->  { 
                    if (!h.equals(myself)) {
                            sendMessage(new ChangeMembershipMessage(msg.getNewReplica(), msg.getInstance(), true), h);
                        }
                    });

                membership.add(msg.getNewReplica());
                triggerNotification(new MembershipChangedNotification(msg.getNewReplica(), true, channelId));              
            }
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {   
        if (!host.equals(myself)) {

            boolean found = false;
            if (joinedInstance >= 0) {
                for(; lastToBeDecided <= msg.getLastChosen(); lastToBeDecided++) {
                    Pair<UUID, byte[]> pair = toBeDecidedMessages.remove(lastToBeDecided);
                    if(pair != null) {
                        if (lastToBeDecided == msg.getInstance())
                            found = true;
                        
                        Pair<UUID, byte[]> val = executedMessages.putIfAbsent(lastToBeDecided, pair);
                        if(val == null)
                            triggerNotification(new DecidedNotification(lastToBeDecided, pair.getLeft(), pair.getRight()));
                    } else {
                        logger.info("I should be here requesting the difference.");
                        AcceptOKMessage m = new AcceptOKMessage(lastToBeDecided, msg.getOpId(), new byte[0], lastToBeDecided);
                        sendMessage(m, host);
                        return;
                    }
                }
            }
            if (found) { //no point in bothering the leader anymore
                return;  
            }
            toBeDecidedMessages.putIfAbsent(msg.getInstance(), Pair.of(msg.getOpId(), msg.getOp()));
        }
        
        AcceptOKMessage acceptOK = new AcceptOKMessage(msg);
        sendMessage(acceptOK, host);
    }

    private void uponAcceptOKMessage(AcceptOKMessage msg, Host host, short sourceProto, int channelId) {
        AgreementInstanceState state = instanceStateMap.get(msg.getInstance());
        if (state != null) {
            if(msg.getMissingIndex() != -1) {
                Pair<UUID, byte[]> pair = executedMessages.get(lastToBeDecided);
                sendMessage(new AcceptMessage(lastToBeDecided, pair.getLeft(), pair.getRight(), lastChosen), host);
                return;
            }

            state.incrementAcceptCount();
            if (state.getAcceptokCount() >= (membership.size() / 2) + 1 && !state.decided()) {
                state.decide();
                triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));

                lastChosen = msg.getInstance();
                executedMessages.putIfAbsent(msg.getInstance(), Pair.of(msg.getOpId(), msg.getOp()));
                membership.forEach(h -> 
                    sendMessage(new AcceptMessage(msg.getInstance(), msg.getOpId(), msg.getOp(), lastChosen), h));
                          
            }
        }
    }

    
    
    //Change Message instead, dunno if addReplica is added to Log  of State Machine
    
    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.remove(request.getReplica());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        //logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
