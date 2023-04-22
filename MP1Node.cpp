/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/*
 * Helper utilities
 */

/**
 * FUNCTION NAME: compareMemberToId
 * 
 * DESCRIPTION: A comparator used to determine the correct position to insert a
 *              new node into a member list
 */
bool compareMemberToId(MemberListEntry entry, int id) {
    return entry.getid() < id;
}

/**
 * FUNCTION NAME: logMemberList
 * 
 * DESCRIPTION: Logs a given member list, providing id, heartbeat, and timestamp
 */
void logMemberList(Log *log, Address *addr, vector<MemberListEntry> *list) {
    string msg = "[";
    for (auto it = list->begin(); it != list->end(); it++) {
        msg += to_string(it->getid()) + ": " + to_string(it->getheartbeat()) + 
               "(" + to_string(it->gettimestamp()) + "), ";
    }
    msg += "]";
    log->LOG(addr, msg.c_str());
}

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    // Add self to member list
    memberNode->memberList.emplace_back(id, port, memberNode->heartbeat, par->getcurrtime());
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
	MessageHdr *msg = (MessageHdr *)data;
    Address *source = (Address *)(msg + 1);

    switch(msg->msgType) {
        case JOINREQ: {
            // Add the new node to the member list
            long heartbeat = *(long *)data;
            int id;
            short port;
            memcpy(&id, &source->addr[0], sizeof(int));
            memcpy(&port, &source->addr[4], sizeof(short));

            auto it = lower_bound(memberNode->memberList.begin(), memberNode->memberList.end(), id, compareMemberToId);
            // If the node already exists, update its heartbeat
            // Note: this shouldn't be the case, but it's better to be safe
            if (it != memberNode->memberList.end() && it->getid() == id) it->setheartbeat(heartbeat);

            memberNode->memberList.emplace(it, id, port, heartbeat, par->getcurrtime());

            // Send back a JOINREP message, indicating successful introduction
            // Include the member list for them to copy

            size_t repsize = gossipMsgSize();
            MessageHdr *joinrep = createGossipMsg(repsize);
            joinrep->msgType = JOINREP;

#ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Sending JOINREP");
#endif
            emulNet->ENsend(&memberNode->addr, source, (char *)joinrep, repsize);
            break;
        }
        case JOINREP: {
            memberNode->inGroup = true;
            // Copy the introducer's member list (no break runs the GOSSIP case)
        }
        case GOSSIP: {
            updateMemberList(data);
            break;
        }
        default: {
#ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Received an unknown message");
#endif       
        }
    }
#ifdef DEBUGLOG
    logMemberList(log, &memberNode->addr, &memberNode->memberList);
#endif
}

/**
 * FUNCTION NAME: updateMemberList 
 * 
 * DESCRIPTION: Extract an incoming member list from message data.
 *              Update own member list with new entries, and the latest heartbeat
 *              information for every member.
 */
void MP1Node::updateMemberList(char *data) {
    int n;
    memcpy(&n, data + sizeof(MessageHdr), sizeof(int));
    
    auto it = memberNode->memberList.begin();
    int offset = sizeof(int);
    for (int i = 0; i < n; i++) {
        int id;
        short port;
        long heartbeat, timestamp;

        memcpy(&id, data + sizeof(MessageHdr) + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&port, data + sizeof(MessageHdr) + offset, sizeof(short));
        offset += sizeof(short);
        memcpy(&heartbeat, data + sizeof(MessageHdr) + offset, sizeof(long));
        offset += sizeof(long);
        memcpy(&timestamp, data + sizeof(MessageHdr) + offset, sizeof(long));
        offset += sizeof(long);

        /* It is unknown where incoming entries should be inserted, since
            * there are existing entries already. 
            * Fortunately, member lists are sorted by id.
            * 
            * it = lower_bound(it) guarantees that the iterator only moves forward
            * without the need to restart from the beginning. 
        */
        it = lower_bound(it, memberNode->memberList.end(), id, compareMemberToId);

        if (id == it->getid()) {
            if (heartbeat > it->getheartbeat()) {
                it->setheartbeat(heartbeat);
                it->settimestamp(par->getcurrtime());
            }
            else {} // Ignore existing up-to-date entry
        } else {
            /*
             * The incoming entry is guaranteed to have a smaller id than 
             * what the iterator is pointing at. This means that the member
             * hasn't been added to this node's member list.
             * Iterator must be refreshed after inserting a new element
             */
            it = memberNode->memberList.emplace(it, id, port, heartbeat, par->getcurrtime());    
        }
    }
}

/**
 * FUNCTION NAME: createGossipMsg
 *
 * DESCRIPTION: Creates a message with the member list attached as data.
 */
MessageHdr * MP1Node::createGossipMsg(size_t msgsize) {
    int n = memberNode->memberList.size();
    
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));

    memcpy((char *)(msg + 1), &n, sizeof(int));

    int offset = sizeof(int);
    for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
        memcpy((char *)(msg + 1) + offset, &it->id, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)(msg + 1) + offset, &it->port, sizeof(short));
        offset += sizeof(short);
        memcpy((char *)(msg + 1) + offset, &it->heartbeat, sizeof(long));
        offset += sizeof(long);
        memcpy((char *)(msg + 1) + offset, &it->timestamp, sizeof(long));
        offset += sizeof(long);
    }
    return msg;
}

/**
 * FUNCTION NAME: gossipMsgSize
 *
 * DESCRIPTION: Returns the size of a message with a member list attached.
 */
size_t MP1Node::gossipMsgSize() {
    int n = memberNode->memberList.size();
    size_t listsize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + (n * listsize);

    return msgsize;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    if (memberNode->pingCounter == 0) {
        int selfId;
        memcpy(&selfId, &memberNode->addr.addr[0], sizeof(int));

        // Update the heartbeat of self in the member list too
        auto self = lower_bound(memberNode->memberList.begin(), memberNode->memberList.end(), selfId, compareMemberToId);
        self->setheartbeat(++memberNode->heartbeat);

        size_t msgsize = gossipMsgSize();
        MessageHdr *gossipmsg = createGossipMsg(msgsize);
        gossipmsg->msgType = GOSSIP;
        for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
            if (it->getid() != selfId) {
                Address toaddr(to_string(it->getid()) + ":" + to_string(it->getport()));
#ifdef DEBUGLOG
                log->LOG(&memberNode->addr, "Sending gossip");
#endif
                emulNet->ENsend(&memberNode->addr, &toaddr, (char *)gossipmsg, msgsize);
            }
        }

        memberNode->pingCounter = TFAIL;
    } else {
        memberNode->pingCounter--;
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
