# Network Membership Protocol

Network failure detection and message dissemination via gossip protocol multicasting.  
In a distributed system, failure among members is fairly common. Implemented here is a protocol that accepts new members, as well as the protocol at each member. 
Each member has a list of active members, and they use the protocol to determine whether others have failed. 
The protocol processes at each member run independently of each other, so member lists are almost never exactly the same. 

The goal is that when a member fails, all other members eventually realize what happened. And preferably, "eventually" meaning quickly. 

The gossip multicast is used to share member lists to other members, and this is used to update their lists. 
Eventually, a failed node will be phased out from all members' lists because it hasn't notified others that it's still active.

Assignment for [Cloud Computing Concepts](https://www.coursera.org/learn/cloud-computing), offered by the University of Illinois at Urbana-Champaign.
