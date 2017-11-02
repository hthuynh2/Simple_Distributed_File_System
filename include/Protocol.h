//
//  Message.h
//  
//
//  Created by Hieu Huynh on 9/27/17.
//

#ifndef Protocol_h
#define Protocol_h
#define MAX_BUF_SIZE 1024
#include "common.h"
#include "UDP.h"

class Protocol{
private:
    //string sender;

public:
    //This is the Heart beat msg
    string create_H_msg();
    
    //This is the Join msg, which is the request to get the membership list
    string create_J_msg();
    
    //This is the New msg, to notify that there is a new node join the system
    string create_N_msg(string id_str);
    
    //This is the Leave msg, to notify that there is a  node fail
    string create_L_msg(int vm_num);
    
    //This is the Initialize msg, this contains the membership list info that VM0 send to the new node
    string create_I_msg(vector<string> vm_list, int vm_num_);
    
    //This is the Gossip msg, this contain either Q_msg, L_msg, or N_msg
	string create_G_msg(string msg, int num_retransmits);
    
    //This is a Target update msg, this msg tell the targets to update it hb targets
    string create_T_msg();
    
    //This is the Quit message, this message tell other VM that this VM is leaving
    string create_Q_msg();
    
    
    //This is the handler function of the H msg
    void handle_H_msg(string msg);
    
    //This is the handler function of the J msg
    void handle_J_msg(string msg);
    
    //This is the handler function of the N msg
    void handle_N_msg(string msg, bool haveLock);
    
    //This is the handler function of the L msg
    void handle_L_msg(string msg, bool haveLock);
    
    //This is the handler function of the I msg
    void handle_I_msg(string msg);
    
    //This is the handler function of the G msg
	void handle_G_msg(string msg, bool haveLock);
    
    //This is the handler function of the T msg
    void handle_T_msg(string msg, bool haveLock);
    
    //This is the handler function of the Q msg
    void handle_Q_msg(string msg, bool haveLock);
    
    //This funcion is used to spread the gossip
    void gossip_msg(string msg, bool haveLock);

};

#endif /* Message_h */
