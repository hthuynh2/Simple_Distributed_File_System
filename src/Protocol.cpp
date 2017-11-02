//
//  Message.cpp
//  
//
//  Created by Hieu Huynh on 9/27/17.
//

#include <stdio.h>
#include "Protocol.h"

#include <sstream>
#include <random>
#include <algorithm>	// for min

#include "logging.h"

extern membership::Logger::Handle protocol_log;

void send_gossip_helper(vector<string> msg, bool haveLock);


/*This function create the H_msg with format: | H | VM_num | \n |
 *Input:    None
 *Output:   HB msg
 */
string Protocol::create_H_msg(){
    string msg("H");
    msg.append(int_to_string(my_vm_info.vm_num));
    msg.append("\n");
    return msg;
}

/*This function create the J_msg with format: | J | ip_address | timestamp | \n|
 *Input:    None
 *Output:   J msg
 */
string Protocol::create_J_msg(){
    string msg("J");
    for(int i = 0 ; i < IP_LEN; i++){
        msg.push_back((unsigned char) my_vm_info.ip_addr[i]);
    }
    msg.append(my_vm_info.time_stamp);
    msg.append("\n");
    return msg;
}


/*This function create the N_msg with format: | N | VM_id | \n |
 *Input:    None
 *Output:   N msg
 */
string Protocol::create_N_msg(string id_str) {
    string msg = "N" + id_str + "\n";
    return msg;
}

/*This function create the L_msg with format: | L | VM_num | \n |
 *Input:    None
 *Output:   HB msg
 */
string Protocol::create_L_msg(int vm_num) {
    string msg("L");
    msg.append(int_to_string(vm_num));
    msg.append("\n");
    return msg;
}

/*This function create the I_msg with format: | I | vm_num | size of membership list (i) | VM0 | VM1 | ... |VMi| \n |
 *Input:    None
 *Output:   I msg
 */
string Protocol::create_I_msg(vector<string> vm_list, int vm_num_){
    string msg("I");
    msg.append(int_to_string(vm_num_));
    msg.append(int_to_string((int)vm_list.size()));
    
    for(int i = 0 ; i < (int)vm_list.size(); i++){
        msg.append(vm_list[i]);
    }
    
    msg.append("\n");
    return msg;
}

/*This function create the G_msg with format: | G | rtt | msg | \n |
 *Input:    None
 *Output:   G msg
 */
string Protocol::create_G_msg(string msg, int num_retransmits) {
	ostringstream ss;
	ss << "G" << num_retransmits << msg;
	return ss.str();
}

/*This function create the T_msg with format: | G | vm_num | \n |
 *Input:    None
 *Output:   T msg
 */
string Protocol::create_T_msg(){
    string msg("T");
    msg.append(int_to_string(my_vm_info.vm_num));
    msg.push_back('\n');
    return msg;
}


/*This function create the Q_msg with format: | Q | vm_num | \n |
 *Input:    None
 *Output:   Q msg
 */
string Protocol::create_Q_msg(){
    string msg("Q");
    msg.append(int_to_string(my_vm_info.vm_num));
    msg.append("\n");
    return msg;
}



/* This function handle H_msg and update HB of HB targets
 *input:    msg: H_msg
 *output:   NONE
 */
void Protocol::handle_H_msg(string msg){
    membership_list_lock.lock();
    hb_targets_lock.lock();

    string sender_id_str = msg.substr(1,2);
    int sender_id = string_to_int(sender_id_str);
    time_t cur_time;
    cur_time = time (NULL);
    //Update the H_msg of the HB
    if(hb_targets.find(sender_id) != hb_targets.end() && vm_info_map.find(sender_id) != vm_info_map.end()){
        vm_info_map[sender_id].heartbeat = (long) cur_time;
    }

    hb_targets_lock.unlock();
    membership_list_lock.unlock();
    return;
}


/* This function handle J_msg and update membershiplist, send gossip and response with I msg
 *input:    msg: J_msg
 *output:   NONE
 */
void Protocol::handle_J_msg(string msg){
    membership_list_lock.lock();
    unsigned char sender_ip[4];
    string sender_time_stamp;
    
    
    //Get data from msg
    for(int i= 0 ; i < IP_LEN; i++){
        sender_ip[i] = msg[1+i];
    }
    string sender_ip_str("");
    for(int i = 0 ; i < IP_LEN; i++){
        sender_ip_str.append(to_string((unsigned int) sender_ip[i]));
        if(i != IP_LEN -1){
            sender_ip_str.push_back('.');
        }
    }
    sender_time_stamp = msg.substr(5, 10);
    bool is_in_ML = false;
    
    //Determined if this is a duplicate request
	int sender_id =-1;
    for(auto it = vm_info_map.begin(); it != vm_info_map.end(); it++){
        if(strcmp(it->second.time_stamp.c_str(), sender_time_stamp.c_str()) == 0
           && strcmp(it->second.ip_addr_str.c_str(), sender_ip_str.c_str()) == 0 ){
            is_in_ML = true;
		sender_id = it->second.vm_num;
        }
    }
    

    //Update membershiplist
    if(is_in_ML == false){
        for(int i = 0 ; i < (int)membership_list.size() +1; i++){
            if(membership_list.find(i) != membership_list.end()){
                continue;
            }
            sender_id = i;
            break;
        }
        membership_list.insert(sender_id);
        VM_info new_vm(sender_id, sender_ip, sender_time_stamp);
        vm_info_map[sender_id] = new_vm;
        cout <<"VM with id: " << new_vm.vm_num << " JOIN.";
        protocol_log << "VM with id: " << new_vm.vm_num <<= " JOIN.";
        print_membership_list();
        
        //Gossip the msg
        Protocol p;
        p.gossip_msg(p.create_N_msg(new_vm.id_str), true);
    }
    
    vector<string> vm_list;
    
    //Create I msg to reponse to new VM
    for(auto it = vm_info_map.begin(); it != vm_info_map.end(); it++){
        vm_list.push_back(it->second.id_str);
    }
    string i_msg = create_I_msg(vm_list, sender_id);
    
    //Send I msg to new VM
    UDP local_udp;
    string ip_addr_str("");
    for(int i = 0; i < IP_LEN; i++){
        int num = (int) sender_ip[i];
        ip_addr_str.append(to_string(num));
        if(i != IP_LEN-1)
            ip_addr_str.push_back('.');
    }
    local_udp.send_msg(ip_addr_str, i_msg);
    
    //Update HB targets
    if(is_in_ML == false){
        hb_targets_lock.lock();
        update_hb_targets(true);
        
        hb_targets_lock.unlock();
    }
    membership_list_lock.unlock();
    
    return;
}


/* This function handle N_msg and update membershiplist
 *input:    msg: N_msg
 *output:   NONE
 */
void Protocol::handle_N_msg(string msg, bool haveLock){
    //Get data from msg
    string new_node_id_str = msg.substr(1,16);
    VM_info new_vm(new_node_id_str);
    string new_node_num_str = msg.substr(1,2);
    
    int new_node_num = string_to_int(new_node_num_str);

    //Update membership list
	if(haveLock == false)
		membership_list_lock.lock();
	set<int>::iterator it = membership_list.find(new_node_num);
	if(it == membership_list.end()) {
		membership_list.insert(new_node_num);
		vm_info_map[new_node_num] = new_vm;
		
		//update hb_targets
		hb_targets_lock.lock();
		update_hb_targets(true);
		hb_targets_lock.unlock();
	}
    cout << "VM with id: " << new_node_num << " JOIN.";
    protocol_log << "VM with id: " << new_node_num <<= " JOIN.";
    print_membership_list();
    
    if(haveLock == false)
        membership_list_lock.unlock();
    
    return;
}

/* This function handle L_msg and update membershiplist
 *input:    msg: L_msg
 *output:   NONE
 */
void Protocol::handle_L_msg(string msg, bool haveLock){
    //Get data from msg
    string new_node_num_str = msg.substr(1,2);
    int new_node_num = string_to_int(new_node_num_str);
    
    //Update membership list
    if(haveLock == false)
        membership_list_lock.lock();
    membership_list.erase(new_node_num);
    vm_info_map.erase(new_node_num);
    
    //update hb_targets
    hb_targets_lock.lock();
    update_hb_targets(true);
    hb_targets_lock.unlock();
    
    cout << "VM with id: " << new_node_num << " FAIL.";
    protocol_log << "VM with id: " << new_node_num <<= " FAIL.";
    print_membership_list();
    
    if(haveLock == false)
        membership_list_lock.unlock();
}

/* This function handle I_msg and update membershiplist
 *input:    msg: I_msg
 *output:   NONE
 */
void Protocol::handle_I_msg(string msg){
     
     //Extract membership list data from msg
    my_vm_info.vm_num = string_to_int( msg.substr(1,2));
    
    string num_node_str = msg.substr(3,2);
    int num_node = string_to_int(num_node_str);
    vector<string> nodes;
    vector<int> nodes_num;
    
    for(int i = 0 ; i < num_node; i++){
        nodes.push_back( msg.substr(5+ i*ID_LEN, ID_LEN));
        nodes_num.push_back(string_to_int(msg.substr(5 + i*ID_LEN,2)));
    }
    
    membership_list_lock.lock();
    
     //Add VMs from I_msg to membershiplist
    for(int i = 0 ; i < (int)nodes.size(); i++){
        membership_list.insert(nodes_num[i]);
        VM_info new_node(nodes[i]);
        vm_info_map[nodes_num[i]] = new_node;
    }

    hb_targets_lock.lock();
    update_hb_targets(true);
    
    hb_targets_lock.unlock();
    membership_list_lock.unlock();
}

/* This function handle G_msg and call corresponsding handler
 *input:    msg: G_msg
 *output:   NONE
 */
void Protocol::handle_G_msg(string msg, bool haveLock) {
    if(haveLock == false)
        membership_list_lock.lock();
    
    int rem_retransmits = string_to_int(msg.substr(1,1));
    string str = msg.substr(2);
    int target_id = string_to_int(str.substr(1,2));
    
    
    //If msg is N_msg, call N_msg handler
    if(str[0] == 'N'){
        if(membership_list.find(target_id) == membership_list.end()){
            handle_N_msg(str, true);
        }
        else{
            if(haveLock == false)
                membership_list_lock.unlock();
            return;
        }
    }
    else if(str[0] == 'L') {    //If msg is L_msg, call L_msg handler
        if(membership_list.find(target_id) != membership_list.end()){
            handle_L_msg(str, true);
        }
        else{
            if(haveLock == false)
                membership_list_lock.unlock();
            return;
        }
    }
    else if(str[0] == 'Q'){     //If msg is Q_msg, call Q_msg handler
        if(membership_list.find(target_id) != membership_list.end()){
            handle_Q_msg(str, true);
        }
        else{
            if(haveLock == false)
                membership_list_lock.unlock();
            return;
        }
    }
    
    //Gossip the message
    vector<string> msg_v;
    while(rem_retransmits > 0){
        rem_retransmits--;
        msg[1] = rem_retransmits +'0';
        msg_v.push_back(msg);
    }
    send_gossip_helper(msg_v, true);
    
    if(haveLock == false)
        membership_list_lock.unlock();
    
    return;
}

/* This function handle T_msg and update hearbeat targets
 *input:    msg: T_msg
 *output:   NONE
 */
void Protocol::handle_T_msg(string msg, bool haveLock){
    if(haveLock == false){
        membership_list_lock.lock();
        hb_targets_lock.lock();
    }
    //Get data from msg
    int sender_id = string_to_int(msg.substr(1,2));
    
    //Update HB targets
    if(hb_targets.find(sender_id) != hb_targets.end()){
        vm_info_map[sender_id].heartbeat = 0;
        hb_targets.erase(sender_id);
    }
    
    if(haveLock == false){
        membership_list_lock.unlock();
        hb_targets_lock.unlock();
    }
    return;
}

/* This function handle Q_msg and update membershiplist
 *input:    msg: Q_msg
 *output:   NONE
 */
void Protocol::handle_Q_msg(string msg, bool haveLock){
    string new_node_num_str = msg.substr(1,2);
    int new_node_num = string_to_int(new_node_num_str);
    
    //Update membership list
    if(haveLock == false)
    membership_list_lock.lock();
    membership_list.erase(new_node_num);
    vm_info_map.erase(new_node_num);
    
    //update hb_targets
    hb_targets_lock.lock();
    update_hb_targets(true);
    hb_targets_lock.unlock();
    
    cout <<"VM with id: " << new_node_num << " VOLUNTARILY LEAVE.";
    protocol_log << "VM with id: " << new_node_num <<= " VOLUNTARILY LEAVE.";
    print_membership_list();
    
    if(haveLock == false)
        membership_list_lock.unlock();
}


/* This function gossips the message
 *input:    msg: vector of messages that will be gossiped
 *          haveLock: indicate is have lock or not
 *output:   NONE
 */
void Protocol::gossip_msg(string msg, bool haveLock){
    //Create G_msg based on the input msg
    string g_msg = create_G_msg(msg,G_MESSAGE_NRTS);
    
    if(!haveLock)
        membership_list_lock.lock();
    int rtt = G_MESSAGE_NRTS - 1;
    vector<string> msg_v;
    
    //Gossip messgaes
    while(rtt >= 0){
        g_msg[1] = rtt +'0';
        msg_v.push_back(g_msg);
        rtt--;
    }
    send_gossip_helper(msg_v, true);
    
    if(!haveLock)
        membership_list_lock.unlock();
}

/* This function is a helper function to gossip message
 *input:    msg: vector of messages that will send
 *          haveLock: indicate is have lock or not
 *output:   NONE
 */
void send_gossip_helper(vector<string> msg, bool haveLock){
    vector<int> alive_id_array;
    if(haveLock == false)
        membership_list_lock.lock();
    
    if(membership_list.size() == 1){
        if(haveLock == false)
            membership_list_lock.unlock();
        return;
    }
    int rtt = msg.size();
    
    //Get random Alive VMs from membership list
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0,membership_list.size()-1);
    
    vector<int> mem_list_v;
    int my_idx = 0;
    int count =0 ;
    
    //Get idx of this vm to prevent sending msg to itself
    for(auto it = membership_list.begin(); it != membership_list.end(); it++){
        mem_list_v.push_back(*it);
        if(*it == my_vm_info.vm_num){
            my_idx = count;
        }
        count++;
    }

    set<int> receivers;
    vector<int> receivers_v;
    
    while((int)receivers_v.size() < rtt*GOSSIP_B){
        while((receivers_v.size() < membership_list.size() -1) && ((int)receivers_v.size() < rtt*GOSSIP_B)){
            int temp = distribution(generator);
            if(receivers.find(temp) == receivers.end() && temp != my_idx){
                receivers.insert(temp);
                receivers_v.push_back(temp);
            }
        }
           while((int)receivers_v.size() < rtt*GOSSIP_B){
                  int temp = distribution(generator);
                  if(temp != my_idx){
                      receivers.insert(temp);
                      receivers_v.push_back(temp);
                  }
              }
      }
    
    //Send msg
    UDP local_udp;
    auto it = receivers_v.begin();
    for(int i = 0 ; i < (int) msg.size(); i++){
	   for(int j = 0 ; j < GOSSIP_B; j++){
           local_udp.send_msg(vm_info_map[mem_list_v[*it]].ip_addr_str, msg[i]);
			protocol_log << "Send Gossip to id " << *it << "--- ip: " << vm_info_map[mem_list_v[*it]].ip_addr_str <<= msg[i];
           it++;
        }
    }
    if(haveLock == false)
        membership_list_lock.unlock();
}















