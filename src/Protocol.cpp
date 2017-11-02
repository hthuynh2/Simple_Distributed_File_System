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

//////////////////////////////////////
//File Transfer Request msg
string create_FTR_msg(string file_name, int version, int M_x){
    string msg("FTR");
    msg += file_name;
    msg += "\n";
    msg += int_to_string(version);
    msg += int_to_string(M_x);
    return msg;
}

string create_MU_msg(int failed_node, int master1_id, int master2_id){
    string msg("MU");
    msg += int_to_string(failed_node);
    msg += int_to_string(master1_id);
    msg += int_to_string(master2_id);
    msg += "\n";
    for(auto it = file_table.begin(); it != file_table.end(); it++){
        string file_str(it->second.file_name);
        file_str.push_back('?');
        file_str += int_to_string(it->second.replica);
        file_str += int_to_string(it->second.version);
        file_str += int_to_string(next_version_map[it->second.file_name]);
        msg += file_str;
    }
    
    return msg;
}



string create_M_msg(int master_id){
    string msg("M");
    msg += int_to_string(master_id);
    return msg;
}







void handle_FTR_msg(string msg){
    char* pch = strchr(msg.c_str(), '\n');
    int file_name_length = pch - msg.c_str() - 4;
    string file_name = msg.substr(3, file_name_length);
    int version = string_to_int(msg.substr(pch-msg.c_str()+1, 2));
    int M_x = string_to_int(msg.substr(pch-msg.c_str()+1 +2, 2));
    
    membership_list_lock.lock();
    if(membership_list.find(M_x) != membership_list.end()){
        VM_info M_x_info = membership_list[M_x];
        membership_list_lock.unlock();
    }
    else{
        membership_list_lock.unlock();
        return;
    }
    
    //NEED to implement so that only send if M_x need this file
    send_file(M_x.ip_addr_str, file_name, version);
    return;
}

void handle_MU_msg(string msg){
    file_table_lock.lock();
    vector<string> lines;
    
    string delimiter = "\n";
    size_t pos = 0;
    std::string token;
    while ((pos = msg.find(delimiter)) != std::string::npos) {
        token = msg.substr(0, pos);
        token.push_back('\n');
        lines.push_back(token);
        msg.erase(0, pos + delimiter.length());
    }
    
    int new_master, new_master1 , new_master2, node_fail_id;
    node_fail_id = string_to_int(lines[0].substr(2, 2));
    new_master = string_to_int(lines[0].substr(4,2));
    new_master1 = string_to_int(lines[0].substr(6, 2));
    new_master2 = string_to_int(lines[0].substr(8, 2));
    
    master_lock.lock();

    master = new_master;
    master_lock.unlock();

    master1_id = new_master1;
    master2_id = new_master2;

    
    //Need lock ??
    file_table.erase(file_table.begin(), file_table.end());
    next_version_map.erase(next_version_map.begin(), next_version_map.end());
    replica_map.erase(replica_map.begin(), replica_map.end());
    filename_map.erase(filename_map.begin(), filename_map.end());
    
    for(int i = 1; i < lines.size(); i++){
        char* pch = strchr(lines[i].c_str(), '?');
        int file_name_length = pch - msg.c_str();
        string file_name = msg.substr(0, file_name_length);
        int replica_id = string_to_int(msg.substr(file_name_length+1, 2));
        int version = string_to_int(msg.substr(file_name_length+1+2, 2));
        int next_version = string_to_int(msg.substr(file_name_length+1+2+2, 2));
        
        file_row new_row;
        new_row.row = i;
        new_row.file_name = file_name;
        new_row.version = version;
        new_row.replica = replica_id;
        
        file_table[i] = file_row;
        next_version_map[file_name] = next_version;
        replica_map[replica_id] = i;
        filename_map[file_name] = i;
    }
    
    file_table_lock.unlock();
    return;
}

//Do I need lock for the master????
void handle_M_msg(string msg){
    master_lock.lock();
    int new_master = string_to_int(msg.substr(1,2));
    master = new_master;
    master_lock.unlock();

}


string create_WR_msg(string file_name){
    string msg("WR");
    msg += file_name;
    return msg;
}


string create_WT_msg(int rep1, int rep2, int rep3, int version){
    string msg("WT");
    msg += int_to_string(rep1);
    msg += int_to_string(rep2);
    msg += int_to_string(rep3);
    msg += int_to_string(version);
    return msg;
}


string reate_WA_msg(int rep1,int  rep2,int rep3, int version, string file_name){
    string msg("WA");
    msg += int_to_string(rep1);
    msg += int_to_string(rep2);
    msg += int_to_string(rep3);
    msg += int_to_string(version);
    msg += file_name;
    return msg;
}



bool write_at_client(string file_name){
    
    master_lock.lock();
    int cur_master_id = master_id;
    master_lock.unlock();
    
    membership_list_lock();
    if(membership_list.find(cur_master_id) == membership_list.end()){
        membership_list_lock.unlock();
        return false;
    }
    VM_info master_info =  membership_list[cur_master_id];
    membership_list_lock.unlock();
    
    //Send write request msg to S
    int master_sock_fd = tcp_open_connection(master_info.ip_addr_str, PORT_WRITE_MSG);
    if(master_sock_fd == -1)
        return false;
    
    //Create Write Request msg
    string msg = create_WR_msg(file_name);
    
    int numbytes = tcp_send_string(master_sock_fd, msg);
    if(numbytes != 0)
        return false;
    
    struct timeval timeout_tv;
    timeout_tv.tv_sec = WRITE_RQ_TIMEOUT;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    char buf[MAX_BUF_LEN];
    numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
    if(numbytes <= 0){      //Time out
        close(master_sock_fd);
        return false;
    }
    close(master_sock_fd);
    string wt_msg(buf, numbytes);
    if(wt_msg.size() != 10){
        cout << "WT msg does not have size of 10. Something is WRONG!!\n";
        return false;
    }
    
    int rep1 = string_to_int(wt_msg.substr(2,2));
    int rep2 = string_to_int(wt_msg.substr(4,2));
    int rep3 = string_to_int(wt_msg.substr(6,2));
    int version = string_to_int(wt_msg.substr(8,2));
    
    VM_info rep1_info, rep2_info, rep3_info;
    
    membership_list_lock.lock();
    if(membership_list.find(rep1) == membership_list.find(rep1)){
        cout << "Cannot find replica in membership list. Something is wrong\n";
        return false;
    }
    else{
        rep1_info = membership_list[rep1];
    }
    if(membership_list.find(rep2) == membership_list.find(rep2)){
        cout << "Cannot find replica in membership list. Something is wrong\n";
        return false;
    }
    else{
        rep1_info = membership_list[rep2];
    }
    
    if(membership_list.find(rep1) == membership_list.find(rep3)){
        cout << "Cannot find replica in membership list. Something is wrong\n";
        return false;
    }
    else{
        rep1_info = membership_list[rep3];
    }
    membership_list_lock.unlock();
    
    // Can do these in parallel to make it faster!!! Use 3 threads to do it
    bool result = true;
    bool flag = false;
    //NEED to check if replica is current vm
    if(rep1_info.vm_num != my_vm_info.vm_num){
        result &&= check_and_write_file(file_name, rep1_info.ip_addr_str, WRITE_FILE_PORT, version);
    }
    else{
        flag = true;
    }
    if(rep1_info.vm_num != my_vm_info.vm_num){
        result &&= check_and_write_file(file_name, rep2_info.ip_addr_str, WRITE_FILE_PORT, version);
    }
    else{
        flag = true;
    }
    if(rep1_info.vm_num != my_vm_info.vm_num){
        result &&= check_and_write_file(file_name, rep3_info.ip_addr_str, WRITE_FILE_PORT, version);
    }
    else{
        flag = true;
    }
    
    if(result == false){
        return false;
    }
    
    //NOTE: This send to old master. If the master fail during the write, then this is unsuccessful write
    //Can Change this by getting current master instead of using old master --> This is more complex.
    
    //Send msg to server say that finish writing
    master_sock_fd = tcp_open_connection(master_info.ip_addr_str, PORT_WRITE_MSG);
    if(master_sock_fd == -1){
        return false;
    }
    
    //Create Write Ack msg
    string wr_msg = create_WA_msg( rep1,  rep2, rep3, version, file_name);
    if(tcp_send_string(master_sock_fd, wr_msg) == -1){
        return false;
    }
    
    timeout_tv.tv_sec = WRITE_RQ_TIMEOUT;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    
    numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
    if(numbytes <= 2){      //Time out
        close(master_sock_fd);
        return false;
    }
    
    if(buf[2] != '1'){
        close(master_sock_fd);
        if(flag == true){
            //NEED TO DO:
            //Add file to local file list && move file into buffer folder
        }
        return false;
    }
    else{
        close(master_sock_fd);
        return true;
    }
}


string create_FC_msg(string file_name, int version){
    string msg("FC");
    msg += file_name;
    msg += "?";
    msg += version;
    return msg;
}

string create_FTR_msg(bool is_accept){
    string msg("FCR");
    if(is_accept)
        msg += "1";
    else
        msg += "0";
    return msg;
}

bool check_and_write_file(string file_name, string dest_ip, int dest_port, int version){
    FILE* fp = fopen(path, "r");

    if(fp == NULL){
        cout<< "Cannot open file\n";
        return true;                //Should I return true or false??
    }
    
    int socket_fd = tcp_open_connection(dest_ip, dest_port);
    if(socket_fd == -1)
        return false;
    
    string fc_msg = create_FC_msg(file_name, version);
    int numbytes;
    if((numbytes = send(socket_fd, fc_msg.c_str(), fc_msg.size())) == -1){
        printf("Fail to send FC_msg\n");
        close(socket_fd);
        return false;
    }
    
    struct timeval timeout_tv;
    timeout_tv.tv_sec = WRITE_FC_TIMEOUT;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    
    char buf[MAX_BUF_LEN];
    numbytes = recv(socket_fd, buf, MAX_BUF_LEN,0);
    if(numbytes <= 3){      //Time out
        close(socket_fd);
        return false;
    }
    
    char reply = buf[3];
    if(reply != '1'){
        close(socket_fd);
        return true;
    }
    
    timeout_tv.tv_sec = 0;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    int file_size;
    fseek(fp, 0L, SEEK_END);
    file_size = ftell(fp);
    fseek(fp, 0L, SEEK_SET);
    
    string file_size_str = to_string(file_size);
    
    //Send size of file
    if (send(socket_fd, file_size_str, file_size_str.size(), 0) == -1){
        perror("send");
        fclose(fp);
        close(new_fd);
        return false;
    }
    
    //Read from file and send data
    int numbytes;
    while((numbytes = fread(buf, 1, MAX_STR_LEN, fp)) != 0){
        if (send(socket_fd, buf, numbytes, 0) == -1){
            perror("send");
            fclose(fp);
            close(new_fd);
            return false;
        }
    }
    fclose(fp);
    close(socket_fd);
}


void handle_WR_msg(int socket_fd, string msg){
    string file_name = msg.substr(2);
    int rep1 = -1, rep2 = -1, rep3 = -1;
    int version ;
    file_table_lock.lock();
    if(filename_map.find(file_name) == filename_map.end()){//file does not exist
        set<int> reps = route_file(file_name);
        for(auto it = reps.begin(); it != reps.end(); it ++){
            if(rep1 == -1){
                rep1 = *it;
            }
            else if(rep2 == -1){
                rep2 = *it;
            }
            else if(rep3 == -1){
                rep3 = *it;
            }
            else
                break;
        }
        if(next_version_map[file_name] == next_version_map.end()){
            version = 1;
            next_version_map[file_name] = 2;
        }
        else{
            version = next_version_map[file_name];
            next_version_map[file_name]++;
        }
    }
    else{
        set<int> rows = filename_map[file_name];
        if(rows.size() != 3){    //Something is wrong!!
            cout << "File has less than 3 replica. Something is WRONG\n";
        }
        for(auto it = rows.begin(); it != rows.end(); it++){
            if(rep1 == -1){
                rep1 = file_table[*it].replica;
            }
            else if(rep2 == -1){
                rep2 = file_table[*it].replica;
            }
            else if(rep3 == -1){
                rep3 = file_table[*it].replica;
            }
            else
                break;
        }
        version = next_version_map[file_name];
        next_version_map[file_name] ++;
    }
    
    file_table_lock.unlock();
    
    string wt_msg = create_WT_msg(rep1, rep2, rep3, version);
    send(socket_fd, wt_msg);
    //Callee function will close the socket_fd;
}


void handle_WA_msg(int socket_fd, string msg){
    int rep1 = int_to_string(msg.substr(2,2));
    int rep2 = int_to_string(msg.substr(4,2));
    int rep3 = int_to_string(msg.substr(6,2));
    int version = int_to_string(msg.substr(8,2));
    string file_name = msg.substr(10);
    
    file_table_lock.lock();
    
    if(filename_map.find(file_name) == filename_map.end()){
        
    }
    else{
        
    }
    
    set<int> = route_file(file_name);
    
    
    
    file_table_lock.unlock();
    
    //Send msg to S1 S2 before reply client!!!!
}

replicas_t route_file(std::string filename) {

    
    























