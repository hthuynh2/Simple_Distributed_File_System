void delete_at_client(string file_name){
    bool isMaster = false;

    master_lock.lock();
    int cur_master = master_id;
    if(my_vm_info.vm_num == cur_master)
        isMaster = true;
    master_lock.unlock();

    //Create Delete Request msg
    string msg = create_DR_msg(file_name);
    if(isMaster == false){
        membership_list_lock.lock();
        if(membership_list.find(cur_master_id) == membership_list.end()){
            membership_list_lock.unlock();
            cout << "Master is currently failed. Please try again later!\n";
            return false;
        }
        VM_info master_info =  vm_info_map[cur_master_id];
        membership_list_lock.unlock();

        //Send delete request msg to S
        int master_sock_fd = tcp_open_connection(master_info.ip_addr_str, PORT_DELETE_MSG);
        if(master_sock_fd == -1){
            cout << "Cannot make connection with master. Please try again later!\n";
            return false;
        }

        int numbytes = tcp_send_string(master_sock_fd, msg);
        if(numbytes != 0){
            cout << "Cannot make connection with master. Please try again later!\n";
            return false;
        }
        struct timeval timeout_tv;
        timeout_tv.tv_sec = DELETE_RQ_TIMEOUT;      //in sec
        timeout_tv.tv_usec = 0;
        setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

        char buf[MAX_BUF_LEN];
        numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
        if(numbytes <= 2){      //Time out
            close(master_sock_fd);
            cout << "...Receive nothing from master. ??? Maybe master's failed. Please try again later\n";
            return false;
        }
        close(master_sock_fd);

        if(buf[2] == '1'){  // "DS1: succeed or DS0: failed"
            cout << "Successfully delete file.\n";
        }
        else{
            cout << "Cannot delete file. Please try again.\n";
        }
    }
    else{
        string ret = handle_DR_msg(msg, -1, false);
        if(ret.size() == 3 && ret[2] == '1'){
            cout << "Successfully delete file.\n";
        }
        else{
            cout << "Cannot delete file. Please try again.\n";
        }
    }
    return;
}


string create_DR_msg(string file_name){
  string msg("DR");
  msg += file_name;
  return msg;
}

//No need handler
string create_DS_msg(bool is_success){
    string msg("DS");
    if(is_success == true){
        msg += "1";
    }
    else{
        msg += "0";
    }
    return msg;
}

string create_RFR_msg(string file_name){
    string msg("RFR");
    msg += file_name;
    return msg;
}

string create_FTD_msg(string file_name){
    string msg("FTD");
    msg += file_name;
    return msg;
}

string handle_DR_msg(string msg, int socket_fd, bool need_to_send){
    string file_name = msg(2);
    file_table_lock.lock();
    if(filename_map.find(file_name) == file_name.end()){
        file_table_lock.unlock();
        string ds_msg = create_DS_msg(false);
        if(need_to_send == true){
            tcp_send_string(socket_fd, ds_msg);
            close(socket_fd);
        }
        return ds_msg;
    }
    set<int> rows = filename_map(file_name);
    int rep1 = -1, rep2 = -1,rep3 = -1;
    for(auto it = rows.begin(); it != rows.end(); it++){
        if(rep1 == -1){
            rep1 = *it;
        }
        else if(rep2 == -1){
            rep2 = *it;
        }
        else if(rep3 == -1){
            rep3 = (*it);
        }
        else{
            cout << "Have more than 3 reps. Sth is wrong!!\n".
            break;
        }
    }

    file_table_lock.unlock();
    master_lock.lock();
    int cur_master1_id = master1_id;
    int cur_master2_id = master2_id;
    master_lock.unlock();

    membership_list_lock.lock();
    std::vector<int> v_rep_id = {rep1, rep2, rep3};
    std::vector<VM_info> v_rep_info(3);
    VM_info master1_info, master2_info;

    membership_list_lock.lock();
    for(int i = 0 ; i < v_rep_id.size(); i++){
        if(membership_list.find(v_rep_id[i]) != membership_list.end()){
            v_rep_info[i] = vm_info_map[v_rep_id[i]];
        }
    }
    if(membership_list.find(cur_master1_id) != membership_list.end()){
        master1_info = vm_info_map[cur_master1_id];
    }
    if(membership_list.find(cur_master2_id) != membership_list.end()){
        master2_info = vm_info_map[cur_master2_id];
    }
    membership_list_lock.unlock();
    //Update file table of Master 1 and Master 2
    string ftd_msg = create_FTD_msg(file_name);
    if(master1_info.ip_addr_str != ""){
        int master1_sock = tcp_open_connection(master1_info.ip_addr_str, DELETE_PORT);
        if(master1_sock != -1){
            tcp_send_string(ftd_msg);
            close(master1_sock);
        }
    }

    if(master2_info.ip_addr_str != ""){
        int master2_sock = tcp_open_connection(master2_info.ip_addr_str, DELETE_PORT);
        if(master2_sock != -1){
            tcp_send_string(ftd_msg);
            close(master2_sock);
        }
    }

    string rfr_msg = create_RFR_msg(file_name);
    for(int = 0 ; i < v_rep_id.size(); i++){
        if(v_rep_id[i] == my_vm_info.vm_num){
            handle_RFR_msg(rfr_msg);
        }
        else if(v_rep_id[i] != -1 && v_rep_info[i].ip_addr_str != ""){
            int local_sock = tcp_open_connection(v_rep_info[i].ip_addr_str, DELETE_PORT);
            if(local_sock != -1){
                tcp_send_string(rfr_msg);
                close(local_sock);
            }
        }
    }

    file_table_lock.lock();
    //Delete rows in file table
    set<int> row_ids = filename_map[filename];
    for(auto it = row_ids.begin(); it != row_ids.end(); it++){
        file_table.erase(*it);
    }
    file_table_lock.unlock();

    next_version_map_lock.lock();
    next_version_map.erase(file_name);
    next_version_map_lock.unlock();
    //Reply to client
    string ds_msg = create_DS_msg(true);
    if(need_to_send == true){
        tcp_send_string(socket_fd, ds_msg);
        close(socket_fd);
    }

    return ds_msg;
}

void handle_FTD_msg(string msg){
    string file_name = msg.substr(2);
    file_table_lock.lock();
    if(filename_map.find(file_name) != filename_map.end()){
        set<int> rows = filename_map[file_name];
        for(auto it = rows.begin(); it != rows.end(); it++){
            file_table.erase(*it);
        }
    }
    file_table_lock.unlock();
    next_version_map_lock.lock();
    next_version_map.erase(file_name);
    next_version_map_lock.unlock();
}

void handle_RFR_msg(string msg){
    string file_name = msg.substr(2);
    delivered_file_map_lock.lock();
    buffer_file_map_lock.lock();

    for(int i = 0 ; i < 99; i++){
        string temp_name(file_name);
        temp_name += int_to_string(i);
        if(buffer_file_map.find(temp_name) != buffer_file_map.end()){
            remove(temp_name);
        }
    }
    if(delivered_file_map.find(file_name) != delivered_file_map.end()){
        remove(temp_name);
    }
    buffer_file_map_lock.unlock();
    delivered_file_map_lock.unlock();
    return;
}
