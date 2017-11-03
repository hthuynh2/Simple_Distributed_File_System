void ls_at_client(string file_name){
    bool isMaster = false;

    master_lock.lock();
    int cur_master = master_id;
    if(cur_master == my_vm_info.vm_num)
        isMaster = true;
    master_lock.unlock();

    string file_info("");

    //Create LS Request msg
    string msg = create_LS_msg(file_name);

    if(isMaster == false){
        membership_list_lock.lock();
        if(membership_list.find(cur_master_id) == membership_list.end()){
            membership_list_lock.unlock();
            cout << "Master is currently failed. Please try again later!\n";
            return;
        }
        VM_info master_info =  vm_info_map[cur_master_id];
        membership_list_lock.unlock();

        //Send read request msg to S
        int master_sock_fd = tcp_open_connection(master_info.ip_addr_str, PORT_READ_MSG);
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
        timeout_tv.tv_sec = READ_RQ_TIMEOUT;      //in sec
        timeout_tv.tv_usec = 0;
        setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

        unsigned char buf[MAX_BUF_LEN];

        numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
        if(numbytes < 0 ){
            cout << "TIMEOUT!\n";
            close(master_sock_fd);
            return;
        }
        string temp(buf,numbytes);
        file_info = temp;
        close(master_sock_fd);
    }
    else{
        file_info = handle_LS_msg(-1, msg, false);
    }
    if(file_info.size() == 1){
        cout << "File not exist.\n";
        return;
    }

    cout << "File " << file_name << " is stored at: \n"
    for(int j = 0 ; j < 3; j ++){
        cout << "......."
        int rep = string_to_int(file_info.substr(0+6*i,2));
        unsigned int ip_b1 = (unsigned int) file_info[2+6*i];
        unsigned int ip_b2 = (unsigned int) file_info[3+6*i];
        unsigned int ip_b3 = (unsigned int) file_info[4+6*i];
        unsigned int ip_b4 = (unsigned int) file_info[5+6*i];
        cout << "VM"<< rep << " with ip address " << ip_b1 << "." << ip_b2 << "."
            << ip_b3 << "." << ip_b4 << "\n";
    }
}


string create_LS_msg(string file_name){
    string msg("LS");
    msg += file_name;
    return msg;
}

string handle_LS_msg(int socket_fd, string msg, bool need_to_send){
    unsigned char ip_addr[4];
    string file_name = msg.substr(2);

    string ret;
    file_table_lock.lock();
    int count = 0;
    if(filename_map.find(file_name) == filename_map.end()){
        ret = "1";
        if(need_to_send){
            tcp_send_string(socket_fd, ret);
            close(socket_fd);
        }
        file_table_lock.unlock();
        return ret;
    }
    else{
        set<int> rows = filename_map[file_name];
        membership_list_lock.lock();
        for(auto it = rows.begin(); it != rows.end(); it ++){
            if(membership_list.find(file_table[*it].replica) == membership_list.end()){
                break;
            }
            else{
                ret += int_to_string(rep_num);
                ret.push_back(vm_info_map[*it].ip_addr[0]);
                ret.push_back(vm_info_map[*it].ip_addr[1]);
                ret.push_back(vm_info_map[*it].ip_addr[2]);
                ret.push_back(vm_info_map[*it].ip_addr[3]);
                count++;
            }
        }
        membership_list_lock.unlock();
    }
    file_table_lock.unlock();
    if(count != 3){
        ret = "1";
        if(need_to_send){
            tcp_send_string(socket_fd, ret);
            close(socket_fd);
        }
        return ret;
    }
    else{
        if(need_to_send){
            tcp_send_string(socket_fd, ret);
            close(socket_fd);
        }
        return ret;
    }
    return "1";
}
