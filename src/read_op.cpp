



bool read_at_client(string file_name){
    master_lock.lock();
    int cur_master = master_id;
    master_lock.unlock();
    
    membership_list_lock.lock();
    if(membership_list.find(cur_master_id) == membership_list.end()){
        membership_list_lock.unlock();
        cout << "Master is currently failed. Please try again later!\n";
        return false;
    }
    VM_info master_info =  vm_info_map[cur_master_id];
    membership_list_lock.unlock();
    
    
    //Send read request msg to S
    int master_sock_fd = tcp_open_connection(master_info.ip_addr_str, PORT_READ_MSG);
    if(master_sock_fd == -1){
        cout << "Cannot make connection with master. Please try again later!\n";
        return false;
    }
    
    //Create Read Request msg
    string msg = create_RR_msg(file_name);
    
    int numbytes = tcp_send_string(master_sock_fd, msg);
    if(numbytes != 0){
        cout << "Cannot make connection with master. Please try again later!\n";
        return false;
    }
    
    struct timeval timeout_tv;
    timeout_tv.tv_sec = READ_RQ_TIMEOUT;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    
    char buf[MAX_BUF_LEN];
    numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
    if(numbytes <= 0){      //Time out
        close(master_sock_fd);
        return false;
    }
    close(master_sock_fd);
    string rt_msg(buf, numbytes);
    if(rt_msg.size() != 11){
        cout << "RT msg does not have size of 11. Something is WRONG!!\n";
        return false;
    }
    
    if(rt_msg[2] != '1'){
        cout << "File not exist!\n";
        return false;
    }
    
    int rep1 = string_to_int(wt_msg.substr(3,2));
    int rep2 = string_to_int(wt_msg.substr(5,2));
    int rep3 = string_to_int(wt_msg.substr(7,2));
    int version = string_to_int(wt_msg.substr(9,2));
    
    if(rep1 == my_vm_info.vm_num || rep2 == my_vm_info.vm_num  || rep3 == my_vm_info.vm_num){
        // File is at local machine.
        //NEED to do: Read from local file.
        delivered_file_map_lock.lock();
        if(delivered_file_map[file_name] != delivered_file_map.end()){
            file_struct file = delivered_file_map[file_name];
            if(version <= file.version){
                cout << "File is currently at local VM.\n";
                delivered_file_map_lock.unlock();
                return true;
            }
        }
        delivered_file_map_lock.unlock();
        if(rep1 == my_vm_info.vm_num)
            rep1 = -1;
        if(rep2 == my_vm_info.vm_num)
            rep2 = -1;
        if(rep3 == my_vm_info.vm_num)
            rep3 = -1;
    }
    
    
    string local_file_name("local_");
    local_file_name += file_name;
    string rft_msg = create_RFT_msg(file_name, version);
    
    vector<int> reps = {rep1, rep2, rep3};
    
    for(int i = 0; i < reps.size(); i++){
        int cur_rep = reps[1];
        if(cur_rep != -1){
            membership_list_lock.lock();
            if(membership_list.find(cur_rep) != membership_list.end()){
                VM_info rep_info = vm_info_map[cur_rep];
                membership_list_lock.unlock();
                
                int local_sock_fd = tcp_open_connection(rep_info.ip_addr_str, READ_PORT_MSG);
                if(local_sock_fd != -1){
                    int numbytes = tcp_send_string(local_sock_fd, rft_msg);
                    if(numbytes == 0 ){
                        setsockopt(local_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
                        numbytes = recv(local_sock_fd, buf, MAX_BUF_LEN, 0);
                        if(numbytes == 3 && buf[2] == '1'){     //This is RS msg
                            if(receive_and_store_file(local_sock_fd, local_file_name) == true){
                                cout << "File stored with name = " << local_file_name;
                                close(local_sock_fd);
                                return true;
                            }
                        }
                    }
                    close(local_sock_fd);
                }
            }
            else{
                membership_list_lock.unlock();
            }
        }
    }
    
    cout << "Cannot read from any replica.\n";
    return false;
}


string create_RFT_msg(string file_name, int version){
    string msg("RFT");
    msg += int_to_string(version);
    msg += file_name;
    return msg;
}

string create_RR_msg(string file_name){
    string msg("RR");
    msg += file_name;
    return msg;
}

//This dont need handler
string create_RT_msg(bool canRead, int rep1, int rep2, int rep3, int version){
    string msg("RT");
    if(canRead == true)
        msg += "1";
    else
        msg += "0";
    
    msg += int_to_string(rep1);
    msg += int_to_string(rep2);
    msg += int_to_string(rep3);
    msg += int_to_string(version);
    return msg;
}

//This dont need handler
string create_RS_msg(bool canRead){
    string msg("RS");
    if(canRead)
        msg += "1";
    else
        msg += "0";
    return msg;
}



void handle_RR_msg(int socket_fd, string msg){
    file_table_lock.lock();
    string file_name = msg.substr(2);
    
    if(filename_map.find(file_name) != filename_map.end()){
        set<int> rows = filename_map[file_name];
        if(rows != 3){
            cout << "File has more or less than 3 replicas. Something is wrong!!\n";
            file_table_lock.unlock();
            return;
        }
        vector<int> reps;
        int version = -1;
        for(auto it = rows.begin(); it != rows.end(); it++){
            reps.push_back(file_table[*it].replica);
            version = version > filename_map[*it].version;
        }
        file_table_lock.unlock();
        string rt_msg = create_RT_msg(true, reps[0], reps[1],reps[2], version);
        tcp_send_string(socket_fd, rt_msg);
    }
    else{
        file_table_lock.unlock();
        string rt_msg = create_RT_msg(false, 99, 99,99, 99);
        tcp_send_string(socket_fd, rt_msg);
    }
}

void handle_RFT_msg(int socket_fd, string msg){
    int version = msg.substr(3,2);
    string file_name = msg.substr(5);
    
    delivered_file_map_lock.lock();
    if(delivered_file_map.find(file_name) != delivered_file_map.end()){
        if(delivered_file_map[file_name].version >= version){       //This does not allow different reads at different files. This is not Good.
            FILE * fp = fopen(file_name);
            if(fp == NULL){
                cout << "Cannot open file\n";
                delivered_file_map_lock.unlock();
                string rs_msg = create_RS_msg(false);
                tcp_send_string(socket_fd, rs_msg);
                return;
            }

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
                    delivered_file_map_lock.unlock();
                    return ;
                }
            }
            fclose(fp);
            close(socket_fd);
            delivered_file_map_lock.unlock();
        }
        else{
            delivered_file_map_lock.unlock();
            string rs_msg = create_RS_msg(false);
            tcp_send_string(socket_fd, rs_msg);
        }
        
    }
    else{
        delivered_file_map_lock.unlock();
        string rs_msg = create_RS_msg(false);
        tcp_send_string(socket_fd, rs_msg);
    }
}









