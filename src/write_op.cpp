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

string create_WA_msg(int rep1,int  rep2,int rep3, int version, string file_name){
    string msg("WA");
    msg += int_to_string(rep1);
    msg += int_to_string(rep2);
    msg += int_to_string(rep3);
    msg += int_to_string(version);
    msg += file_name;
    return msg;
}

bool write_at_client(string file_name, string sdfs_file_name){
    FILE* temp;
    if((temp = fopen(file_name, "r"))== NULL){
        cout << "Cannot open file with name: "<< file_name;
        return false;
    }
    else{
        close(temp);
    }

    bool isMaster = false;
    master_lock.lock();
    int cur_master_id = master_id;
    if(master_id == my_vm_info.vm_num){
        isMaster = true;
    }
    master_lock.unlock();

    string wt_msg("");
    string wr_msg = create_WR_msg(sdfs_file_name);

    if(isMaster == false){
        membership_list_lock.lock();
        if(membership_list.find(cur_master_id) == membership_list.end()){
            membership_list_lock.unlock();
            return false;
        }
        VM_info master_info =  vm_info_map[cur_master_id];
        membership_list_lock.unlock();

        //Send write request msg to S
        int master_sock_fd = tcp_open_connection(master_info.ip_addr_str, PORT_WRITE_MSG);
        if(master_sock_fd == -1)
            return false;

        //Create Write Request msg

        int numbytes = tcp_send_string(master_sock_fd, wr_msg);
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
        string temp(buf, numbytes);
        if(temp.size() != 10){
            cout << "WT msg does not have size of 10. Something is WRONG!!\n";
            return false;
        }
        else{
            wt_msg = temp;
        }
    }
    else{
        wt_msg =  handle_WR_msg(-1, wr_msg, false);
        if(wt_msg.size() != 10){
            cout << "WT msg does not have size of 10. Something is WRONG!!\n";
            return false;
        }
    }


    int rep1 = string_to_int(wt_msg.substr(2,2));
    int rep2 = string_to_int(wt_msg.substr(4,2));
    int rep3 = string_to_int(wt_msg.substr(6,2));
    int version = string_to_int(wt_msg.substr(8,2));

    VM_info rep1_info, rep2_info, rep3_info;

    membership_list_lock.lock();
    if(membership_list.find(rep1) == membership_list.find(rep1)){
        cout << "Cannot find replica in membership list. Something is wrong\n";
        membership_list_lock.unlock();
        return false;
    }
    else{
        rep1_info = vm_info_map[rep1];
    }
    if(membership_list.find(rep2) == membership_list.find(rep2)){
        cout << "Cannot find replica in membership list. Something is wrong\n";
        membership_list_lock.unlock();
        return false;
    }
    else{
        rep1_info = vm_info_map[rep2];
    }

    if(membership_list.find(rep1) == membership_list.find(rep3)){
        cout << "Cannot find replica in membership list. Something is wrong\n";
        return false;
    }
    else{
        rep1_info = vm_info_map[rep3];
    }
    membership_list_lock.unlock();

    // Can do these in parallel to make it faster!!! Use 3 threads to do it
    bool result = true;
    bool flag = false;

    //NOTE: Write only succeed when all 3 replicas alive during the writing time.
    //Can change this by re-send request to Master to get new replica

    //NEED to check if replica is current vm
    if(rep1_info.vm_num != my_vm_info.vm_num){
        result &&= check_and_write_file(file_name, sdfs_file_name, rep1_info.ip_addr_str, WRITE_FILE_PORT, version);
    }
    else{
        flag = true;
    }
    if(rep1_info.vm_num != my_vm_info.vm_num){
        result &&= check_and_write_file(file_name, sdfs_file_name, rep2_info.ip_addr_str, WRITE_FILE_PORT, version);
    }
    else{
        flag = true;
    }
    if(rep1_info.vm_num != my_vm_info.vm_num){
        result &&= check_and_write_file(file_name, sdfs_file_name, rep3_info.ip_addr_str, WRITE_FILE_PORT, version);
    }
    else{
        flag = true;
    }

    if(result == false){
        return false;
    }

    if(flag == true){
        delivered_file_map_lock.lock();
        if(delivered_file_map.find(sdfs_file_name) != delivered_file_map.end()
            && delivered_file_map[sdfs_file_name].version >= version){
                delivered_file_map_lock.lock();
        }
        else{
            delivered_file_map_lock.unlock();
            buffer_file_map_lock.lock();
            file_struct f_str;
            f_str.file_name = sdfs_file_name;
            f_str.version = version;

            string f_buffer_name(sdfs_file_name);
            f_buffer_name += int_to_string(version);
            buffer_file_map[f_buffer_name] = f_str;
            std::ifstream ifs(file_name, std::ios::binary); //Copy file
            std::ofstream ofs(f_buffer_name, std::ios::binary);
            ofs << ifs.rdbuf();
            buffer_file_map_lock.unlock();
        }
    }

    //NOTE: This send to old master. If the master fail during the write, then this is unsuccessful write
    //Can Change this by getting current master instead of using old master --> This is more complex.

    string wa_msg = create_WA_msg( rep1,  rep2, rep3, version, sdfs_file_name);
    if(isMaster == false){
        //Send msg to server say that finish writing
        master_sock_fd = tcp_open_connection(master_info.ip_addr_str, PORT_WRITE_MSG);
        if(master_sock_fd == -1){
            cout << "fail to make connection with master.\n";
            return false;
        }

        //Create Write Ack msg
        if(tcp_send_string(master_sock_fd, wa_msg) == -1){
            return false;
        }

        timeout_tv.tv_sec = WRITE_RQ_TIMEOUT;      //in sec
        timeout_tv.tv_usec = 0;
        setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

        //Master reply with WS message
        numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
        if(numbytes <= 2){      //Time out
            close(master_sock_fd);
            return false;
        }
        if(buf[2] == '1'){      //If write succeed.
            close(master_sock_fd);
            cout << "Write successfully.\n";
            return true;
        }
        else{           //If write failed
            cout << "Write failed.\n";
            close(master_sock_fd);
            return false;
        }
    }
    else{
        string ws_msg = handle_WA_msg(-1, wa_msg, false);
        if(ws_msg == 3 && ws_msg[2] == '1'){
            cout << "Write successfully.\n";
            return true;
        }
        else{
            cout << "Write failed.\n";
            return false;
        }
    }
}


string create_FC_msg(string file_name, int version){
    string msg("FC");
    msg += int_to_string(version);
    msg += file_name;
    return msg;
}

string create_FCR_msg(bool is_accept){
    string msg("FCR");
    if(is_accept)
        msg += "1";
    else
        msg += "0";
    return msg;
}

bool check_and_write_file(string file_name, string sdfs_file_name, string dest_ip, int dest_port, int version){
    FILE* fp = fopen(file_name, "r");

    if(fp == NULL){
        cout<< "Cannot open file\n";
        return true;                //Should I return true or false??
    }

    int socket_fd = tcp_open_connection(dest_ip, dest_port);
    if(socket_fd == -1)
        return false;

    string fc_msg = create_FC_msg(sdfs_file_name, version);
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


string handle_WR_msg(int socket_fd, string msg, bool need_to_send){
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
        next_version_map_lock.lock();
        if(next_version_map[file_name] == next_version_map.end()){
            version = 1;
            next_version_map[file_name] = 2;
        }
        else{
            version = next_version_map[file_name];
            next_version_map[file_name]++;
        }
        next_version_map_lock.unlock();
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
        next_version_map_lock.lock();
        version = next_version_map[file_name];
        next_version_map[file_name] ++;
        next_version_map_lock.unlock();
    }
    file_table_lock.unlock();

    string wt_msg = create_WT_msg(rep1, rep2, rep3, version);
    if(need_to_send == true){
        send(socket_fd, wt_msg);
        string ret("");
        return ret;
    }
    else{
        return wt_msg;
    }
    //Callee function will close the socket_fd;
}



string handle_WA_msg(int socket_fd, string msg, bool need_to_send){
    int rep1 = int_to_string(msg.substr(2,2));
    int rep2 = int_to_string(msg.substr(4,2));
    int rep3 = int_to_string(msg.substr(6,2));
    int version = int_to_string(msg.substr(8,2));
    string file_name = msg.substr(10);

    file_table_lock.lock();

    if(filename_map.find(file_name) == filename_map.end()){ //File is not in file table
        string msg("WS0");  //Reply that write operation failed
        if(need_to_send == true)
            send(socket_fd, msg.c_str, msg.size(),0);
        else
            return msg;
    }
    else{
        set<int> rows = filename_map[filename_map];
        for(auto it = rows.begin(); it != end(); it++){
            if(file_table[*it].version > version){
                string msg ("WS1");         //Have a newer version. Tell client that write succeed
                if(need_to_send == true)
                    send(socket_fd, msg.c_str, msg.size(),0);
                else
                    return msg;
            }
        }
        //Check if the 3 reps are still the same
        //If any of them failed or not the same, tell client that write failed
        set<int> cur_reps;
        for(auto it = rows.begin(); it != rows.end(); it ++){
            cur_reps.insert(file_table[*it].replica);
        }
        if(cur_reps.find(rep1) == cur_reps.end() || cur_reps.find(rep2) == cur_reps.end()  || cur_reps.find(rep3) == cur_reps.end() ){
            string msg ("WS0");         //Reply that write operation failed
            if(need_to_send == true)
                send(socket_fd, msg.c_str, msg.size(),0);
            else
                return msg;
        }
        else{
            //Send msg to S1 and S2 to update its file table
            string fu_msg = create_FU_msg(filename, rep1, rep2, rep3, version);

            VM_info rep1_info, rep2_info , rep3_info, master1_info, master2_info;
            //Send msg to rep1,rep2,rep3 to deliver file (move file from buffer to system folder)
            membership_list_lock.lock();
            if(membership_list.find(rep1) == membership_list.end() || membership_list.find(rep2) == membership_list.end() || membership_list.find(rep3) == membership_list.end()){
                membership_list_lock.unlock();
                string msg ("WS0");                     //Reply that write operation failed. "WS0": write failed. "WS1": write succeed
                if(need_to_send)
                    send(socket_fd, msg.c_str, msg.size(),0);
                else
                    return msg;
            }
            else{
                rep1_info = vm_info_map[rep1];
                rep2_info = vm_info_map[rep2];
                rep3_info = vm_info_map[rep3];
                if(membership_list.find(master1_id) == membership_list.end()){
                    master1_info = vm_info_map[master1_id];
                }
                else{
                    master1_info.vm_num = -1;
                }

                if(membership_list.find(master2_id) == membership_list.end()){
                    master2_info = vm_info_map[master2_id];
                }
                else{
                    master2_info.vm_num = -1;
                }
                membership_list_lock.unlock();

                //Send msg to S1 and S2 to update their file table
                if(master1_info.vm_num != -1){
                    int master1_fd_sock = tcp_open_connection(master1_info.ip_addr_str, FILE_TALBE_UP_PORT);
                    if(master1_fd_sock != -1){
                        if(tcp_send_string(master1_fd_sock,fu_msg) == -1){
                            cout << "Cannot send file table update msg to master1\n";
                        }
                        close(master1_fd_sock);
                    }
                }
                if(master2_info.vm_num != -1){
                    int master2_fd_sock = tcp_open_connection(master2_info.ip_addr_str, FILE_TALBE_UP_PORT);
                    if(master2_fd_sock != -1){
                        if(tcp_send_string(master2_fd_sock,fu_msg) == -1){
                            cout << "Cannot send file table update msg to master1\n";
                        }
                        close(master2_fd_sock);
                    }
                }
                string df_msg = create_DF_msg(string file_name, int version);

                //Send msg to rep1,rep2,rep3 to deliver file (move file from buffer to system folder)
                if(rep1 == my_vm_info.vm_num){
                    //Add file to local file list && move file into buffer folder
                    handle_DF_msg(df_msg);
                }
                else{
                    int rep1_sock_fd = tcp_open_connection(rep1_info.ip_addr_str, PORT_WRITE_MSG);
                    if(rep1_sock_fd != -1){
                        tcp_send_string(rep1_sock_fd, df_msg);
                        close(rep1_sock_fd);
                    }
                }

                if(rep1 == my_vm_info.vm_num){
                    //Add file to local file list && move file into buffer folder
                    handle_DF_msg(df_msg);
                }
                else{
                    int rep2_sock_fd = tcp_open_connection(rep2_info.ip_addr_str, PORT_WRITE_MSG);
                    if(rep2_sock_fd != -1){
                        tcp_send_string(rep2_sock_fd, df_msg);
                        close(rep2_sock_fd);
                    }
                }

                if(rep3 == my_vm_info.vm_num){
                    //Add file to local file list && move file into buffer folder
                    handle_DF_msg(df_msg);
                }
                else{
                    int rep3_sock_fd = tcp_open_connection(rep3_info.ip_addr_str, PORT_WRITE_MSG);
                    if(rep3_sock_fd != -1){
                        tcp_send_string(rep3_sock_fd, df_msg);
                        close(rep3_sock_fd);
                    }
                }

                //Update its file_table
                for(auto it = rows.begin(); it != rows.end(); it++){
                    file_table[*it].version = version;
                }

                // Reply to client
                string msg ("WS1");         //Tell client that write succeed
                if(need_to_send)
                    send(socket_fd, msg.c_str, msg.size(),0);
                else
                    return msg;
            }
        }
    }

    file_table_lock.unlock();
    return "";
}


string create_FU_msg(string filename, int rep1, int rep2, int rep3, int version){
    string msg("FU");
    msg += int_to_string(rep1);
    msg += int_to_string(rep2);
    msg += int_to_string(rep3);
    msg += int_to_string(version);
    msg += filename;
    return msg;
}


string create_DF_msg(string file_name, int version){
    string msg("DF");
    msg += int_to_string(version);
    msg += file_name;
    return msg;
}


void handle_DF_msg(string msg){
    delivered_file_map_lock.lock();
    buffer_file_map_lock.lock();
    int version = string_to_int(msg.substr(2,2));
    string file_name = msg.substr(4);

    string file_name_in_buffer(file_name);
    file_name_in_buffer += msg.substr(2,2);

    if(buffer_file_map.find(file_name_in_buffer) == buffer_file_map.end()){
        cout << "Try to deliver unexist file in buffer. Something is wrong\n";
    }
    else{
        file_struct file = buffer_file_map[file_name_in_buffer];
        //If file exist, delete it
        if(delivered_file_map.find(file_name) != delivered_file_map.end()){
            if(delivered_file_map[file_name].version >= version){
                cout << "Try to deliver older version of file. Something is wrong\n";
                buffer_file_map_lock.unlock();
                delivered_file_map_lock.unlock();
                return;
            }
            remove(file_name);
            delivered_file_map.erase(file_name);
        }
        //Rename file in buffer to become file in delivered map
        rename(file_name_in_buffer.c_str(), file_name);

        file_stuct new_file_struct;
        new_file_struct.file_name = file_name;
        new_file_struct.version = version;
        //Add new file to delivered_file_map.
        delivered_file_map[file_name] = new_file_struct;

        //Erase file in buffer_file_map
        buffer_file_map.erase(file_name_in_buffer);

        //Delete all file in buffer with lower version than this version
        for(int i = 0; i < version; i++){
            string temp(file_name );
            temp += int_to_string(i);
            if(buffer_file_map.find(temp) != buffer_file_map.end()){
                remove(temp);
                buffer_file_map.erase(temp);
            }
        }
    }

    buffer_file_map_lock.unlock();
    delivered_file_map_lock.unlock();
}

bool handle_FC_msg(int socket_fd, string msg){
    delivered_file_map_lock.lock();
    int version = string_to_int(msg.substr(2,2));
    string file_name = msg.substr(4);
    string fcr_msg;
    bool flag;
    if(delivered_file_map.find(file_name) == delivered_file_map.end()){
        fcr_msg= create_FCR_msg(true);
        flag = true;
    }
    else{
        file_struct f = delivered_file_map[file_name];
        if(f.version < version){            //Only accept file if file is not exist or new file has newer version
            fcr_msg = create_FCR_msg(true);
            flag = true;
        }
        else{
            fcr_msg = create_FCR_msg(false);
            flag = false;
        }
    }

    send(socket_fd, fcr_msg.c_str(), fcr_msg.size(), 0);
    delivered_file_map_lock.unlock();
    return flag;
}


void handle_FU_msg(string msg){
    file_table_lock.lock();
    int rep1, rep2,rep3;
    rep1 = string_to_int(msg.substr(2,2));
    rep2 = string_to_int(msg.substr(4,2));
    rep3 = string_to_int(msg.substr(6,2));
    version = string_to_int(msg.substr(8,2));
    string file_name = msg.substr(10);

    if(filename_map.find(file_name) == filename_map.end()){
        file_table_lock.unlock();
        return;
    }

    set<int> rows = filename_map.find(file_name);
    for(auto it = rows.begin(); it != rows.end(); it++){
        if(file_table[*it].replica == rep1 || file_table[*it] == rep2 || file_table[*it] == rep3){
            if(file_table[*it].version >  version){
                cout << "Updated version is older than current version. Something's wrong!!!\n";
            }
            else{
                file_table[*it].version = version;
            }
        }
        else{
            cout << "Replica mismatch. Something's wrong!!!\n";
        }
    }
    file_table_lock.unlock();
    return;
}


bool receive_and_store_file(int socket_fd, int file_name){
    FILE* fp = fopen(file_name, "w");

    int file_size = -1;
    struct timeval timeout_tv;
    timeout_tv.tv_sec = FILE_TRANS_TIMEOUT;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    char buf[MAX_BUF_LEN];
    int numbytes;
    if((numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0)) <= 0){
        fclose(fp);
        return false;
    }
    else{
        string file_size_str(buf, numbytes);
        file_size = stoi(file_size_str);
    }

    while(file_size > 0){
        if((numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0)) < 0){
            fclose(fp);
            remove(file_name);
            return false;
        }
        else{
            fwrite(buf, sizeof(char), numbytes, fp);
            file_size -= numbytes;
        }
    }
    fclose(fp);
    return true;
}
