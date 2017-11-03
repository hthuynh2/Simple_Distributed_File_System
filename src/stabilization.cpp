using replicas_t = std::set<int>;
using row_num_t = int;
using vm_id_t = int;


replicas_t route_file(std::string filename) {
    std::hash<std::string> hash_fn;
    std::size_t first_replica = hash_fn(filename) % NUM_VMS;

    replicas_t ret;

    for(int i = 0, j = first_replica; i < NUM_REPLICAS; i++) {
        while(1){
            membership_list_lock.lock();
            if(membership_list.find(j) != membership_list.end() && ret.find(j) != ret.end()){   //j's in membership list
                membership_list_lock.unlock();
                break;
            }
            membership_list_lock.unlock();
            j = (j + 1) % NUM_VMS;
        }
        ret.insert(j);
        j++;
    }
    return ret;
}

void handle_update(std::atomic<int> & count, std::vector<file_update> & updates, std::mutex & updates_mutex, file_update tup) {
    VM_info M_y =  vm_info_map[tup.M_y];
    //Make connection with M_y
    int local_sock = tcp_open_connection(M_y.ip_addr_str, PORT_STABLILIZATION_MSG);

    //Create FTR msg and send to M_y
    string msg = create_FTR_msg(tup.filename, tup.version, tup.M_x);
    if(tcp_send_string(local_sock, msg) == -1){     //Cannot send to M_y --> M_y failed
        // Get the node with the newest version of the file
        // that is not M_y
        int newest_ver = -1;
        row_num_t newest_ver_row;
        for(auto && k : filename_map[tup.filename]) {
            if(file_table[k].version > newest_ver && file_table[k].replica != tup.M_y) {
                newest_ver = file_table[k].version;
                newest_ver_row = file_table[k].row;
            }
        }

        // Get the node which has the file to send
        vm_id_t sender = newest_ver_row.version < tup.version ? newest_ver_row.client : newest_ver_row.replica;

        // Schedule update
        updates_mutex.lock();
        updates.emplace_back(tup.filename, sender, tup.M_x, tup.version);
        updates_mutex.unlock();
    }
    else{
        char* buf[MAX_BUF_LEN];
        int numbytes = 0;
        if((numbytes = tcp_receive_short_msg(local_sock, tup.M_y, buf)) == -1){ //Cannot receive from M_y --> M_y failed
            // Get the node with the newest version of the file
            // that is not M_y
            int newest_ver = -1;
            row_num_t newest_ver_row;

            for(auto && k : filename_map[tup.filename]) {
                if(file_table[k].version > newest_ver && file_table[k].replica != tup.M_y) {
                    newest_ver = file_table[k].version;
                    newest_ver_row = file_table[k].row;
                }
            }


            // Get the node which has the file to send
            vm_id_t sender = newest_ver_row.version < tup.version ? newest_ver_row.client : newest_ver_row.replica;

            // Schedule update
            updates_mutex.lock();
            updates.emplace_back(tup.filename, sender, tup.M_x, tup.version);
            updates_mutex.unlock();
        }
        else{
            close(local_sock);
            //Check if msg say done? Actually don't need to check for the msg. If receive, means done
        }
    }
    count --;
}


void node_fail_handler_at_master(vm_id_t node, int cur_master_id, int cur_master1_id, int cur_master2_id){
    std::vector<file_update> updates;
    std::mutex updates_mutex;

    // Replicate the keys on the failed node
    // Get all file rows with replica 'node'
    for(auto && i : replica_map[node]) {
        auto j = file_table[i];
        // Get the row with the newest version of 'j.filename'
        int newest_ver = -1;
        row_num_t newest_ver_row;
        for(auto && k : filename_map[j.filename]) {
            if(file_table[k].version > newest_ver) {
                newest_ver = file_table[k].version;
                newest_ver_row = file_table[k].row;
            }
        }

        // Get M_y
        vm_id_t M_y = newest_ver_row.replica;

        // Get M_x
        vm_id_t M_x;
        replicas_t replicas = route_file(newest_ver_row.filename);      //NOTE: To avoid re-transmit file, M_x be the next alive node after M_y

        std::set<row_num_t> file_stores = filename_map[newest_ver_row.filename];

        //assert(file_stores.size() == NUM_REPLICAS);
        for(auto && k : file_stores) {
            replicas.erase(file_table[k].replica);
        }

        /* Cannot replicate to the client that made the most
         * recent update to the file. If such a client is a
         * replica, 'replicas' will contain the client vm_id. */
        replicas.erase(newest_ver_row.client);

        M_x = *replicas.begin();
        updates.emplace_back(newest_ver_row.filename, M_y, M_x, newest_ver);
    }

    // Add all updates to file table
    for(auto && i : updates) {
        std::vector<int> intersection_result;
        std::set<row_num_t> & filename_set = filename_map[i.filename];
        std::set<row_num_t> & M_x_set = replicas_map[i.M_x];

        std::set_intersection(
                              filename_set.begin(),
                              filename_set.end(),
                              M_x_set.begin(),
                              M_x_set.end(),
                              //                std::back_inserter(intersection_result.begin())
                              intersection_result.begin()
                              );

        if(intersection_result.empty()) {   //If row is not in the table, add it to the table
            file_table.emplace(
                               file_table.rbegin()->first + 1,
                               file_table.rbegin()->first + 1,
                               i.filename,
                               i.replica,
                               i.version,
                               );
        }
        else if(intersection_result.size() >1){
            cout << "Having 2 replicas storing at same VM ???? Something is wrong! \n";
        }
        else{   //If row is already in the table update the version of row that has file and M_x
            file_table[intersection_result[0]].version = i.version;
        }
    }

    // Atomic variable to count the number of running threads
    std::atomic<int> count = 0;

    // If new elements are added to 'updates' run new thread
    while(1){
        updates_mutex.lock();
        if(updates.empty() && count == 0){
            updates_mutex.unlock();
            break;
        }
        else if(!updates.empty()){
            membership_list_lock.lock();
            if(membership_list.find(tup.M_x) == membership_list.end()){     /*tup.M_x failed*/
                membership_list.unlock();
                updates.erase(updates.rbegin());
            }
            else{
                membership_list_lock.unlock();
                count++;
                std::thread t(handle_update, count, updates, updates_mutex, *updates.rbegin());
                updates.erase(updates.rbegin());            }
        }
        updates_mutex.unlock();
    }
    //
    //        do {
    //            updates_mutex.lock();
    //                for(auto it = updates.begin(); it != updates.end(); it++) {
    //                count++;
    //                std::thread t(std::ref(count), std::ref(updates), std::ref(updates_mutex), std::ref(i));
    //
    //                // Store iterator so it is not invalidated
    //                auto it2 = it - 1;
    //                updates.erase(it);
    //
    //                // Restore invalidated iterator
    //                it = it2;
    //            }
    //            updates_mutex.unlock();
    //        } while(count > 0 || !updates.empty());     //???

    // Delete all the rows in the file_table that have 'node' as a replica
    for(auto && i : replicas_map[node]) {
        file_table.erase(i);
    }

    // Delete the node from the replicas map
    replicas_map.erase(node);

    int new_master2_id = -1;

    VM_info master1_info, master2_info, new_master2_info;

    membership_list_lock.lock();
    if(membership_list[cur_master1_id] != membership_list.end()){
        master1_info = vm_info_map[cur_master1_id];
    }
    else{
        master1_info.vm_num = -1;       //To mark that master 1 failed
    }

    if(membership_list[cur_master2_id] != membership_list.end()){
        master2_info = vm_info_map[cur_master2_id];
    }
    else{
        master2_info.vm_num = -1;       //To mark that master 1 failed
    }
    membership_list_lock.unlock();


    if(node == cur_master1_id || node == cur_master2_id){
        //Choose randomly VM_k to be new master2 if the failed node is any of the 2 old masters
        membership_list_lock.lock();
        for(auto it = membership_list.begin(); it != membership_list.end(); it++){
            if(it->first != cur_master_id && it->first != cur_master1_id && it->first != cur_master1_id){
                new_master2_id = it->first;
                break;
            }
        }

        if(new_master2_id != -1){
            new_master2_info = vm_info_map[new_master2_id];
        }
        else{
            cout << "There is less than 3 VMs in the system!!! This should never happen!!\n";
            exit(1);
        }
        membership_list_lock.unlock();
    }

    if(node == cur_master1_id){     //If node failed is master1
        string msg;
        if(master2_info.vm_num != -1){  //If old master 2 still alive
            //Tell old master2 to become new master 1 && tell that it will choose VM_k to be master2 && update its file table
            msg = create_MU_msg(node, cur_master2_id, new_master2_id);
            int master2_sock_fd = tcp_open_connection(master2_info.ip_addr_str, PORT_STABLILIZATION_MSG);
            if(master2_sock_fd != -1){  //Can make connection with old_master2
                int numbytes = tcp_send_string(master2_sock_fd, msg);        //Don't care if send successfully or not!
                if(numbytes == 0)
                    close(master2_sock_fd);
            }
        }
        else{
            msg = create_MU_msg(node, 99, new_master2_id);        //use 99 to indicate that old master2 failed
        }

        //Tell VM_k to become master 2 && Send the whole file table to new master 2
        int new_master2_sock_fd = tcp_open_connection(new_master2_info.ip_addr_str, PORT_STABLILIZATION_MSG);
        if(new_master2_sock_fd != -1){                                  //Can make connection with new_master2
            int numbytes = tcp_send_string(new_master2_sock_fd, msg);        //Don't care if send successfully or not!
            if(numbytes == 0)
                close(new_master2_sock_fd);
        }
    }
    else if (node == cur_master2_id){
        string msg;
        if(master1_info.vm_num != -1){              //If old master 1 still alive
            //Tell old master2 to become new master 1 && tell that it will choose VM_k to be master2 && update its file table
            msg = create_MU_msg(node, cur_master1_id, new_master2_id);
            int master1_sock_fd = tcp_open_connection(master1_info.ip_addr_str, PORT_STABLILIZATION_MSG);
            if(master1_sock_fd != -1){  //Can make connection with old_master2
                int numbytes = tcp_send_string(master1_sock_fd, msg);        //Don't care if send successfully or not!
                if(numbytes == 0)
                    close(master1_sock_fd);
            }
        }
        else{
            msg = create_MU_msg(node, 99, new_master2_id);        //use 99 to indicate that old master2 failed
        }

        //Tell VM_k to become master 2 && Send the whole file table to new master 2
        int new_master2_sock_fd = tcp_open_connection(new_master2_info.ip_addr_str, PORT_STABLILIZATION_MSG);
        if(new_master2_sock_fd != -1){                                  //Can make connection with new_master2
            int numbytes = tcp_send_string(new_master2_sock_fd, msg);        //Don't care if send successfully or not!
            if(numbytes == 0)
                close(new_master2_sock_fd);
        }
    }
    else{           //Both master1 and master2 are still alive. Send updated file table to both of them
        msg = create_MU_msg(node, cur_master1_id, cur_master1_id);

        int master1_sock_fd = tcp_open_connection(master1_info.ip_addr_str, PORT_STABLILIZATION_MSG);
        if(master1_sock_fd != -1){                                      //Can make connection with new_master1
            int numbytes = tcp_send_string(master1_sock_fd, msg);        //Don't care if send successfully or not!
            if(numbytes == 0)
                close(master1_sock_fd);
        }

        int master2_sock_fd = tcp_open_connection(master2_info.ip_addr_str, PORT_STABLILIZATION_MSG);
        if(master2_sock_fd != -1){                                      //Can make connection with new_master2
            int numbytes = tcp_send_string(master2_sock_fd, msg);        //Don't care if send successfully or not!
            if(numbytes == 0)
                close(master2_sock_fd);
        }
    }
}

void node_fail_handler_at_master1(vm_id_t node){
    if(node == master_id){          //I'll become the master!
        if(last_failed_node != -1){ //If there is another node failed before the master fail
            //Choose another node to be new master 1
            if(last_failed_node == master2_id){         //Last failed node is master2
                //Choose two new master1 and master2
                int new_master1_id = -1;
                int new_master2_id = -1;
                membership_list_lock.lock();
                for(auto it = membership_list.begin(), it != membership_list.end(); it++){
                    if(*it != my_vm_info.vm_num && *it != node && *it != last_failed_node){
                        if(new_master1_id == -1){
                            new_master1_id = *it;
                        }
                        else if(new_master2_id == -1){
                            new_master2_id = *it;
                        }
                        else{
                            break;
                        }
                    }
                }
                membership_list_lock.unlock();

                //Handle failure at last failed node
                node_fail_handler_at_master(last_failed_node, my_vm_info.vm_num, new_master1_id, new_master2_id);

                //Hanlde failure at current failed node
                node_fail_handler_at_master(node, my_vm_info.vm_num, new_master1_id, new_master2_id);

                //Already handle 2 failure. Reset last_failed_node
                last_failed_node = -1;

                master1_id = new_master1_id;
                master2_id = new_master2_id;
            }
            else{                   //Last failed node is normal node
                //Choose a new node to be new master 1
                int new_master2_id = -1;
                membership_list_lock.lock();
                for(auto it = membership_list.begin(), it != membership_list.end(); it++){
                    if(*it != my_vm_info.vm_num && *it != node && *it != master2_id){
                        if(new_master2_id == -1){
                            new_master2_id = *it;
                            break;
                        }
                    }
                }
                membership_list_lock.unlock();

                //Handle failure at last failed node
                node_fail_handler_at_master(last_failed_node, my_vm_info.vm_num, master2_id, new_master2_id);
                //Hanlde failure at current failed node
                node_fail_handler_at_master(node, my_vm_info.vm_num, master2_id, new_master2_id);
                //Already handle 2 failure. Reset last_failed_node
                last_failed_node = -1;

                //master1 will become new master;
                //master2 will beomce new master1
                master1_id = master2_id;
                master2_id = new_master2_id;
            }
        }
        else{                       //There is no node failed before this
            //Choose a new node to be new master 2
            int new_master2_id = -1;
            membership_list_lock.lock();
            for(auto it = membership_list.begin(), it != membership_list.end(); it++){
                if(*it != my_vm_info.vm_num && *it != node && *it != master2_id){
                    if(new_master2_id == -1){
                        new_master2_id = *it;
                        break;
                    }
                }
            }
            membership_list_lock.unlock();

            //Hanlde failure at current failed node
            node_fail_handler_at_master(node, my_vm_info.vm_num, master1_id, new_master2_id);
            //Already handle failure. Reset last_failed_node
            last_failed_node = -1;

            //master1 will become new master;
            //master2 will beomce new master1
            //Need to be in this order to prevent system from hanging
            master1_id = master2_id;
            master2_id = new_master2_id;
        }

        //Update myself as master!
        master = my_vm_info.vm_num;

        //Send out msg to say that I'm master
        string msg = create_M_msg(master);
        vector<string> ip_strings;
        membership_list_lock.lock();
        for(auto it = membership_list.begin(); it != membership_list.end(); it++){
            if(*it != my_vm_info.vm_num)
                ip_strings.push_back(vm_info_map[*it].ip_addr_str);
        }
        membership_list_lock.unlock();

        //Send all msg to other VMs. Is there a better way to do this???? This seems too slow
        for(int i = 0; i < ip_strings.size(); i++){
            int temp_sock_fd = tcp_open_connection(ip_strings[i], PORT_STABLILIZATION_MSG);
            if(temp_sock_fd != -1){
                int numbytes = tcp_send_string(temp_sock_fd, msg);
                if(numbytes == 0 )
                    close(temp_sock_fd);
            }
        }
    }
    else{
        //Mark this one as last_failed_node and wait for Master to handle it!!
        last_failed_node = node;
    }
}


void node_fail_handler_at_master2(vm_id_t node){
    if((node == master_id && last_failed_node == master1_id) ||((node == master1_id) && (last_failed_node == master_id))) {          //I'll become the master!
        //Choose two new master1 and master2
        int new_master1_id = -1;
        int new_master2_id = -1;
        membership_list_lock.lock();
        for(auto it = membership_list.begin(), it != membership_list.end(); it++){
            if(*it != my_vm_info.vm_num && *it != node && *it != last_failed_node){
                if(new_master1_id == -1){
                    new_master1_id = *it;
                }
                else if(new_master2_id == -1){
                    new_master2_id = *it;
                }
                else{
                    break;
                }
            }
        }
        membership_list_lock.unlock();

        //Handle failure at last failed node
        node_fail_handler_at_master(last_failed_node, my_vm_info.vm_num, new_master1_id, new_master2_id);

        //Hanlde failure at current failed node
        node_fail_handler_at_master(node, my_vm_info.vm_num, new_master1_id, new_master2_id);

        //Already handle 2 failure. Reset last_failed_node
        last_failed_node = -1;

        //Send out msg to say that I'm master
        //Update myself as master!
        master = my_vm_info.vm_num;

        //Update master1 and master2
        master1_id = new_master1_id;
        master2_id = new_master1_id;

        //Send out msg to say that I'm master
        string msg = create_M_msg(master);
        vector<string> ip_strings;
        membership_list_lock.lock();
        for(auto it = membership_list.begin(); it != membership_list.end(); it++){
            if(*it != my_vm_info.vm_num)
                ip_strings.push_back(vm_info_map[*it].ip_addr_str);
        }
        membership_list_lock.unlock();

        //Send all msg to other VMs. Is there a better way to do this???? This seems too slow
        for(int i = 0; i < ip_strings.size(); i++){
            int temp_sock_fd = tcp_open_connection(ip_strings[i], PORT_STABLILIZATION_MSG);
            if(temp_sock_fd != -1){
                int numbytes = tcp_send_string(temp_sock_fd, msg);
                if(numbytes == 0 )
                    close(temp_sock_fd);
            }
        }
    }
    else{
        //Mark this as last_failed node and wait for Master and Master1 to handle it!!
        last_failed_node = node;
    }
}


// Called whenever a node is detected to fail
void node_fail_handler(vm_id_t node) {
    file_table_lock.lock();
    if(my_vm_info.vm_num == master_id) {        //I'm the Master
        node_fail_handler_at_master(vm_id_t node);
    }
    else {          //
        if(!file_table.empty()){/*I am the next master*/
            if(my_vm_info.vm_num == master1_id){
                node_fail_handler_at_master1(vm_id_t node);
            }
            else if(my_vm_info.vm_num == master2_id){
                node_fail_handler_at_master2(vm_id_t node);
            }
        }
    }
    file_table_lock.unlock();
    //Check if there is any node waiting to hanled. If yes, handle it!
    waiting_to_handle_fail_id_lock.lock();
    if(waiting_to_handle_fail_id != -1){
        int node_to_handle = waiting_to_handle_fail_id;
        waiting_to_handle_fail_id = -1;
        waiting_to_handle_fail_id_lock.unlock();
        node_fail_handler(node_to_handle);
    }
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
    next_version_map_lock.lock();
    for(auto it = file_table.begin(); it != file_table.end(); it++){
        string file_str(it->second.file_name);
        file_str.push_back('?');
        file_str += int_to_string(it->second.replica);
        file_str += int_to_string(it->second.version);
        file_str += int_to_string(next_version_map[it->second.file_name]);
        msg += file_str;
    }
    next_version_map_lock.unlock();
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
        VM_info M_x_info = vm_info_map[M_x];
        membership_list_lock.unlock();
    }
    else{
        membership_list_lock.unlock();
        return;
    }

    check_and_write_file(file_name, M_x.ip_addr_str, PORT_STABLILIZATION_FILE, version);
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
    master1_id = new_master1;
    master2_id = new_master2;
    master_lock.unlock();


    //Need lock ??
    file_table.erase(file_table.begin(), file_table.end());
    next_version_map.lock();
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

    if(last_failed_node == node_fail_id){
        last_failed_node = -1;
    }
    if(waiting_to_handle_fail_id == node_fail_id){
        waiting_to_handle_fail_id = -1;
    }

    next_version_map.unlock();

    file_table_lock.unlock();
    return;
}

void handle_M_msg(string msg){
    master_lock.lock();
    int new_master = string_to_int(msg.substr(1,2));
    master = new_master;
    master_lock.unlock();
}
