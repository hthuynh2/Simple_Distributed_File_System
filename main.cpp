#include <iostream>
#include <algorithm>
#include <set>
#include <unordered_map>

////////////////////
#include "common.h"
#include "Protocol.h"
#include "UDP.h"
#include "logging.h"

membership::Logger my_logger;
membership::Logger::Handle main_log = my_logger.get_handle("Main\t\t\t\t");
membership::Logger::Handle hb_sender_log = my_logger.get_handle("Heartbeat Sender\t");
membership::Logger::Handle hb_checker_log = my_logger.get_handle("Heartbeat Checker\t");
membership::Logger::Handle io_log = my_logger.get_handle("User Input\t\t\t");
membership::Logger::Handle update_hbs_log = my_logger.get_handle("Heartbeat Targets\t");
membership::Logger::Handle protocol_log = my_logger.get_handle("Protocol\t\t\t");

unordered_map<int, VM_info> vm_info_map;
set<int> membership_list;
std::mutex membership_list_lock;
set<int> hb_targets;
std::mutex hb_targets_lock;

//No need lock
int my_socket_fd;
VM_info my_vm_info;
void update_hb_targets(bool haveLock);

UDP* my_listener;

string vm_hosts[NUM_VMS] =  {
    "fa17-cs425-g13-01.cs.illinois.edu",
    "fa17-cs425-g13-02.cs.illinois.edu",
    "fa17-cs425-g13-03.cs.illinois.edu",
    "fa17-cs425-g13-04.cs.illinois.edu",
    "fa17-cs425-g13-05.cs.illinois.edu",
    "fa17-cs425-g13-06.cs.illinois.edu",
    "fa17-cs425-g13-07.cs.illinois.edu",
    "fa17-cs425-g13-08.cs.illinois.edu",
    "fa17-cs425-g13-09.cs.illinois.edu",
    "fa17-cs425-g13-10.cs.illinois.edu"
};

void heartbeat_checker_handler();
void get_membership_list(bool is_VM0);
void init_machine();
void msg_handler_thread(string msg);
void heartbeat_sender_handler();
void heartbeat_checker_handler();
void update_hb_targets(bool haveLock);
string int_to_string(int num);
int string_to_int(string str);
void print_membership_list();

bool isJoin;
std::mutex isJoin_lock;


/////////////////////////////


#define NUM_VMS			10
#define NUM_REPLICAS	4
#define PORT_STABLILIZATION_MSG 4000       // All connection used for stablilization use this as dest port

// Membership list ml
using replicas_t = std::set<int>;
using row_num_t = int;
using vm_id_t = int;


int master_id;
int master1_id;
int master2_id;


// File Updates
// Used in stabilization protocol
struct file_update {
	std::string filename;
	vm_id_t M_y;
	vm_id_t M_x;
	int version;

	file_updates(std::string filename, vm_id_t M_y, vm_id_t M_x, int version)
		: filename(filename),
		  M_y(M_y),
		  M_x(M_x),
		  version(version) {
	}
};

// File Table Row
struct file_row {
	row_num_t		row;
	std::string		filename;
	vm_id_t			replica;
	int				version;
	vm_id_t			client;

	file_row(
		row_num_t row,
		std::string filename,
		vm_id_t replica,
		int version,
		vm_id_t client
	)
		: row(row),
		  filename(filename),
		  replica(replica),
		  version(version),
		  client(client) {
	}
};

// File Table Maps
std::map			<row_num_t		, file_row> 			file_table;
std::unordered_map	<vm_id_t		, std::set<row_num_t>>	client_map;
std::unordered_map	<std::string	, std::set<row_num_t>>	filename_map;
std::unordered_map	<vm_id_t		, std::set<row_num_t>>	replica_map;

// Next File Version
std::unordered_map<std::string	, int>					next_version_map;

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



int tcp_open_connection(string dest_ip, int dest_port){
    int sockfd, numbytes;
    char buf[MAXDATASIZE];
    struct addrinfo hints, *servinfo, *p;
    int rv;
    char s[INET6_ADDRSTRLEN];
    

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    
    if ((rv = getaddrinfo(dest_ip, dest_port, &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return -1;
    }
    
    // loop through all the results and connect to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype,
                             p->ai_protocol)) == -1) {
            perror("client: socket");
            continue;
        }
        
        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            perror("client: connect");
            continue;
        }
        
        break;
    }
    
    if (p == NULL) {
        fprintf(stderr, "client: failed to connect\n");
        return 2;
    }
    
    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
              s, sizeof s);
    printf("client: connecting to %s\n", s);
    
    freeaddrinfo(servinfo); // all done with this structure
    return sockfd;
}

int tcp_send_string(int sockfd, string str){
    while(len >0){
        int num_send = 0;
        if ((num_send = send(sockfd, buf_ptr, len, 0)) == -1){
            perror("send");
            return -1;
        }
        len -= num_send;
    }
    return 0;
}

//int tcp_close_connection(int sockfd){
//    if(sockfd != -1)
//        close(sockfd);
//}

//This function used to receive only ONE short msg!!! Not to transfer file!!
int tcp_receive_short_msg(int sockfd, int node_id, char* buf){  //Receive msg less than MAX_BUF_LEN
    fd_set r_master, r_fds;
    FD_ZERO(&r_fds);
    FD_SET(sockfd, node_id);
    int numbytes = 0;

    while(1){
        r_fds = r_master;
        if(select(sockfd+1, &r_fds, NULL, NULL, NULL ) == -1){
            return -1;
        }
        if(FD_ISSET(sockfd, r_fds)){
            if ((numbytes = recv(sockfd, buf, MAX_BUF_LEN, 0)) <= 0){
                perror("Receive error!!\n");
                close(sockfd);
                return -1 ;
            }
            return numbytes;
        }
           
        membership_list_lock.lock();
        if(membership_list.find(node_id) == membership_list.end()){
            membership_list_lock.unlock();
            close(sockfd);
            return -1;
        }
        membership_list_lock.unlock();
    }
    return -1;
}



void handle_update(std::atomic<int> & count, std::vector<file_update> & updates, std::mutex & updates_mutex, file_update tup) {
    membership_list_lock.lock();
    if(membership_list.find(tup.M_x) == membership_list.end()){     /*tup.M_x failed*/
        membership_list.unlock();
    }
    else{
        VM_info M_y =  vm_info_map[tup.M_y];
        membership_list_lock.unlock();
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
    }
    count --;
}

// Called whenever a node is detected to fail
void node_fail_handler(vm_id_t node) {
	if(my_vm_info.vm_num == master_id) {        //I'm the Master
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
			replicas_t replicas = route_file(newest_ver_row.filename);
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

		// Replicate the keys of a failed node on client failure
		// Count the number of rows of filename that have the same client
		std::unordered_map<std::string, int> num_client_writes;
		for(auto && i : client_map[node]) {
			num_client_writes[file_table[i].filename]++;
		}
	
		// Replicate iff the client has written twice
		for(auto && [k, v] : num_client_writes) {
			if(v == 2) {
				// Get the row with the newest node and the oldest node
				int newest_ver = -1, oldest_ver = 0x7fffffff;
				row_num_t newest_ver_row, oldest_ver_row;
				for(auto && i : filename_map[k]) {
					if(file_table[i].version > newest_ver) {
						newest_ver = file_table[i].version;
						newest_ver_row = file_table[i].row;
					}
					if(file_table[i].version < oldest_ver) {
						oldest_ver = file_table[i].version;
						oldest_ver_row = file_table[i].row;
					}
				}

				// Get M_y - The node with the newest replica
				vm_id_t M_y = newest_ver_row.replica;
			
				// Get M_x - The node with the oldest replica
				vm_id_t M_x = oldest_ver_row.replica;

				// Schedule update
				updates.emplace_back(k, M_y, M_x, newest_ver);
			}
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
					i.client
				);
			}
            else if(intersection_result.size() >1){
                cout << "Having 2 replicas storing at same VM ???? Something is wrong! \n";
            }
            else{   //If row is already in the table update the version of row that has file and M_x
                file_table[intersection_result[0]].version = i.version;
                file_table[intersection_result[0]].client = i.client;
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
                count++;
                std::thread t(handle_update, count, updates, updates_mutex, *updates.rbegin());
                updates.erase(updates.rbegin());
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
        
        if(node_id == master1_id){
            //Tell master2 to become new master 1 && tell that it will choose VM_k to be master2
            tcp_open_connection()
            
            //Send update to new master1
            //Tell VM_k to become master 2 && Send the whole file table to new master 2
        }
        else if (node_id == master2_id){
            int new_master2_id = -1;
            //choose VM_k to be new master2
            membership_list_lock.lock();
            for(auto it = membership_list.begin(); it != membership_list.end(); it++){
                if(it->first != master_id && it->first != master1_id && it->first != master2_id){
                    new_master2_id = it->first;
                    break;
                }
            }
            if(new_master2_id == -1 ){
                cout << "There is less than 3 VMs in the system!!! This should never happen!!\n";
            }
            VM_info new_master2_info = membership_list[new_master2_id];
            
            if(membership_list.find(master1_id) != membership_list.end()){  //Master 1 is still alive
                VM_info master1_info = membership_list[master1_id];
                membership_list.unlock();
                
                
                
            }
            else{   //master 1 also failed
                membership_list_lock.unlock();
                //Send to new_master2_id msg tell it become new master2 and send file_table
                
            }

            //Tell master 1 that it will choose VM_k to be master2
            //Send update to master1
            //Tell VM_k to become master 2 && Send the whole file table to new master 2
            if(membership_list.find(master1_id) != membership_list.end()){
                VM_info master1_info = membership_list[master1_id];
                membership_list.unlock();
                
                
            }
            else{   //master 1 also failed
                membership_list_lock.unlock();
                
                
            }
            int local_sock = tcp_open_connection(M_y.ip_addr_str, PORT_STABLILIZATION_MSG);
            
            
        }
        else{
            
        }
        //Send msg to master1 and master2 if they are still alive
        int master1_id;
        int master2_id;
        
	} else {
        if(!file_table.empty()){/*I am the next master*/
            
        }

	}
}

int main(int argc, char ** argv) {
	return 0;
}









///////////////////

/*This function update the HB targets based on the current membershiplist
 *input:    haveLock: indicate if having lock or not
 *return:   Nothing
 */
void update_hb_targets(bool haveLock){
    if(haveLock == false){
        membership_list_lock.lock();
        hb_targets_lock.lock();
    }
    
    set<int> new_hb_targets;
    //Get new targets
    
    auto my_it = membership_list.find(my_vm_info.vm_num);
    int count = 0;
    
    //Get sucessors
    for(auto it = next(my_it); it != membership_list.end() && count < NUM_TARGETS/2; it++){
        new_hb_targets.insert(*it);
        count ++;
    }
    
    for(auto it = membership_list.begin(); it != my_it && count < NUM_TARGETS/2; it++){
        new_hb_targets.insert(*it);
        count ++;
    }
    
    //Get predecessors
    if(count == NUM_TARGETS/2){
        count = 0;
        if(my_it != membership_list.begin()){
            for(auto it = prev(my_it); it != membership_list.begin() && count < NUM_TARGETS/2; it--){
                if(new_hb_targets.find(*it) == new_hb_targets.end()){
                    new_hb_targets.insert(*it);
                    count++;
                }
            }
            if(count < (NUM_TARGETS/2)  && new_hb_targets.find(*membership_list.begin()) == new_hb_targets.end()
               && (*membership_list.begin()) != *my_it){
                count++;
                new_hb_targets.insert(*membership_list.begin());
            }
        }
        for(auto it = prev(membership_list.end()); it != my_it && count < NUM_TARGETS/2; it--){
            if(new_hb_targets.find(*it) == new_hb_targets.end()){
                new_hb_targets.insert(*it);
                count++;
            }
        }
    }
    Protocol p ;
    UDP udp;
    //Old targets is not in new targets, set HB to 0
    for(auto it = hb_targets.begin(); it != hb_targets.end(); it++){
        if(new_hb_targets.find(*it) == new_hb_targets.end() && membership_list.find(*it) != membership_list.end()){
            vm_info_map[*it].heartbeat = 0;
            string t_msg = p.create_T_msg();
            udp.send_msg(vm_info_map[*it].ip_addr_str, t_msg);
        }
    }
    
    //Update targets
    hb_targets.erase(hb_targets.begin(), hb_targets.end());
    for(auto it = new_hb_targets.begin(); it != new_hb_targets.end(); it++){
        hb_targets.insert(*it);
    }
    
    // Log Updates
    update_hbs_log << "New targets ";
    for(auto i : hb_targets) {
        update_hbs_log << i << " ";
    }
    update_hbs_log <<= "";
    
    if(haveLock == false){
        hb_targets_lock.unlock();
        membership_list_lock.unlock();
    }
    return;
}

/*This function convert the num from 0-99 to a string with 2 char
 *input:    num: number
 *return:   string of that number
 */
string int_to_string(int num){
    string ret("");
    int first_digit = num/10;
    int sec_digit = num%10;
    ret.push_back((char)(first_digit + '0'));
    ret.push_back((char)(sec_digit + '0'));
    return ret;
}

/*This function convert the string of number to int
 *input:    str: string
 *return:    number
 */
int string_to_int(string str){
    int ret = 0;
    for(int i = 0; i < (int)str.size(); i++){
        ret = ret*10 + (str[i] - '0');
    }
    return ret;
}

/*This function print out the membershiplist
 *input:    none
 *return:    none
 */
void print_membership_list(){
    cout <<"Current membership list: \n";
    for(auto it = membership_list.begin(); it != membership_list.end(); it++){
        cout << "id: " << vm_info_map[*it].vm_num << " --- ip address: " << vm_info_map[*it].ip_addr_str
        << " --- time stamp: "<<vm_info_map[*it].time_stamp << "\n";
    }
}


/*This function send request to VM0 to get membershiplist, and set the membership list based on response
 *Input:    bool:
 *Return:   None
 */
void get_membership_list(bool is_vm0){
    UDP local_udp;
    Protocol local_proc;
    string request_msg = local_proc.create_J_msg();
    if(is_vm0 == false){    //If this is not VM0, send request to VM0 until get the response
        cout << "Requesting membership list from VM0...\n";
        while(1){
            local_udp.send_msg(vm_hosts[0], request_msg);
            string i_msg = local_udp.read_msg_non_block(200);
            if((i_msg.size() == 0) || (i_msg[0] != 'I') ){
                continue;
            }
            int num_node = string_to_int(i_msg.substr(3,2));        //Initialize the membership list based on VM0 response
            if((int)i_msg.size() == num_node*16 + 6){
                local_proc.handle_I_msg(i_msg);
                break;
            }
        }
    }
    else{           //If this is VM0, send msg to all other VMs to to check if they are still alive or not
        bool got_msg = false;
        cout << "Checking if there is any VM still alive...\n";
        for(int i = 1; i < NUM_VMS; i++){
            local_udp.send_msg(vm_hosts[i], request_msg);
            string i_msg = local_udp.read_msg_non_block(200);
            if((i_msg.size() == 0) || (i_msg[0] != 'I') ){
                continue;
            }
            int num_node = string_to_int(i_msg.substr(3,2));        //Set membership list based on the response
            if((int)i_msg.size() == num_node*16 + 6){
                got_msg = true;
                local_proc.handle_I_msg(i_msg);
                break;
            }
        }
        if(got_msg == false){
            string i_msg = local_udp.read_msg_non_block(200);
            if((i_msg.size() > 0) || (i_msg[0] == 'I') ){
                int num_node = string_to_int(i_msg.substr(3,2));
                if((int)i_msg.size() == num_node*16 + 6){
                    got_msg = true;
                    local_proc.handle_I_msg(i_msg);
                }
            }
        }
        if(got_msg == false){
            membership_list.insert(0);
            vm_info_map[0] = my_vm_info;
        }
    }
}

/*This function return ip of this VM
 *Input:    bool:
 *Return:   None
 */
void get_my_ip(){
    struct addrinfo hints, *res, *p;
    int status;
    char ipstr[INET6_ADDRSTRLEN];
    
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    char my_addr[512];
    gethostname(my_addr,512);
    
    if ((status = getaddrinfo(my_addr, NULL, &hints, &res)) != 0) {
        perror("Cannot get my addrinfo\n");
        exit(1);
    }
    
    for(p = res;p != NULL; p = p->ai_next) {
        struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
        void * addr = &(ipv4->sin_addr);
        // convert the IP to a string and print it:
        inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));
        break;
    }
    freeaddrinfo(res); // free the linked list
    
    //Get Bytes from ip address
    unsigned short a, b, c, d;
    sscanf(ipstr, "%hu.%hu.%hu.%hu", &a, &b, &c, &d);
    my_vm_info.ip_addr[0] = (unsigned char) a;
    my_vm_info.ip_addr[1] = (unsigned char) b;
    my_vm_info.ip_addr[2] = (unsigned char) c;
    my_vm_info.ip_addr[3] = (unsigned char) d;
    
    for (int i = 0 ; i < 4; i++) {
        my_vm_info.ip_addr_str.append(to_string((unsigned int) my_vm_info.ip_addr[i]));
        if(i != 3)
            my_vm_info.ip_addr_str.push_back('.');
    }
    
    return;
}



/*This function initilize the vm. It sets my_id, my_id_str, my_logger, my_listener, membership list
 *Input:    None
 *Return:   None
 */
void init_machine(){
    //Init my_id and my_id_str
    get_my_ip();
    bool is_VM0 = false;
    char my_addr[512];
    gethostname(my_addr,512);
    if(strncmp(my_addr, vm_hosts[0].c_str(), vm_hosts[0].size()) == 0){
        is_VM0 = true;
    }
    
    
    ///Initialize my_socket_fd
    struct addrinfo hints, *servinfo, *p;
    int rv;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC; // set to AF_INET to force IPv4
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE; // use my IP
    
    if ((rv = getaddrinfo(my_addr,PORT, &hints, &servinfo)) != 0) {
        perror("getaddrinfo: failed \n");
        exit(1);
    }
    
    // loop through all the results and make a socket
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((my_socket_fd = socket(p->ai_family, p->ai_socktype,
                                   p->ai_protocol)) == -1) {
            perror("server: socket fail");
            continue;
        }
        bind(my_socket_fd, p->ai_addr, p->ai_addrlen);
        break;
    }
    if(p == NULL){
        perror("server: socket fail to bind");
        exit(1);
    }
    freeaddrinfo(servinfo);
    
    //Initialize UDP listener
    my_listener = new UDP();
    
    //Get time_stamp
    time_t seconds;
    seconds = time (NULL);
    my_vm_info.time_stamp = to_string(seconds);
    
    //Get membership_list
    if(is_VM0 == false){
        get_membership_list(false);
    }
    else{
        my_vm_info.vm_num = 0;
        my_vm_info.make_id_str();
        my_vm_info.heartbeat = 0;
        get_membership_list(true);
        
    }
}

/* This function handles msg based on msg types
 *Input:    msg: message
 *Return:   None
 */
void msg_handler_thread(string msg){
    Protocol local_protocol;
    if(msg[0] == 'H'){
        if(msg.size() != H_MESSAGE_LENGTH)
            return;
        local_protocol.handle_H_msg(msg);
    }
    else if(msg[0] == 'N'){
        if(msg.size() != N_MESSAGE_LENGTH)
            return;
        local_protocol.handle_N_msg(msg, false);
        
    }
    else if(msg[0] == 'L'){
        if(msg.size() != L_MESSAGE_LENGTH)
            return;
        local_protocol.handle_L_msg(msg, false);
    }
    else if(msg[0] == 'J'){
        if(msg.size() != J_MESSAGE_LENGTH)
            return;
        local_protocol.handle_J_msg(msg);
    } else if(msg[0] == 'G') {
        local_protocol.handle_G_msg(msg, false);
    }
    else if(msg[0] == 'T'){
        local_protocol.handle_T_msg(msg, false);
    }
    else if(msg[0] == 'Q'){
        local_protocol.handle_Q_msg(msg, false);
        
    }
    //    else if(msg[0] == 'R'){       //Only receive this once when startup. Did it in init_machine
    //        if((msg.size()-2)%12 != 0)
    //            return;
    //        msg_handler.handle_R_msg(msg);
    //    }
}

/* This is thread handler to read and handle msg
 *Input:    None
 *Return:   None
 */
void listener_thread_handler(){
    vector<std::thread> thread_vector;
    while(1){
        isJoin_lock.lock();
        if(isJoin == false){
            isJoin_lock.unlock();
            return;
        }
        isJoin_lock.unlock();
        string msg = my_listener->read_msg_non_block(500);
        
        if(msg.size() == 0){
            continue;
        }
        msg_handler_thread(msg);
    }
}

/* This is thread handler to send heartbeats to pre/successors
 *Input:    None
 *Return:   None
 */
void heartbeat_sender_handler(){
    Protocol local_protocol;
    string h_msg = local_protocol.create_H_msg();
    while(1){
        isJoin_lock.lock();
        if(isJoin == false){
            isJoin_lock.unlock();
            return;
        }
        
        isJoin_lock.unlock();
        UDP local_udp;
        membership_list_lock.lock();
        hb_targets_lock.lock();
        
        //Send HB to all HB targets
        for(auto it = hb_targets.begin(); it != hb_targets.end(); it++){
            if(vm_info_map.find(*it) != vm_info_map.end()){
                local_udp.send_msg(vm_info_map[*it].ip_addr_str, h_msg);
            }
        }
        
        hb_targets_lock.unlock();
        membership_list_lock.unlock();
        //Sleep for HB_TIME
        std::this_thread::sleep_for(std::chrono::milliseconds(HB_TIME));
    }
}

/* This is thread handler to check heartbeats of pre/successors. If timeout, set that node to DEAD, and send msg
 *Input:    None
 *Return:   None
 */
void heartbeat_checker_handler(){
    while(1){
        isJoin_lock.lock();
        if(isJoin == false){
            isJoin_lock.unlock();
            return;
        }
        
        isJoin_lock.unlock();
        membership_list_lock.lock();
        hb_targets_lock.lock();
        time_t cur_time;
        cur_time = time (NULL);
        
        for(auto it = hb_targets.begin(); it != hb_targets.end(); it++){
            std::unordered_map<int,VM_info>::iterator dead_vm_it;
            if((dead_vm_it = vm_info_map.find(*it)) != vm_info_map.end()){
                //If current time - last hearbeat > HB_TIMEOUT, mark the VM as dead and send gossip to other VM
                if(cur_time - vm_info_map[*it].heartbeat > HB_TIMEOUT && vm_info_map[*it].heartbeat != 0){
                    VM_info dead_vm = vm_info_map[*it];
                    vm_info_map.erase(dead_vm_it);
                    membership_list.erase(*it);
                    cout << "Failure Detected: VM id: " << dead_vm.vm_num << " --- ip: "<< dead_vm.ip_addr_str << " --- ts: "<<dead_vm.time_stamp
                    << " --- Last HB: " << dead_vm.heartbeat << "--- cur_time:  " << cur_time << "\n";
                    
                    hb_checker_log << "Failure Detected: VM id: " << dead_vm.vm_num << " --- ip: "<< dead_vm.ip_addr_str << " --- ts: "<<dead_vm.time_stamp
                    << " --- Last HB: " << dead_vm.heartbeat << "--- cur_time:  " <<= cur_time;
                    print_membership_list();
                    update_hb_targets(true);
                    if(membership_list.size() > 1){
                        Protocol p;
                        p.gossip_msg(p.create_L_msg(dead_vm.vm_num), true);
                    }
                }
            }
        }
        hb_targets_lock.unlock();
        membership_list_lock.unlock();
    }
}

int main(){
    isJoin = false;
    
    std::thread listener_thread;
    std::thread heartbeat_sender_thread;
    std::thread heartbeat_checker_thread;
    cout << "Type JOIN to join the system!\n";
    cout << "Type QUIT to stop the system!\n";
    cout << "Type ML to print membership list\n";
    cout << "Type MyVM to print this VM's information\n";
    cout << "-----------------------------\n";
    
    //Main while Loop
    while(1){
        string input;
        cin >> input;
        if(strncmp(input.c_str(), "JOIN", 4) == 0){ //Join the system
            isJoin_lock.lock();
            //If user want to join the system
            if(isJoin == true){
                cout << "VM is running!!!\n";
                isJoin_lock.unlock();
                continue;
            }
            isJoin = true;
            isJoin_lock.unlock();
            
            //Initialize the VM
            init_machine();
            cout <<"-----------Successfully Initialize-----------\n";
            cout << "My VM info: id: " << my_vm_info.vm_num << " --- ip: "<< my_vm_info.ip_addr_str << " --- ts: "<<my_vm_info.time_stamp<<"\n";
            print_membership_list();
            //Start all threads
            listener_thread = std::thread(listener_thread_handler);
            heartbeat_sender_thread = std::thread(heartbeat_sender_handler);
            heartbeat_checker_thread = std::thread(heartbeat_checker_handler);
        }
        else if(strncmp(input.c_str(), "QUIT", 4) == 0){    //Quit program
            isJoin_lock.lock();
            if(isJoin == false){
                cout << "VM is NOT running!!!\n";
                isJoin_lock.unlock();
                continue;
            }
            //Set flag to false to stop all threads
            isJoin = false;
            
            cout <<"Quitting the Program..\n";
            isJoin_lock.unlock();
            
            Protocol p;
            UDP udp;
            membership_list_lock.lock();
            hb_targets_lock.lock();
            
            //Send msg to notify other VM before quitting
            string t_msg = p.create_T_msg();
            for(auto it = hb_targets.begin(); it != hb_targets.end(); it++){
                udp.send_msg(vm_info_map[*it].ip_addr_str, t_msg);
            }
            p.gossip_msg(p.create_Q_msg(), true);
            hb_targets_lock.unlock();
            membership_list_lock.unlock();
            break;
        }
        else if(strncmp(input.c_str(), "ML", 2) == 0){            //Print membershiplist
            isJoin_lock.lock();
            if(isJoin == false){
                cout << "VM is NOT running!!!\n";
                isJoin_lock.unlock();
                continue;
            }
            
            isJoin_lock.unlock();
            membership_list_lock.lock();
            print_membership_list();
            membership_list_lock.unlock();
        }
        else if(strncmp(input.c_str(), "MyVM", 4) == 0){            //Print VM info
            isJoin_lock.lock();
            if(isJoin == false){
                cout << "VM is NOT running!!!\n";
                isJoin_lock.unlock();
                continue;
            }
            
            cout << "My VM info: id: " << my_vm_info.vm_num << " --- ip: "<< my_vm_info.ip_addr_str << " --- ts: "<<my_vm_info.time_stamp<<"\n";
            isJoin_lock.unlock();
        }
    }
    
    cout << "Quit Successfully\n";
    
    //Wait for all other threads to stop
    listener_thread.join();
    heartbeat_sender_thread.join();
    heartbeat_checker_thread.join();
    
    my_logger.write_to_file("vm_log");
    
    
    return 0;
}
















