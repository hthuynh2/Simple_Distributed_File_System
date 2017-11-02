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

	file_row(
		row_num_t row,
		std::string filename,
		vm_id_t replica,
		int version,
	)
		: row(row),
		  filename(filename),
		  replica(replica),
		  version(version) {
	}
};

// File Table Maps
std::map			<row_num_t		, file_row> 			file_table;
std::unordered_map	<std::string	, std::set<row_num_t>>	filename_map;
std::unordered_map	<vm_id_t		, std::set<row_num_t>>	replica_map;

// Next File Version
std::unordered_map<std::string	, int>					next_version_map;


struct file_struct{
    string file_name;       //If in delivered_map. file_name = file name; If in buffer_file_id_map: file_name = file name + version
    int version;
}


mutex delivered_file_map_lock;
map<string, file_struct> delivered_file_map;

mutex buffer_file_map_lock;
map<string, file_struct> buffer_file_map;       //key = "file_name" + "version"



mutex waiting_to_handle_fail_id_lock;
int waiting_to_handle_fail_id = -1;
int last_failed_node = -1;

mutex file_table_lock;
mutex next_version_map_lock;




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
















