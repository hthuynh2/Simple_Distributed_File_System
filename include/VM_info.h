//
//  VM_info.hpp
//  
//
//  Created by Hieu Huynh on 10/7/17.
//

#ifndef VM_info_h
#define VM_info_h

using namespace std;
#include <stdio.h>
#include <string>

class VM_info{
public:
    int vm_num;             //id number given by VM0 when join the system
    unsigned char ip_addr[4];
    string ip_addr_str; //string of ip address
    string time_stamp;  //Time stamp of the VM
    string id_str;  //This includes id, ip, timestamp
    long heartbeat;
    
    VM_info();
    VM_info(int id_, unsigned char* ip_addr_, string time_stamp_);
    VM_info(string id_str_);
    void make_id_str();  // Make id_str from vm_num,ip_addr, and time_stamp

};

#endif /* VM_info_hpp */

