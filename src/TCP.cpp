

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
        return -1;
    }
    
    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
              s, sizeof s);
    printf("client: connecting to %s\n", s);
    
    freeaddrinfo(servinfo); // all done with this structure
    return sockfd;
}

int tcp_send_string(int sockfd, string str){
    len = str.size();
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
