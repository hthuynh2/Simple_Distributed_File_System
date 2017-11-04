
void op_handler_thread(int socket_fd){
    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    struct timeval timeout_tv;
    timeout_tv.tv_sec = 10;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0) <= 0 ){
        close(socket_fd);
        cout << "op_handler_thread: Receive error!!\n";
        return;
    }
    else{
        string msg(buf, numbytes);
        if(strncmp(msg.c_str(), "RFT", 3) == 0){
            handle_RFT_msg();
        }
        else if(strncmp(msg.c_str(), "RT", 2) == 0){
            cout << "op_handler_thread: Receive RT msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in read_at_client
        }
        else if(strncmp(msg.c_str(), "RS", 2) == 0){
            cout << "op_handler_thread: Receive RS msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in read_at_client
        }
        else if(strncmp(msg.c_str(), "RR", 2) == 0){
            cout << "op_handler_thread: Receive RR msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in master_thread
        }
        else if(strncmp(msg.c_str(), "WA", 2) == 0){
            cout << "op_handler_thread: Receive WA msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in write_at_client
        }
        else if(strncmp(msg.c_str(), "WR", 2) == 0){
            cout << "op_handler_thread: Receive WA msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in master_thread
        }
        else if(strncmp(msg.c_str(), "WT", 2) == 0){
            cout << "op_handler_thread: Receive WT msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in write_at_client
        }
        else if(strncmp(msg.c_str(), "WS", 2) == 0){
            cout << "op_handler_thread: Receive WS msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in write_at_client
        }
        else if(strncmp(msg.c_str(), "FC", 2) == 0){
            if(msg.size() <= 4){
                cout << "op_handler_thread: " << "Error with msg FC\n";
            }
            else {
                if(handle_FC_msg(socket_fd, msg) == true){
                    string file_name = msg.substr(4);
                    if(receive_and_store_file(socket_fd, file_name) == true){
                        cout << "op_handler_thread: " << "Receive file correctly.\n";
                    }
                    else{
                        cout << "op_handler_thread: " << "Error when Receive file.\n";
                    }
                }
            }
        }
        else if(strncmp(msg.c_str(), "DF", 2) == 0){    //Deliver file
            handle_DF_msg(msg);
        }
        else if(strncmp(msg.c_str(), "RFR", 2) == 0){   //Remove file
            handle_RFR_msg(msg);
        }
        else {
            cout << "op_handler_thread: " << "Received undefined msg:  "<< msg << "\n";
        }
    }
    close(socket_fd);
    return;
}


/* This is thread handler to read and handle msg
 *Input:    None
 *Return:   None
 */
 void op_listening_thread(){
     string port = OP_PORT;
     int sockfd = tcp_bind_to_port(OP_PORT);

     if (listen(sockfd, 10) == -1) {
         perror("listen");
         exit(1);
     }
     while(1){
         sin_size = sizeof their_addr;
         new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
         if (new_fd == -1) {
			perror("accept");
			continue;
		 }
         inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *)&their_addr),s, sizeof s);
         printf("server: got connection from %s\n", s);
         thread new_thread(op_handler_thread, new_fd);
     }
}



void master_handler_thread(int socket_fd){
    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    struct timeval timeout_tv;
    timeout_tv.tv_sec = 10;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0) <= 0 ){
        close(socket_fd);
        cout << "master_handler_thread: Receive error!!\n";
        return;
    }
    else{
        string msg(buf, numbytes);
        if(strncmp(msg.c_str(), "WR", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding WR msg\n";
            handle_WR_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "WA", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding WA msg\n";
            handle_WA_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "RR", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding RR msg\n";
            handle_RR_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "DR", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding DR msg\n";
            handle_DR_msg(msg, socket_fd, true);
        }
        else if(strncmp(msg.c_str(), "LS", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding DS msg\n";
            handle_LS_msg(socket_fd, msg, true);
        }
        else {
            cout << "master_handler_thread: " << "Received undefined msg:  "<< msg << "\n";
        }
    }
}

void master_listening_thread(){
    string port = MASTER_PORT;
    int sockfd = tcp_bind_to_port(MASTER_PORT);

    if (listen(sockfd, 10) == -1) {
        perror("listen");
        exit(1);
    }
    while(1){
        sin_size = sizeof their_addr;
        new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
        if (new_fd == -1) {
           perror("accept");
           continue;
        }
        inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *)&their_addr),s, sizeof s);
        printf("server: got connection from %s\n", s);
        thread new_thread(master_handler_thread, new_fd);
    }
}




void stabilization_handler_thread(int socket_fd){
    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    struct timeval timeout_tv;
    timeout_tv.tv_sec = 10;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0) <= 0 ){
        close(socket_fd);
        cout << "Receive error!!\n";
        return;
    }
    else{
        string msg(buf, numbytes);
        if(strncmp(msg.c_str(), "FTR", 3) == 0){
            cout << "Stabilization_thread: " << "Handling FTR msg\n";
            handle_FTR_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "FC", 2) == 0){
            cout << "Stabilization_thread: " << "Handling FC msg\n";

            if(msg.size() <= 4){
                cout << "Stabilization_thread: " << "Error with msg FC\n";
            }
            else {
                if(handle_FC_msg(socket_fd, msg) == true){
                    string file_name = msg.substr(4);
                    if(receive_and_store_file(socket_fd, file_name) == true){
                        cout << "Stabilization_thread: " << "Receive file correctly.\n";
                    }
                    else{
                        cout << "Stabilization_thread: " << "Error when Receive file.\n";
                    }
                }
            }
        }
        else if(strncmp(msg.c_str(), "MU", 2) == 0){
            cout << "Stabilization_thread: " << "Handling MU msg\n";
            handle_MU_msg(msg);
        }
        else if(strncmp(msg.c_str(), "M", 1) == 0){
            cout << "Stabilization_thread: " << "Handling M msg\n";
            handle_M_msg(msg);
        }
        else if(strncmp(msg.c_str(), "FTD", 3) == 0){
            cout << "Stabilization_thread: " << "Handling FTD msg\n";
            //Need to check if this is master 1 or 2???
            handle_FTD_msg(msg);
        }
        else if(strncmp(msg.c_str(), "FU", 3) == 0){
            cout << "Stabilization_thread: " << "Handling FU msg\n";
            //Need to check if this is master 1 or 2???
            handle_FU_msg(msg);
        }
        else {
            cout << "Stabilization_thread: " << "Received undefined msg:  "<< msg << "\n";
        }
    }
    close(socket_fd);
    return;
}

/* This is thread handler to read and handle msg
 *Input:    None
 *Return:   None
 */
 void stabilization_listening_thread(){
     string port = STABILIZATION_PORT;
     int sockfd = tcp_bind_to_port(STABILIZATION_PORT);

     if (listen(sockfd, 10) == -1) {
         perror("listen");
         exit(1);
     }
     while(1){
         sin_size = sizeof their_addr;
         new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
         if (new_fd == -1) {
			perror("accept");
			continue;
		 }
         inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *)&their_addr),s, sizeof s);
         printf("server: got connection from %s\n", s);
         thread new_thread(stabilization_handler_thread, new_fd);
     }
}
