
"""
Created on 2017/10/05
Authors: Don Scoffield, Ferndando Dias 
"""

import os, struct, time

from my_tcpip import tcpip_server, TCPIP_server_error
from my_helpers import write_lines_to_disk, convert_time_to_string, coordinated_time

# ============================================================================
#                                Inputs
# ============================================================================

num_rtds_time_steps_in_each_status_message_record = 50
write_to_disk_iteration_time_sec = 60

#--------------------------

base_path = '/home/linux1/Desktop/mux_demux'
path_to_output_files = base_path + '/Output_Files'
path_to_input_files = base_path + '/Input_Files'
rtds_msg_structure_filepath = path_to_input_files + '/rtds_message_structure.csv'

rt_process_is_fifo_not_rr = True
rt_process_priority = 95

warning_PQ_response_count_less_than_expected_initial_delay_sec = 60
max_num_error_msgs_to_write_each_data_log_iteration = 4
sleep_time_after_sending_Vrms_f_sec = 0.0001
end_sleep_time_in_sec = 0.0005
pi_max_time_to_wait_to_send_msg_sec = 0.002
pi_max_time_to_wait_to_recieve_msg_sec  = 0.012

pi_server_ip_address = '141.221.118.70'
pi_server_port = 5700
pi_max_num_clients = 100
pi_send_poll_time_sec = 0.0001
pi_receive_poll_time_sec = 0.0001

rtds_server_ip_address = '141.221.118.70'
rtds_server_port = 12000
rtds_send_poll_time_sec = 0.0001
rtds_receive_poll_time_sec = 0.0001
rtds_max_time_to_wait_to_send_msg_sec = 3600*60*24*365 # wait a year
rtds_max_time_to_wait_to_recieve_msg_sec = 3600*60*24*365 # wait a year

# ============================================================================
#                           Read Input File
# ============================================================================
def read_rtds_msg_structure(filepath):
    file_obj = open(filepath, 'r')
    ip_addresses = file_obj.readlines()
    file_obj.close()
    
    del ip_addresses[0]
    ip_addresses = [x.strip() for x in ip_addresses]
    num_records = len(ip_addresses)
    num_unique_ip_addresses = len(set(ip_addresses))
    pi_card_order = ip_addresses
    
    if num_records != num_unique_ip_addresses:
        file_format_error = 'Duplicate IP addressses in rtds_message_structure.csv.'
    else:
        file_format_error = None
    
    return (pi_card_order, file_format_error)
    
# ============================================================================
#                           RTDS Server
# ============================================================================

coordinated_time_obj = coordinated_time(0)
(rtds_msg_pi_card_order, file_format_error) = read_rtds_msg_structure(rtds_msg_structure_filepath)    
num_pi_cards = len(rtds_msg_pi_card_order)

if file_format_error != None:
    print(file_format_error)
else:
    #-----------------------------------------    
    #        Set Process Priority
    #-----------------------------------------    
    if rt_process_is_fifo_not_rr:
        scheduling_policy = '-f'    # -f = FIFO
    else:
        scheduling_policy = '-r'    # -r = Round Robbin
    
    set_rt_priority = 'sudo chrt {} -p {} {}'.format(scheduling_policy, rt_process_priority, os.getpid())
    os.system(set_rt_priority)
    
    #-----------------------------------------
    #       Initialize Server Object
    #-----------------------------------------    
    rtds_server_obj = tcpip_server(rtds_server_ip_address, rtds_server_port, 1, coordinated_time_obj, rtds_send_poll_time_sec, rtds_receive_poll_time_sec, rtds_max_time_to_wait_to_send_msg_sec, rtds_max_time_to_wait_to_recieve_msg_sec)
    pi_server_obj = tcpip_server(pi_server_ip_address, pi_server_port, pi_max_num_clients, coordinated_time_obj, pi_send_poll_time_sec, pi_receive_poll_time_sec, pi_max_time_to_wait_to_send_msg_sec, pi_max_time_to_wait_to_recieve_msg_sec)
    
    #-----------------------------------------    
    #        Initialize Output Files
    #-----------------------------------------    
    pi_warnings_file = write_lines_to_disk(path_to_output_files, 'ServerWarnings_pi.csv')
    pi_warnings_file.add_line('time_now, datetime_now, ip_address, warning_message')
    
    warnings_file = write_lines_to_disk(path_to_output_files, 'ServerWarnings.csv')
    warnings_file.add_line('time_now, datetime_now, warning_message')
    
    socket_create_delete_file = write_lines_to_disk(path_to_output_files, 'SocketCreateDelete.csv')
    socket_create_delete_file.add_line('time_now, datetime_now, ip_address, port, message')
        
    msg_val = 'time_now, datetime_now, numRTDSSockets, num_rtds_timesteps, avg_RTDS_rt_delay_sec, min_RTDS_rt_delay_sec, max_RTDS_rt_delay_sec'
    msg_val += ', avg_mux_de_mux_rt_delay_sec, min_mux_de_mux_rt_delay_sec, max_mux_de_mux_rt_delay_sec'
    msg_val += ', avg_P_feeder_MW, min_P_feeder_MW, max_P_feeder_MW, avg_Q_feeder_MVAR, min_Q_feeder_MVAR, max_Q_feeder_MVAR'
    msg_val += ', avg_P_pev_MW, min_P_pev_MW, max_P_pev_MW, avg_Q_pev_MVAR, min_Q_pev_MVAR, max_Q_pev_MVAR'
    msg_val += ', avg_f_Hz, min_f_Hz, max_f_Hz'
    for ip_address in rtds_msg_pi_card_order:
        tmp = '_V_' + ip_address[-3:]
        msg_val += ', avg' + tmp + ', min' + tmp + ', max' + tmp
        
    status_file = write_lines_to_disk(path_to_output_files, 'Status.csv')
    status_file.add_line(msg_val)
    
    #-----------------------------------------
    #       Initialize Loop Variables
    #-----------------------------------------
    # pi_state_vals[sock_id] = [msg_time_id, P, Q, rt_delay_sec]
    
    rtds_socket_id = None
    pi_socket_ids = []
    pi_state_vals = {}
    sockets_timedout_on_recieve = []
    
    Vrms_msg_vals = {}
    last_sent_value = {}
    for ip_address in rtds_msg_pi_card_order:
        last_sent_value[ip_address] = (0, 0)
        Vrms_msg_vals[ip_address] = (0, 10000, 0, 0)
        
    other_msg_vals = {}
    other_msg_vals['P_feeder_MW'] = (0, 100000, 0, 0)
    other_msg_vals['Q_feeder_MVAR'] = (-100000, 100000, 0, 0)
    other_msg_vals['P_pev_MW'] = (0, 100000, 0, 0)
    other_msg_vals['Q_pev_MVAR'] = (-100000, 100000, 0, 0)
    other_msg_vals['f_Hz'] = (0, 100000, 0, 0)
    other_msg_vals['RTDS_rt_delay_sec'] = (0, 100000, 0, 0)
    other_msg_vals['mux_de_mux_rt_delay_sec'] = (0, 100000, 0, 0)
    
    #------------------
    
    num_error_msgs_this_iteration = {}
    num_error_msgs_this_iteration['send_V_pi'] = 0
    num_error_msgs_this_iteration['send_V'] = 0
    num_error_msgs_this_iteration['receive_P_pi'] = 0
    num_error_msgs_this_iteration['receive_P'] = 0
    num_error_msgs_this_iteration['rt_msg_out_of_sinc_pi'] = 0
    num_error_msgs_this_iteration['rt_msg_out_of_sinc'] = 0
    num_error_msgs_this_iteration['all_pi_cards_not_reporting'] = 0
    
    #------------------
    
    time_now = coordinated_time_obj.get_time()
    
    last_time_written_to_disk = time_now
    start_of_simulation_time = time_now
    
    write_status_file = False
    num_rtds_timesteps = 1
    
    while 1:
        #-----------------------------------------------------------------------------------------------------------
        # This loop is set up with the following characteristics:
        #   1. Vrms & f is sent to ALL sockets connected to the server
        #       a) Execution doesn't move on until Vrms & f values have been sent
        #          to all sockets or until a timeout occurs.
        #   2. P & Q is recieved from ALL sockets connected to the server
        #       a) Execution doesn't move on until P & Q values have been recieved
        #          from all sockets or until a timeout occurs.
        #
        #   A. If a timeout occurs on send:
        #       a) Make sure that for the remainder of the current iteration, all sockets that timed-out 
        #          are removed from the list of sockets passed to the tcpip function to recieve P & Q.
        #       b) If note 'a)' is ignored the recieve will also stall until the timeout occurs
        #          because all sockets that timed-out in the sending of Vrms & f will not return P & Q 
        #          since they did not recieve Vrms and f values.
        #
        #   B. If a timeout occurs on received:
        #       a) Add all sockets that timed out on recieved to a list named sockets_timedout_on_recieve
        #       b) At the beginning of the loop, pass all sockets in sockets_timedout_on_recieve to the tcpip
        #          function named recieve_msg_if_available().
        #       c) When one of these socket recieves a message after calling recieve_msg_if_available() remove 
        #          that socket from the sockets_timedout_on_recieve list.
        #       d) Vrms & f should not be sent to sockets in the sockets_timedout_on_recieve list.
        #       e) P & Q should not be recieved from sockets in the sockets_timedout_on_recieve list.
        #           1) When a timeout occurs on received, the P & Q values become buffered.  There are two P & Q
        #              pairs queued up (The P & Q values read will be the previous value not the 
        #              current value).  The process described above will correct this situation.
        #-----------------------------------------------------------------------------------------------------------
        
        timestep_begin_time = coordinated_time_obj.get_time()
        time_string = '{}, {}, '.format(timestep_begin_time, convert_time_to_string(timestep_begin_time))
        
        #===================================================
        #     Update List sockets_timedout_on_recieve
        #===================================================
        if 0 < len(sockets_timedout_on_recieve):
            (received_msgs, received_msg_times, closed_socket_ids, do_not_exist_socket_ids) = pi_server_obj.recieve_msg_if_available(sockets_timedout_on_recieve, True, 16)
            
            #-------------------------
            #  Remove closed sockets
            #-------------------------
            closed_socket_ids.extend(do_not_exist_socket_ids)
            for sock_id in closed_socket_ids:
                pi_socket_ids.remove(sock_id)
                sockets_timedout_on_recieve.remove(sock_id)
            
            #-------------------------
            # Remove all sockets that 
            # recieved  a message from
            # sockets_timedout_on_recieve
            #-------------------------
            for sock_id in received_msgs.keys():
                sockets_timedout_on_recieve.remove(sock_id)
            
        #===================================================
        #     Check for New Client Connection Requests
        #===================================================        
        if rtds_socket_id == None:
            tmp_socks = rtds_server_obj.accept_TCPIP_client_sockets()            
            if 0 < len(tmp_socks):
                rtds_socket_id = tmp_socks[0]
                
        pi_socket_ids.extend(pi_server_obj.accept_TCPIP_client_sockets())
        
        #===================================================
        #        Wait to recieve (Vrms, f) from RTDS
        #===================================================       
        if rtds_socket_id != None:
            (received_msgs, received_msg_times, closed_socket_ids, timed_out_socket_ids, do_not_exist_socket_ids, num_poll_iterations) = rtds_server_obj.poll_until_recieve_msg([rtds_socket_id], True, 16+8*num_pi_cards)
            
            #-------------------------
            #  Remove closed sockets
            #-------------------------
            closed_socket_ids.extend(do_not_exist_socket_ids)
            if rtds_socket_id in closed_socket_ids:
                rtds_socket_id == None
                continue
            
            #------------------------------
            #  Process (Vrms, f) from RTDS
            #------------------------------
            msg_time_id = timestep_begin_time
            tmp_bytes = received_msgs[rtds_socket_id]
	        
            pi_state_vals = {}
            msg_dict = {}
			
            (rtds_time_val, RTDS_rt_delay_sec, feeder_P_MW, feeder_Q_MVAR) = struct.unpack('>ffff', tmp_bytes[0:16])
            
            #--------------------------------------
            #             feeder_P_MW
            #--------------------------------------
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['P_feeder_MW']          
            cnt_val += 1
            sum_val += feeder_P_MW
            if feeder_P_MW < min_val: min_val = feeder_P_MW
            if feeder_P_MW > max_val: max_val = feeder_P_MW
            other_msg_vals['P_feeder_MW'] = (max_val, min_val, sum_val, cnt_val)
            
            #--------------------------------------
            #             feeder_Q_MVAR
            #--------------------------------------
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['Q_feeder_MVAR']       
            cnt_val += 1
            sum_val += feeder_Q_MVAR
            if feeder_Q_MVAR < min_val: min_val = feeder_Q_MVAR
            if feeder_Q_MVAR > max_val: max_val = feeder_Q_MVAR
            other_msg_vals['Q_feeder_MVAR'] = (max_val, min_val, sum_val, cnt_val)
            
            #--------------------------------------
            #             RTDS_rt_delay_sec
            #--------------------------------------
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['RTDS_rt_delay_sec']        
            cnt_val += 1
            sum_val += RTDS_rt_delay_sec
            if RTDS_rt_delay_sec < min_val: min_val = RTDS_rt_delay_sec
            if RTDS_rt_delay_sec > max_val: max_val = RTDS_rt_delay_sec
            other_msg_vals['RTDS_rt_delay_sec'] = (max_val, min_val, sum_val, cnt_val)
            
            #--------------------------------------
            #         Process Vrms, f Values
            #--------------------------------------
            index = 16
            record_f_Hz = True
            for sock_id in rtds_msg_pi_card_order:
                (Vrms, f) = struct.unpack('>ff', tmp_bytes[index:index+8])
                bytes_to_write = struct.pack('=dff', msg_time_id, Vrms, f)
                index += 8
                
                if record_f_Hz:
                    record_f_Hz = False
                    (max_val, min_val, sum_val, cnt_val) = other_msg_vals['f_Hz']   
                    cnt_val += 1
                    sum_val += f
                    if f < min_val: min_val = f
                    if f > max_val: max_val = f
                    other_msg_vals['f_Hz'] = (max_val, min_val, sum_val, cnt_val)
            
                (max_val, min_val, sum_val, cnt_val) = Vrms_msg_vals[sock_id]
                cnt_val += 1
                sum_val += Vrms
                if Vrms < min_val: min_val = Vrms
                if Vrms > max_val: max_val = Vrms
                Vrms_msg_vals[sock_id] = (max_val, min_val, sum_val, cnt_val)
                
                if sock_id in pi_socket_ids:
                    if sock_id not in sockets_timedout_on_recieve:
                        msg_dict[sock_id] = bytes_to_write
                        pi_state_vals[sock_id] = [msg_time_id, None, None, None] # pi_state_vals[sock_id] = [msg_time_id, P, Q, rt_delay_sec]
            
            #===================================================
            #           Send (Vrms, f) to pi cards
            #===================================================
            if 0 < len(msg_dict):
                (sent_msg_times, closed_socket_ids, timed_out_socket_ids, do_not_exist_socket_ids, num_poll_iterations) = pi_server_obj.send_messages(msg_dict, True)
                
                #-------------------------
                #  Remove closed sockets  
                #-------------------------
                closed_socket_ids.extend(do_not_exist_socket_ids)
                for sock_id in closed_socket_ids:
                    pi_socket_ids.remove(sock_id)
                    if sock_id in sockets_timedout_on_recieve: sockets_timedout_on_recieve.remove(sock_id)
                    del pi_state_vals[sock_id]
                
                #-------------------------
                #       Timed Out 
                #-------------------------
                for sock_id in timed_out_socket_ids:
                    del pi_state_vals[sock_id]
                    
                    if num_error_msgs_this_iteration['send_V_pi'] < max_num_error_msgs_to_write_each_data_log_iteration:
                        num_error_msgs_this_iteration['send_V_pi'] += 1
                        tmp_msg = time_string + '{}, Vrms & f values have not been sent to pi card.  A wait time has expired (sending Vrms & f).'.format(sock_id)
                        pi_warnings_file.add_line(tmp_msg)
    
                if 0 < len(timed_out_socket_ids):
                    if num_error_msgs_this_iteration['send_V'] < max_num_error_msgs_to_write_each_data_log_iteration:
                        num_error_msgs_this_iteration['send_V'] += 1
                        tmp_msg = time_string + 'Vrms & f values have not been sent to {} pi cards.  A wait time has expired (sending Vrms & f).'.format(len(timed_out_socket_ids))
                        warnings_file.add_line(tmp_msg)
                        print(tmp_msg)
                
                #===================================================
                #               Intermediate Sleep
                #===================================================
                
                if 0 <= sleep_time_after_sending_Vrms_f_sec:
                    time.sleep(sleep_time_after_sending_Vrms_f_sec)
                
                #===================================================
                #             Receive (P, Q)
                #===================================================
                        # Must send list(pi_state_vals.keys()) not pi_socket_ids 
                        #   1. Because the sock_ids that timed out when sending Vrms & f will probably not ever return a value causing 'server_obj.poll_until_recieve_msg' to timeout every iteration.
                        #   2. This may cause some messenges to be out of sinc.
                
                if 0 < len(pi_state_vals):
                    (received_msgs, received_msg_times, closed_socket_ids, timed_out_socket_ids, do_not_exist_socket_ids, num_poll_iterations) = pi_server_obj.poll_until_recieve_msg(list(pi_state_vals.keys()), True, 16)
                    
                    #-------------------------
                    #  Remove closed sockets
                    #-------------------------
                    closed_socket_ids.extend(do_not_exist_socket_ids)
                    for sock_id in closed_socket_ids:
                        pi_socket_ids.remove(sock_id)
                        if sock_id in sockets_timedout_on_recieve: sockets_timedout_on_recieve.remove(sock_id)
                        del pi_state_vals[sock_id]
                    
                    #-------------------------
                    #       Timed Out 
                    #-------------------------
                    for sock_id in timed_out_socket_ids:
                        sockets_timedout_on_recieve.append(sock_id)
                        del pi_state_vals[sock_id]
                        
                        if num_error_msgs_this_iteration['receive_P_pi'] < max_num_error_msgs_to_write_each_data_log_iteration:
                            num_error_msgs_this_iteration['receive_P_pi'] += 1                            
                            tmp_msg = time_string + '{}, P and Q values have not been recieved.  A wait time has expired (Recieving P & Q).'.format(sock_id)
                            pi_warnings_file.add_line(tmp_msg)
    
                    if 0 < len(timed_out_socket_ids):
                        if num_error_msgs_this_iteration['receive_P'] < max_num_error_msgs_to_write_each_data_log_iteration:
                            num_error_msgs_this_iteration['receive_P'] += 1  
                            tmp_msg = time_string + 'P and Q values have not been recieved from {} pi cards.  A wait time has expired (Recieving P & Q).'.format(len(timed_out_socket_ids))
                            warnings_file.add_line(tmp_msg)
                            print(tmp_msg)
                    
                    #------------------------------
                    #  Process P & Q from pi cards
                    #------------------------------
                    # pi_state_vals[sock_id] = [msg_time_id, P, Q, rt_delay_sec]
                    num_out_of_sinc_pi_cards = 0
                    for sock_id, tmp_bytes in received_msgs.items():
                        (msg_time_id1, P, Q) = struct.unpack('=dff', tmp_bytes)
                        msg_time_id2 = pi_state_vals[sock_id][0]
                        
                        if 0.000001 < abs(msg_time_id1 - msg_time_id2):
                            if num_error_msgs_this_iteration['rt_msg_out_of_sinc_pi'] < max_num_error_msgs_to_write_each_data_log_iteration:
                                num_error_msgs_this_iteration['rt_msg_out_of_sinc_pi'] += 1  
                                tmp_msg = time_string + '{}, rt messages are out of sinc.  Either Vrms or P is being buffered.  Time difference in seconds:{}'.format(sock_id, abs(msg_time_id1 - msg_time_id2))
                                pi_warnings_file.add_line(tmp_msg)
                                
                            num_out_of_sinc_pi_cards += 1
                            del pi_state_vals[sock_id]
                            
                        else:
                            rt_delay_sec = received_msg_times[sock_id] - sent_msg_times[sock_id]
                            pi_state_vals[sock_id] = [msg_time_id2, P, Q, rt_delay_sec]
                    
                    if 0 < num_out_of_sinc_pi_cards:
                        if num_error_msgs_this_iteration['rt_msg_out_of_sinc'] < max_num_error_msgs_to_write_each_data_log_iteration:
                            num_error_msgs_this_iteration['rt_msg_out_of_sinc'] += 1  
                            tmp_msg = time_string + 'rt messages are out of sinc in {} pi cards.  Either Vrms or P is being buffered.'.format(num_out_of_sinc_pi_cards)
                            warnings_file.add_line(tmp_msg)                    
                    
                    #===================================================
                    #                Send P & Q to RTDS
                    #===================================================
                    P_total_MW = 0
                    Q_total_MVAR = 0
                    bytes_to_write = struct.pack('>f', rtds_time_val)
                    for sock_id in rtds_msg_pi_card_order:
                        if sock_id in pi_state_vals:
                            (P, Q) =  pi_state_vals[sock_id][1:3]
                            last_sent_value[sock_id] = (P, Q)
                        else:
                            (P, Q) = last_sent_value[sock_id]                        
                        
                        bytes_to_write += struct.pack('>ff', P, Q)
                        P_total_MW += P
                        Q_total_MVAR += Q
                    
                    msg_dict = {}
                    msg_dict[rtds_socket_id] = bytes_to_write
                    (sent_msg_times, closed_socket_ids, timed_out_socket_ids, do_not_exist_socket_ids, num_poll_iterations) = rtds_server_obj.send_messages(msg_dict, True)   
                    
                    #-------------------------
                    #  Remove closed sockets
                    #-------------------------
                    closed_socket_ids.extend(do_not_exist_socket_ids)
                    if rtds_socket_id in closed_socket_ids:
                        rtds_socket_id == None
                        
                #===================================================
                #       Update PEV P_total_MW and Q_total_MVAR
                #===================================================                
                P_total_MW = P_total_MW/1000
                (max_val, min_val, sum_val, cnt_val) = other_msg_vals['P_pev_MW']
                cnt_val += 1
                sum_val += P_total_MW
                if P_total_MW < min_val: min_val = P_total_MW
                if P_total_MW > max_val: max_val = P_total_MW
                other_msg_vals['P_pev_MW'] = (max_val, min_val, sum_val, cnt_val)
                
                Q_total_MVAR = Q_total_MVAR/1000
                (max_val, min_val, sum_val, cnt_val) = other_msg_vals['Q_pev_MVAR']
                cnt_val += 1
                sum_val += Q_total_MVAR
                if Q_total_MVAR < min_val: min_val = Q_total_MVAR
                if Q_total_MVAR > max_val: max_val = Q_total_MVAR
                other_msg_vals['Q_pev_MVAR'] = (max_val, min_val, sum_val, cnt_val)
                
                #===================================================
                #              update rt delay values
                #===================================================
                for sock_id, (msg_time_id, P, Q, rt_delay_sec) in pi_state_vals.items():
                    (max_val, min_val, sum_val, cnt_val) = other_msg_vals['mux_de_mux_rt_delay_sec']
                    cnt_val += 1
                    sum_val += rt_delay_sec
                    if rt_delay_sec < min_val: min_val = rt_delay_sec
                    if rt_delay_sec > max_val: max_val = rt_delay_sec
                    other_msg_vals['mux_de_mux_rt_delay_sec'] = (max_val, min_val, sum_val, cnt_val)
        
        if len(pi_state_vals) != num_pi_cards:
            if warning_PQ_response_count_less_than_expected_initial_delay_sec < coordinated_time_obj.get_time() - start_of_simulation_time:
                if num_error_msgs_this_iteration['all_pi_cards_not_reporting'] < max_num_error_msgs_to_write_each_data_log_iteration:
                    num_error_msgs_this_iteration['all_pi_cards_not_reporting'] += 1  
                    tmp_msg = time_string + 'Only received P and Q values from {} pi cards.  Expecting values from {} pi cards.'.format(len(pi_state_vals), num_pi_cards)
                    warnings_file.add_line(tmp_msg)
                      
        #================================================
        #                   Bookkeeping
        #================================================
        if num_rtds_time_steps_in_each_status_message_record <= num_rtds_timesteps:
            # 'time_now, datetime_now, numRTDSSockets, num_rtds_timesteps, avg_RTDS_rt_delay_sec, min_RTDS_rt_delay_sec, max_RTDS_rt_delay_sec'
            # ', avg_mux_de_mux_rt_delay_sec, min_mux_de_mux_rt_delay_sec, max_mux_de_mux_rt_delay_sec'
            # ', avg_P_feeder_MW, min_P_feeder_MW, max_P_feeder_MW, avg_Q_feeder_MVAR, min_Q_feeder_MVAR, max_Q_feeder_MVAR'
            # ', avg_P_pev_MW, min_P_pev_MW, max_P_pev_MW, avg_Q_pev_MVAR, min_Q_pev_MVAR, max_Q_pev_MVAR'
            # ', avg_f_Hz, min_f_Hz, max_f_Hz'    
            # ', avg_V_xxx, min_V_xxx, max_V_xxx, ...'
            
            msg_list = []
            
            msg_list.append(time_string + '{}, {}'.format(len(pi_socket_ids), num_rtds_timesteps))
            
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['RTDS_rt_delay_sec']
            if cnt_val == 0: avg_val = 0
            else: avg_val = sum_val/cnt_val
            msg_list.append(', {}, {}, {}'.format(avg_val, min_val, max_val))
            
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['mux_de_mux_rt_delay_sec']
            if cnt_val == 0: avg_val = 0
            else: avg_val = sum_val/cnt_val
            msg_list.append(', {}, {}, {}'.format(avg_val, min_val, max_val))
            
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['P_feeder_MW']
            if cnt_val == 0: avg_val = 0
            else: avg_val = sum_val/cnt_val
            msg_list.append(', {}, {}, {}'.format(avg_val, min_val, max_val))
            
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['Q_feeder_MVAR']
            if cnt_val == 0: avg_val = 0
            else: avg_val = sum_val/cnt_val
            msg_list.append(', {}, {}, {}'.format(avg_val, min_val, max_val))
            
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['P_pev_MW']
            if cnt_val == 0: avg_val = 0
            else: avg_val = sum_val/cnt_val
            msg_list.append(', {}, {}, {}'.format(avg_val, min_val, max_val))
            
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['Q_pev_MVAR']
            if cnt_val == 0: avg_val = 0
            else: avg_val = sum_val/cnt_val
            msg_list.append(', {}, {}, {}'.format(avg_val, min_val, max_val))
            
            (max_val, min_val, sum_val, cnt_val) = other_msg_vals['f_Hz']
            if cnt_val == 0: avg_val = 0
            else: avg_val = sum_val/cnt_val
            msg_list.append(', {}, {}, {}'.format(avg_val, min_val, max_val))
            
            for ip_address in rtds_msg_pi_card_order:
                (max_val, min_val, sum_val, cnt_val) = Vrms_msg_vals[ip_address]
                if cnt_val == 0: avg_val = 0
                else: avg_val = sum_val/cnt_val
                msg_list.append(', {}, {}, {}'.format(avg_val, min_val, max_val))
        
            msg_val = ''.join(msg_list)            
            status_file.add_line(msg_val)
            
            #---------------------------------------
            
            num_rtds_timesteps = 0
            
            for ip_address in Vrms_msg_vals:
                Vrms_msg_vals[ip_address] = (0, 10000, 0, 0)
   
            other_msg_vals['P_feeder_MW'] = (0, 100000, 0, 0)
            other_msg_vals['Q_feeder_MVAR'] = (-100000, 100000, 0, 0)
            other_msg_vals['P_pev_MW'] = (0, 100000, 0, 0)
            other_msg_vals['Q_pev_MVAR'] = (-100000, 100000, 0, 0)
            other_msg_vals['f_Hz'] = (0, 100000, 0, 0)
            other_msg_vals['RTDS_rt_delay_sec'] = (0, 100000, 0, 0)
            other_msg_vals['mux_de_mux_rt_delay_sec'] = (0, 100000, 0, 0)
            
        elif last_time_written_to_disk + write_to_disk_iteration_time_sec < timestep_begin_time:
            last_time_written_to_disk = timestep_begin_time
            write_status_file = True
            
            #-------------
            
            socket_create_delete_msgs = pi_server_obj.get_socket_create_delete_msgs()
            for (time_val, ip_address, port, msg_val) in socket_create_delete_msgs:
                line_val = '{}, {}, {}, {}, {}'.format(time_val, convert_time_to_string(time_val), ip_address, port, msg_val)
                socket_create_delete_file.add_line(line_val)
            
            #-------------
            
            warnings_file.write_to_disk()
            pi_warnings_file.write_to_disk()
            socket_create_delete_file.write_to_disk()
            
            #-------------
            
            num_error_msgs_this_iteration['send_V_pi'] = 0
            num_error_msgs_this_iteration['send_V'] = 0
            num_error_msgs_this_iteration['receive_P_pi'] = 0
            num_error_msgs_this_iteration['receive_P'] = 0
            num_error_msgs_this_iteration['rt_msg_out_of_sinc_pi'] = 0
            num_error_msgs_this_iteration['rt_msg_out_of_sinc'] = 0
            num_error_msgs_this_iteration['all_pi_cards_not_reporting'] = 0
        
        elif write_status_file:
            write_status_file = False
            status_file.write_to_disk()
            
        else:                                                                  
            if 0 <= end_sleep_time_in_sec:
                time.sleep(end_sleep_time_in_sec)

        num_rtds_timesteps += 1
