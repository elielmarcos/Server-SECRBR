# HOTSPOT para Raspberry: https://github.com/idev1/rpihotspot

import os
from datetime import datetime, timedelta, timezone
import socket
import threading


HEADER = 10
PORT = 12000
#SERVER = socket.gethostbyname(socket.gethostname())
# Server Hotspot
#SERVER = '192.168.137.1'
# Server Local / No-IP
SERVER = '192.168.0.107'
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def recvall(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data


def handle_client(conn, addr):
    print(f"<{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}> NEW CONNECTION: [{addr}] connected.")

    conn.settimeout(15)

    path = None
    file_name = None
    file_size_max = 1800000 # 1.000.000B = 1MB
    file_size = 0
    powermeter = None
    file_csv = None

    #print(os.path.join('c:/', 'pm01' ,'arquivo'+ str(fileC) +'_'+ str(file_count) +'.csv'))

    while True:
        try:
            msg_length = conn.recv(HEADER).decode(FORMAT)

            if powermeter is None:
                powermeter = msg_length[0:4]
            else:

                if powermeter != msg_length[0:4]:
                    print(f"<{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}> POWERMETER ERRO: [{powermeter}:{msg_length[0:4]}]")
                    break

            msg_length = int(msg_length[msg_length.find('&')+1:]) if msg_length.find('&') != -1 else None

            if msg_length is not None:
                if file_csv is None:
                    path = 'C:/DATASET/' + str(powermeter)

                    if not os.path.exists(path):
                        os.makedirs(path)
                    file_name = path + '/' + str(powermeter) + '__' + str(
                        datetime.now().strftime('%Y_%m_%d__%H_%M_%S')) + '.csv'
                    file_csv = open(file_name, 'a', encoding=FORMAT, newline='')

                msg = recvall(conn, msg_length).decode(FORMAT)

                if msg is not None:
                    if msg == DISCONNECT_MESSAGE:
                        break

                    file_size += msg_length

                    if file_size >= file_size_max:
                        file_csv.close()
                        path = 'C:/DATASET/' + str(powermeter)

                        if not os.path.exists(path):
                            os.makedirs(path)
                        file_name = path + '/' + str(powermeter) + '__' + str(
                            datetime.now().strftime('%Y_%m_%d__%H_%M_%S')) + '.csv'
                        file_csv = open(file_name, 'a', encoding=FORMAT, newline='')
                        file_size = msg_length

                    file_csv.write(msg)
                    print(f"<{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}> POWERMETER:[{powermeter}] MSG_SIZE:[{msg_length}B] FILE:[{file_name}] FILE_SIZE:[{file_size}B]")

                #print(f"[{addr}] {msg}")
                #conn.send("Msg received".encode(FORMAT))
        except: #socket.timeout:
            print(f"<{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}> WITHOUT POST: [{addr}] POWERMETER: [{powermeter}]")
            break

    if file_csv is not None:
        file_csv.close()
    conn.close()
    print(f"<{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}> CLOSE SOCKET: [{addr}]  POWERMETER: [{powermeter}]")


def start():
    server.listen()
    print(f"<{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}> LISTENING: Server is listening on [{SERVER}]")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))

        print(f"<{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}> ACTIVE CONNECTIONS: [{threading.activeCount()}]")
        thread.start()

print(f"<{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}> STARTING: Server is starting...")
start()
