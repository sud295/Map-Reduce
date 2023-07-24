import os
import pickle
import socket
import threading

def mapper(file) -> None:
    global status
    status = "1"
    with open(f"partitioned_data/{file}",'r') as input_file:
        temp_store = []
        out_index = len(os.listdir("map_intermediates/"))
        with open(f"map_intermediates/{out_index}.pickle",'wb') as output_file:
            lines = input_file.readlines()
            for content in lines:
                items = content.split(" ")
                while '' in items:
                    items.remove('')
                for val in items:
                    temp_store.append(tuple([val,1]))
            temp_store = tuple(temp_store)
            pickle.dump(temp_store,output_file)
    status = "0"

def reducer(file):
    global status
    status = "1"

    with open(f"shuffled_data/{file}","r") as f:
        content = f.readline()

    content = content.split(",")

    while '' in content:
        content.remove('')

    total_count = 0

    for i in content:
        total_count+=int(i)

    with open(f"output/{file}","w") as o_f:
        o_f.write(str([file[:-4],total_count]))

    status = "0"

def shuffler(file) -> None: #,client_socket
    global status
    status = "1"
    with open(f"map_intermediates/{file}","rb") as input_file:
        data = pickle.load(input_file)
        for pair in data:
            with open(f"shuffled_data/{pair[0]}.txt", 'a') as output_file:
                output_file.write(str(pair[1])+",")
    status = "0"

def worker():
    global status
    status = "0" #0 is free, 1 is busy

    HOST = '127.0.0.1'
    PORT = 12346

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    client_socket.connect((HOST, PORT))
    print("Connected")

    while True: #add vars here to track phases (map phase etc.)
        message = client_socket.recv(1024)
        if message == b"status":
            client_socket.sendall(status.encode())
        elif b"map" in message:
            decoded_message = message.decode()
            decoded_message = decoded_message[4:]
            print(f"Mapping {decoded_message}")
            map_thread = threading.Thread(target = mapper, args = (decoded_message,))
            map_thread.start()
        elif b"shuffle" in message:
            decoded_message = message.decode()
            decoded_message = decoded_message[8:]
            print(f"Shuffling {decoded_message}")
            shuffle_thread = threading.Thread(target = shuffler, args = (decoded_message,)) #client_socket,
            shuffle_thread.start()
        elif b"reduce" in message:
            decoded_message = message.decode()
            decoded_message = decoded_message[7:]
            print(f"Reducing {decoded_message}")
            shuffle_thread = threading.Thread(target = reducer, args = (decoded_message,)) #client_socket,
            shuffle_thread.start()
        elif message == b"exit":
            client_socket.close()
            raise SystemExit(0)

if __name__ == "__main__":
    worker()