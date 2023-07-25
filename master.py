import os
import socket
import threading
import time
import select
#from word_count import Word_Count

def word_count(new_socket):
    global in_progress

    global map_phase
    global map_tasks

    global shuffle_phase
    global shuffle_tasks
    global shuffle_counts

    global reduce_tasks
    global reduce_counts
    while True:
        time.sleep(0.1) 
        message = "status"
        new_socket.sendall(message.encode())
        response = new_socket.recv(1024)
        if(response == b"0") and num_workers == n:
            if len(map_tasks)!=0 and map_phase:
                map_task = map_tasks.pop()
                message = f"map {map_task}"
                new_socket.sendall(message.encode())
                if len(map_tasks) == 0:
                    shuffle_tasks = os.listdir("map_intermediates/")
                    map_phase = False
            
            elif len(shuffle_tasks)!=0 and shuffle_phase:
                if shuffle_counts == 0:
                    shuffle_tasks = os.listdir("map_intermediates/")
                shuffle_task = shuffle_tasks.pop()
                message = f"shuffle {shuffle_task}"
                new_socket.sendall(message.encode())
                shuffle_counts+=1 
                if len(shuffle_tasks) == 0:
                    reduce_tasks = os.listdir("map_intermediates/")
                    map_phase = False
            
            elif len(reduce_tasks) != 0:
                if reduce_counts == 0:
                    reduce_tasks = os.listdir("shuffled_data")
                reduce_task = reduce_tasks.pop()
                message = f"reduce {reduce_task}"
                new_socket.sendall(message.encode())
                reduce_counts+=1

            else:
                new_socket.sendall("exit".encode())
                new_socket.close()
                in_progress = False
                break
        else:
            continue

def partitioner(num_tasks):
    raw_data = os.listdir("raw_data/")
    for file in raw_data:
        with open(f"raw_data/{file}", 'r') as f:
            content = f.readlines()
    
    content = ' '.join([''.join(i).replace('\n', '') for i in content])

    partitions = []

    content = content.split(" ")

    while '' in content:
        content.remove('')

    total_size = len(content)
    for i in range(0,total_size,total_size//num_tasks):
        temp_arr = content[i:i+total_size//num_tasks]
        temp_str = ""
        for j in temp_arr:
            temp_str+=j
            temp_str+=" "
        partitions.append(temp_str)

    for i in range(len(partitions)):
        with open(f'partitioned_data/{i+1}.txt','w') as f:
            f.write(partitions[i])

def main() -> None:
    global n
    n = 2 #number of workers needed

    to_clear = os.listdir("partitioned_data/")
    for file in to_clear:
        os.system(f"rm -r partitioned_data/{file}")

    to_clear = os.listdir("map_intermediates/")
    for file in to_clear:
        os.system(f"rm -r map_intermediates/{file}")

    to_clear = os.listdir("shuffled_data/")
    for file in to_clear:
        os.system(f"rm -r shuffled_data/{file}")
    
    to_clear = os.listdir("output/")
    for file in to_clear:
        os.system(f"rm -r output/{file}")

    partitioner(3)

    HOST = '127.0.0.1'
    PORT = 12346
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((HOST,PORT))
    sock.listen(5)
    print(f"Listening on {PORT}")

    global num_workers
    num_workers = 0
    workers = []

    global map_tasks
    map_tasks = os.listdir("partitioned_data/")

    global shuffle_tasks
    shuffle_tasks = os.listdir("map_intermediates/")

    global map_phase
    map_phase = True

    global shuffle_phase
    shuffle_phase = True
    global shuffle_counts
    shuffle_counts = 0

    global reduce_tasks
    reduce_tasks = os.listdir("shuffled_data/")
    global reduce_counts
    reduce_counts = 0

    global in_progress
    in_progress = True

    while num_workers < n:
        new_socket, addr = sock.accept()
        workers.append([new_socket,addr])
        print("Accepted connection")
        num_workers += 1
        #model = Word_Count()
        #worker_thread = threading.Thread(target=model.run, args=(new_socket,))
        worker_thread = threading.Thread(target=word_count, args=(new_socket,))
        worker_thread.start()
    
    while in_progress:
        time.sleep(1)
    
    sock.close()
    raise SystemExit(0)

if __name__ == "__main__":
    main()