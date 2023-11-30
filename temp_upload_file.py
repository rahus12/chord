
import os
import hashlib
import json
import socket

class FileUploader:
    def __init__(self, local_node):
        self.local_node = local_node

    def upload_file(self, file_path):
        # Generate a unique key for the file
        file_key = hashlib.sha256(file_path.encode()).hexdigest()

        # Find the successor node responsible for storing the file
        successor = self.local_node.find_successor(int(file_key, 16))

        # Read the file and send it to the successor node
        with open(file_path, 'rb') as file:
            file_data = file.read()
            self.send_file(successor.address_.ip, successor.address_.port, file_key, file_data)

        print(f"File '{file_path}' uploaded successfully.")

    def send_file(self, ip, port, key, data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, port))
            command = f"store_file {key}"
            s.sendall(command.encode() + b"\r\n")
            s.sendall(data)
            s.sendall(b"\r\n")

if __name__ == "__main__":
    # Assuming 'local' is an instance of the Local class from chord.py
    uploader = FileUploader(local)
    
    # Specify the path to the file you want to upload
    file_path = "path/to/your/file.txt"

    # Upload the file to the Chord network
    uploader.upload_file(file_path)
