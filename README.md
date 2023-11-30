python-chord
============

# Chord Network Implementation
This repository contains an implementation of a Chord network in Python with text file upload, download and replication.

## Usage
- `$>python create_chord.py $N_CHORD_NODES` to run a DHT that lets you ask questions to random members.

### Number of supported commands?
- `get_successor $node_id` to get the nearest successor node
- `get_predecessor $node_id` to get the nearest predecessor node
- `find_successor $key` to find the nearest successor node in which the key can be stored
- `upload $key $text` to upload the file in node
- `download $key $text` to upload the file in node
- `show_fingers` to display finger_table of current node
- `shutdown` to shutdown one node in chord randomly

The behaviour of the network can be greatly modified by setting the appropriate values 
on `settings.py`.

### How to test?
- `$>python test.py` to check consistency. Tests can fail due to the fact that the network is not stable yet, should work by increasing the rate of updates.


### Distributed Hash Table
A distributed hash table implementation on top of Chord is available in `dht.py`. It 
uses the overlay network provided by Chords algorithms and adds two more commands to
the network, the commands `set` and `get`.

After registering those commands with the appropriate callbacks we have a fairly 
simple DHT implementation that also balances loads according to node joins.

### File Upload, Download, and Replication
The Chord network supports file upload and download functionalities. To utilize these features, follow these guidelines:

## File Upload
To upload a file to the Chord network, use the upload command `create_chord.py #number_of_nodes -> upload 35 hello`. Specify the desired key to upload and text you want to upload.

### File download
To download a file to the Chord network, use the download command `create_chord.py #number_of_nodes -> download 35`. Specify the desired key that you want to download

### Replication
The Chord network handles replication of files for fault tolerance. Upon file upload, the network replicates the file to ensure redundancy and fault tolerance. The replication mechanism ensures data availability even if some nodes fail.

### References
Python implementation of [this paper](http://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf).

### FAQ
If you are running it locally then the output is shown thrice due to replication on single server and multiple nodes.

