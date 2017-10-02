# Intelligent-Parallel-File-Processing-System

Goal:  Parallel processing of a file in a distributed system based on the size of the file using map reduce. 
 
Challenges: 
1. Determining the number of servers required to process the file based on the size of the file (i.e. performance and scalability). 
2. Splitting the file data into independent chunks (key-value pair) which are processed by the map tasks in a completely parallel manner over a group of servers. 
3. Reducing all the independent chunks and assembling those chunks based on the same key and producing the final result. 
 
Solution to the above challenges: 
1. We won’t assign random number of servers to process a particular file. Rather, we will be predefining the range for various sizes of the files that will help in determining the required number of servers needed to process those files. This will help us in achieving scalability (For instance, if the file size in between 1-2 MB use 2 servers, 2-3 MB use 3 servers and so on). We will not divide the file if the size of the file is small, as computational time will be high if we process this file using map reduce compared to process that file without map reduce. So, we will use map reduce to process only the large file whose size is greater than the predefined threshold. Thus, we will improve the performance for processing the different size files.    
2. We will use a master that will divide the file into independent chunks and map each chunk to the respective server where the mapper function will generate the key value pair for the assigned chunk. 
3. We will use a reducer function that will assemble all the values of the same key from all the servers that are performing mapping functions and do further computation on it to get the result. 
 
Demo: 
1. User will be given an option to choose a file from the list of files. 
2. Based on the file selection, we will display the information of file size and the number of servers needed for processing. 
3. Then, display the size of newly created independent chunks at each server. 
4. Now we will display the time required to process each chunk at the respective server. 
5. Then we will display notification message when all the intermediate results of chunks are combined at one server. 
6. Later, output the total time taken to process the file selected in step 1 using map reduce. 
7. Show input file and the generated output file using map reduce.


a) Steps to run the program 
1) Uncompress the zip file. 
2) Create seven folders for each glados, kansas, kinks, newyork, reddwarf, jopline, doors which represent multiple servers of the network. 
3) Store the Colors.java file in all the folders. 
4) Store the main bootstrap.java file along with the files to be processed in the newyork folder. 
5) Store the Process_Mapreduce.java file in the remaining folder. These servers in future will be behaving either as mapper or reducer for processing the file. 
6) Compile and run all the files. 
7) View the console and follow the instructions to view the difference in time consumed while processing the file on single server and while processing it using map-reduce framework


b) Summary: 
We designed our own map-reduce algorithm. Our algorithm based on the file size decides the number of mappers to be used to complete the job. After every job completion we keep a track of slow mapper and ensure that it will be used as reducer for next job and not as a mapper. We provide the statistics of the time taken to complete the job in the single machine and a set of machines using map-reduce algorithm.
