
import jdk.internal.dynalink.beans.StaticClass;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/*
 *  @author    Jinesh Dhruv
 *  @author    Nisha Bhnaushali
 *
 * MapReduce Implementation
 * This program will ask user for a file to sort. Then it will sort the file using map reduce approach.
 *
 */

public class Bootstrap extends Thread {
    static int[] new_array; // for merge sort
    static int[] elements; // for merge sort
    static ArrayList<Integer> sorted_numbers; // for merge sort result
    static HashMap<String, Double> file_info = new HashMap<String, Double>(); // to store key value info of splitting work
    static ArrayList<Integer> given_input = new ArrayList<Integer>(); // original input from file
    static HashMap<Integer, ArrayList<Integer>> key_value = new HashMap<Integer, ArrayList<Integer>>(); // key value pair
    static HashMap<Integer, String> map = new HashMap<>(7); // to store ip-address of all process
    static HashMap<Integer, Integer> port_map = new HashMap<>(7);// port number to connect to all process
    static String ip1, ip2, ip3, ip4, ip5,ip6,ip7; // ip address
    static int id; // process_id
    static int port; // port number of this process
    static int job_reply_count = 0; //
    String type = "";
    Socket socket;
    static long start_time = 0;
    static long end_time = 0;
    static String client_ip = "";
    static Boolean job_completion_flag = false; // flag indicates completion of work
    static Boolean flag1 = false, flag2 = false; // flag for restarting the whole mapreduce process for other files
    static String slow_mapper_ip="";
    static int slow_mapper_port=0;
    static double machine_time=0;
    static double map_red_time=0;
    static String file_name="";
    Colors colors=new Colors(); // object of class Colors


    // Constructors
    Bootstrap(Socket socket) {
        this.socket = socket;
    }

    Bootstrap(String type) {
        this.type = type;
    }

    Bootstrap(String type, int port) {
        this.type = type;
        this.port = port;
    }

    Bootstrap() {
    }


    public void run() {
        try {
            if (type.equals("check")) { // this thread will restart map reduce process for other file
                while (true) {
                    if (flag1 && flag2) {
                        String h1="Map_Reduce Time(Sec)";String h2="Single Machine Time(Sec)";
                        String divider="------------------------------------------------------";
                        System.out.printf("%-25s %25s %n", h1, h2);
                        System.out.println(divider);
                        System.out.print(colors.ANSI_RED);
                        System.out.printf("%-15s %15s %n", map_red_time, machine_time);
                        System.out.print(colors.ANSI_RESET);
                        System.out.println(divider);
                        map_red_time=machine_time=0;
                        flag1=false;flag2=false;
                        System.out.println();
                        System.out.println("Press Y to continue & N to quit");
                        Scanner sc = new Scanner(System.in);
                        String temp = sc.nextLine().toLowerCase();
                        if (temp.equals("y")) {
                            reinitialize();
                            Thread t4 = new Thread(new Bootstrap("client"), "t4");
                            t4.start();
                        } else {
                            break;
                        }
                    }else {
                        sleep(1000);
                    }
                }
            } else if (type.equals("client")) { // This thread will take input from user
                Bootstrap bootstrap_1 = new Bootstrap();
                bootstrap_1.list_files();
                Scanner scanner = new Scanner(System.in);
                System.out.println("Enter the filename you want to sort");
                String fname = scanner.nextLine();
                file_name=fname;
                start_time = System.currentTimeMillis();
//                File file = new File("C:\\Users\\JINESH\\IdeaProjects\\Distributed_System\\src\\MapReduce\\"+fname);
                File file = new File(fname);
                scanner = new Scanner(file);
                int max_num = 0;
                System.out.println();
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    // finding max num from the given input to split input
                    if (max_num < Integer.parseInt(line)) {
                        max_num = Integer.parseInt(line);
                    }
                    // storing input in arraylist
                    given_input.add(Integer.parseInt(line));
                }
                scanner.close();
                ArrayList<String> mapper_list=new ArrayList<String>();
                    if (file_info.get(fname) <=1000) {
                    // use 2 server and split
                    job_reply_count = 2;
                    int rnd = bootstrap_1.Random_Number(1, 3);// select reducer for this job
                    String rnd_ip = map.get(rnd);
                    int rnd_port = port_map.get(rnd);
                    ArrayList<Integer> temp_obj = new ArrayList<Integer>();
                    bootstrap_1.send_object(temp_obj, rnd_ip, rnd_port, "bootstrap", job_reply_count, rnd_ip, rnd_port);
                    // divide the work
                    int n = max_num / 2;
                    ArrayList<Integer> temp1 = new ArrayList<Integer>();
                    ArrayList<Integer> temp2 = new ArrayList<Integer>();
                    for (int i = 0; i < given_input.size(); i++) {
                        if (given_input.get(i) < n) {
                            temp1.add(given_input.get(i));
                        } else  {
                            temp2.add(given_input.get(i));
                        }
                    }
                    key_value.put(1, temp1);
                    key_value.put(2, temp2);
                    int count = 1;
                    // send the respective work to the mapper
                    for (int i = 1; i <= 7; i++) {
                        if (i != rnd && port_map.get(i)!=slow_mapper_port) {
                            if (count == 1) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp1, map.get(i), port_map.get(i), "job", 1, rnd_ip, rnd_port);
                            } else if (count == 2) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp2, map.get(i), port_map.get(i), "job", 2, rnd_ip, rnd_port);
                                display_map_red_info(rnd_ip, mapper_list);
                            } else {
                                break;
                            }
                        }
                    }
                } else if (file_info.get(fname) >1000 && file_info.get(fname)< 2000) {
                    // use 3 server and split
                    // same as above, the only differenc is that mapper used are 3
                    job_reply_count = 3;
                    int rnd = bootstrap_1.Random_Number(1, 3);
                    String rnd_ip = map.get(rnd);
                    int rnd_port = port_map.get(rnd);
                    ArrayList<Integer> temp_obj = new ArrayList<Integer>();
                    bootstrap_1.send_object(temp_obj, rnd_ip, rnd_port, "bootstrap", job_reply_count, rnd_ip, rnd_port);
                    int n = max_num / 3;
                    ArrayList<Integer> temp1 = new ArrayList<Integer>();
                    ArrayList<Integer> temp2 = new ArrayList<Integer>();
                    ArrayList<Integer> temp3 = new ArrayList<Integer>();
                    for (int i = 0; i < given_input.size(); i++) {
                        if (given_input.get(i) < n) {
                            temp1.add(given_input.get(i));
                        } else if (given_input.get(i) < (2 * n)) {
                            temp2.add(given_input.get(i));
                        } else {
                            temp3.add(given_input.get(i));
                        }
                    }
                    key_value.put(1, temp1);
                    key_value.put(2, temp2);
                        key_value.put(3, temp3);

                    int count = 1;
                    for (int i = 1; i <= 7; i++) {
                        if (i != rnd && port_map.get(i)!=slow_mapper_port) {
//                            System.out.println("count = " + count + " & port = " + port_map.get(i));
                            if (count == 1) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp1, map.get(i), port_map.get(i), "job", 1, rnd_ip, rnd_port);
                            } else if (count == 2) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp2, map.get(i), port_map.get(i), "job", 2, rnd_ip, rnd_port);
                            } else if (count == 3) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp3, map.get(i), port_map.get(i), "job", 3, rnd_ip, rnd_port);
                                display_map_red_info(rnd_ip, mapper_list);

                            } else {
                                break;
                            }
                        }
                    }
                } else if (file_info.get(fname) > 3000) {
                    // use 4 server and split
                    // same as above, the only differenc is that mapper used are 4
                    job_reply_count = 4;
                    int rnd = bootstrap_1.Random_Number(1, 3);
                    String rnd_ip = map.get(rnd);
                    int rnd_port = port_map.get(rnd);
                    ArrayList<Integer> temp_obj = new ArrayList<Integer>();
                    bootstrap_1.send_object(temp_obj, rnd_ip, rnd_port, "bootstrap", job_reply_count, rnd_ip, rnd_port);
                    int n = max_num / 4;
                    ArrayList<Integer> temp1 = new ArrayList<Integer>();
                    ArrayList<Integer> temp2 = new ArrayList<Integer>();
                    ArrayList<Integer> temp3 = new ArrayList<Integer>();
                    ArrayList<Integer> temp4 = new ArrayList<Integer>();
                    for (int i = 0; i < given_input.size(); i++) {
                        if (given_input.get(i) < n) {
                            temp1.add(given_input.get(i));
                        } else if (given_input.get(i) < (2 * n)) {
                            temp2.add(given_input.get(i));
                        } else if(given_input.get(i)<(3*n)){
                            temp3.add(given_input.get(i));
                        }else{
                            temp4.add(given_input.get(i));
                        }
                    }
                    key_value.put(1, temp1);
                    key_value.put(2, temp2);
                    key_value.put(3, temp3);
                    key_value.put(4, temp4);
                    int count = 1;
                    for (int i = 1; i <= 7; i++) {
                        if (i != rnd && port_map.get(i)!=slow_mapper_port) {
//                            System.out.println("count = " + count + " & port = " + port_map.get(i));
                            if (count == 1) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp1, map.get(i), port_map.get(i), "job", 1, rnd_ip, rnd_port);
                            } else if (count == 2) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp2, map.get(i), port_map.get(i), "job", 2, rnd_ip, rnd_port);
                            } else if (count == 3) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp3, map.get(i), port_map.get(i), "job", 3, rnd_ip, rnd_port);
                            }else if (count == 4) {
                                count += 1;
                                mapper_list.add(map.get(i));
                                bootstrap_1.send_object(temp3, map.get(i), port_map.get(i), "job", 4, rnd_ip, rnd_port);
                                display_map_red_info(rnd_ip, mapper_list);
                            }else {
                                break;
                            }
                        }
                    }
                }

                // Perform sorting in this machine for time and performance comparision
                System.out.println(colors.ANSI_YELLOW + "Performing sorting in this machine:-" + colors.ANSI_RESET);
                long st_time = System.currentTimeMillis();
                System.out.println(colors.ANSI_YELLOW + "Input size = " + given_input.size() + colors.ANSI_RESET);
                bootstrap_1.Sorting(given_input);
                long finish_time = System.currentTimeMillis();
                System.out.println(colors.ANSI_YELLOW + "Time  = " + (finish_time - st_time) + " ms" + colors.ANSI_RESET);
                machine_time=(finish_time-st_time)/1000;
                System.out.println();
                System.out.println();
                flag1=true;
            } else if (type.equals("server")) {
                // new connection
                ServerSocket serverSocket = new ServerSocket(port);
                while (true) {
                    Socket socket = serverSocket.accept();
                    Thread t = new Thread(new Bootstrap(socket), "obj2");
                    t.start();
                    t.join();
                }
            } else {
                // reply from reducer
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                String result = inputStream.readUTF();
                ArrayList<Integer> final_result=new ArrayList<Integer>();
                if (result.equals("success")) {
                    String temp[]=inputStream.readUTF().split(" ");
                    slow_mapper_ip=temp[0];
                    slow_mapper_port=Integer.parseInt(temp[1]);
                    final_result = (ArrayList<Integer>) inputStream.readObject();

                    System.out.println(colors.ANSI_GREEN+"slow mapper : "+get_name(slow_mapper_ip));
                    System.out.println("Map_reduce completed : ");
                    job_completion_flag = true;
                }
                end_time = System.currentTimeMillis();
                System.out.println("Time to complete work using Map_Reduce = " + (end_time - start_time) + " ms"+colors.ANSI_RESET);
                System.out.println();
                map_red_time=(end_time-start_time)/1000;
                flag2=true;
                inputStream.close();
                String temp_result=file_name+"_sorted.txt";
                write(temp_result,final_result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     *
     * @param temp_ip   ip address of the machine
     * @return          hostname of the given ip
     * @throws UnknownHostException
     */
    String get_name(String temp_ip) throws UnknownHostException {
        InetAddress addr = InetAddress.getByName(temp_ip);
        String red_host = addr.getHostName();
        return red_host;
    }

    /**
     * This function will send the object to other servers for map-reduce job
     *
     * @param temp      object that needs to be send
     * @param ip        ipaddress of the server to send
     * @param temp_port port of the server to send
     * @param msg       message to send
     */
    void send_object(ArrayList<Integer> temp, String ip, int temp_port, String msg, int key, String reducer_ip, int reducer_port) {
        try {
            Socket socket = new Socket(ip, temp_port);
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            if (msg.equals("bootstrap")) {
                outputStream.writeUTF(msg);
                outputStream.write(key);
                outputStream.writeUTF(reducer_ip);
                outputStream.writeInt(reducer_port);
                outputStream.writeUTF(client_ip);
                outputStream.writeInt(port);
                outputStream.writeObject(temp);
            } else {
                outputStream.writeUTF(msg);
                outputStream.write(key);
                outputStream.writeUTF(reducer_ip);
                outputStream.writeInt(reducer_port);
                outputStream.writeObject(temp);
            }
            outputStream.flush();
            outputStream.close();

        } catch (Exception e) {
            if (e.getMessage().contains("refused")) {
                System.out.println("Process " + ip + " is down");
            } else {
                System.out.println("Error = " + e);
                e.printStackTrace();
            }
        }
    }


    /**
     * Below function will list all the files in the given directory
     */
    void list_files() {
//        File directory = new File("C:\\Users\\JINESH\\IdeaProjects\\Distributed_System\\src\\MapReduce");
        File directory = new File(".");
        File[] fList = directory.listFiles();
        String h1="File_Name";String h2="Size";
        String divider="--------------------------------------";
        System.out.printf("%-15s %15s %n",h1,h2);
        System.out.println(divider);
        System.out.print(colors.ANSI_GREEN);
        for (File file : fList) {
            if (file.isFile()) {
                // don't display .java and .class files
                if (!(file.getName().contains("java") || file.getName().contains("class"))) {
                    double bytes = file.length();
                    double kilobytes = Math.round((bytes / 1024) * 1000d) / 1000d;
                    double megabytes = Math.round((kilobytes / 1024) * 1000d) / 1000d;
                    System.out.printf("%-15s %15s %n",file.getName(),kilobytes);
                    file_info.put(file.getName(), kilobytes);
                }
            }
        }
        System.out.print(colors.ANSI_RESET);
        System.out.println(divider);
    }

    /**
     * This function will display mapper and reducer info in tabular format
     *
     * @param t1            ipaddress of the machine
     * @param mapper_list   It contains Mapper server info
     */
    void display_map_red_info(String t1,ArrayList<String> mapper_list){
        try {
            InetAddress addr = InetAddress.getByName(t1);
            String red_host = addr.getHostName();
            String h1="Mapper_Name";String h2="Reducer_Name";
            String divider="--------------------------------------";
            System.out.printf("%-15s %15s %n",h1,h2);
            System.out.println(divider);
            System.out.print(colors.ANSI_BLUE);
            for (int i = 0; i <mapper_list.size() ; i++) {
                addr = InetAddress.getByName(mapper_list.get(i));
                String host = addr.getHostName();
                System.out.printf("%-15s %15s %n",host,red_host);
            }
            System.out.print(colors.ANSI_RESET);
            System.out.println(divider);

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     *
     * @param temp   It contains the arraylist which is to be displayed
     */
    void display(ArrayList<Integer> temp) {
        for (int i = 0; i < temp.size(); i++) {
            System.out.print(temp.get(i) + " ");
        }
        System.out.println();
    }


    /***
     * This method will generate random numbers between min and max(both inclusive)
     *
     * @param min starting range
     * @param max ending range
     * @return any number between min and max(both inclusive)
     */
    int Random_Number(int min, int max) {
        Random r = new Random();
        int n, num;
        n = r.nextInt((max - min) + 1);
        num = min + n;
        return num;
    }

    /**
     * This function will reinitialize all the necessary parameters needed for sorting
     * next file
     */
    void reinitialize() {
        sorted_numbers = new ArrayList<Integer>(); // for merge sort result
        file_info = new HashMap<String, Double>();
        given_input = new ArrayList<Integer>();
        key_value = new HashMap<Integer, ArrayList<Integer>>();
        job_reply_count = 0;
        start_time = 0;
        end_time = 0;
        flag1 = false; // sorting flag for current server
        flag2 = false; // sorting flag map reduce completion
    }


    /**
     * @param args command line arguments (ignored)
     * @throws UnknownHostException, InterruptedException
     */
    public static void main(String[] args) throws FileNotFoundException {
        try {
            ip1 = "129.21.22.196"; // glados server
            ip2 = "129.21.37.18";// kansas server
            ip3 = "129.21.30.50";//reddwarf server
            ip4 = "129.21.37.31"; // joplin server
            ip5 = "129.21.37.36"; // doors server
            ip6 = "129.21.37.30"; //kinks server
            ip7 = "129.21.37.16";// newyork server



            // start the server thread
            Thread t1 = new Thread(new Bootstrap("server", 9500), "obj1");
            t1.start();
            sleep(1000);
            InetAddress inetAddress = InetAddress.getLocalHost();
            client_ip = inetAddress.getHostAddress();
            System.out.println("Machine ip = " + client_ip+" & Machine Name = "+inetAddress.getHostName());
//            ip1=ip2=ip3=ip4=ip5=ip6=ip7=client_ip;

            // Hashmap to store server ip and port number
            map.put(1, ip1); // glados server
            map.put(2, ip2);// kansas server
            map.put(3, ip3); //reddwarf server
            map.put(4, ip4); // joplin server
            map.put(5,ip5); // doors server
            map.put(6,ip6); //kinks server
            map.put(7, ip7); // newyork server

            port_map.put(1, 9000);
            port_map.put(2, 10000);
            port_map.put(3, 11000);
            port_map.put(4, 12000);
            port_map.put(5, 13000);
            port_map.put(6, 14000);
            port_map.put(7, 15000);
            Thread t3 = new Thread(new Bootstrap("client"), "obj3");
            t3.start();
            Thread t5 = new Thread(new Bootstrap("check"), "t5");
            t5.start();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error = " + e);
        }

    }


    /**
     * This function will initiate the sorting process
     * @param numbers  It contains the input to be sorted
     */
    public void Sorting(ArrayList<Integer> numbers) {
        elements = new int[numbers.size()];
        new_array = new int[numbers.size()];
        sorted_numbers = new ArrayList<Integer>();
        moveToArray(numbers, elements);
        mergeSort(elements, 0, elements.length - 1);
        moveToArrayList(sorted_numbers, new_array);
    }

    /**
     * This function will arrange the input in the format required for sorting
     * @param sorted_numbers    It contains the number list to be sorted
     * @param new_array         Reference array for storing intermediate output
     */
    public void moveToArrayList(ArrayList<Integer> sorted_numbers, int[] new_array) {
        for (int count = 0; count < new_array.length; count++) {
            sorted_numbers.add(new_array[count]);
        }
    }

    /**
     * This function will arrange the sorting output in the format required for reducer
     * @param numbers   It stores the intermediate result
     * @param elements  It stores the final result of sorting
     */
    public void moveToArray(ArrayList<Integer> numbers, int[] elements) {
        for (int count = 0; count < numbers.size(); count++) {
            elements[count] = numbers.get(count);
        }
    }

    /**
     * This function starts the merge sort the two sorted list
     * @param elements  input elements
     * @param low       lowest index in the list
     * @param high      highest index in the list
     */
    public static void mergeSort(int[] elements, int low, int high) {
        if (low < high) {
            int middle = (low + high) / 2;
            mergeSort(elements, low, middle);
            mergeSort(elements, middle + 1, high);
            mergeParts(elements, low, middle, high);
        }
    }

    /**
     * This function merges the result of the two sorted list
     * @param elements
     * @param low       lowest index in the list
     * @param middle    Partition index
     * @param high      highest index in the list
     *
     */
    public static void mergeParts(int[] elements, int low, int middle, int high) {
        new_array = new int[elements.length];
        int first_index = low;
        int middle_index = middle + 1;
        int last_index = low;
        while (first_index <= middle && middle_index <= high) {
            if (elements[first_index] <= elements[middle_index]) {
                new_array[last_index] = elements[first_index];
                first_index++;
            } else {
                new_array[last_index] = elements[middle_index];
                middle_index++;
            }
            last_index++;
        }
        while (first_index <= middle) {
            new_array[last_index] = elements[first_index];
            last_index++;
            first_index++;
        }
        while (middle_index <= high) {
            new_array[last_index] = elements[middle_index];
            last_index++;
            middle_index++;
        }
        for (int counter = low; counter <= high; counter++) {
            elements[counter] = new_array[counter];
        }
    }


    /**
     *
     * @param filename output file name
     * @param x object
     * @throws IOException
     */
    public static void write (String filename, ArrayList<Integer> x) throws IOException{
        BufferedWriter outputWriter = null;
        outputWriter = new BufferedWriter(new FileWriter(filename));
        for (int i = 0; i < x.size(); i++) {
            // Maybe:
            outputWriter.write(x.get(i)+"");
            outputWriter.newLine();
        }
        outputWriter.flush();
        outputWriter.close();
    }
}