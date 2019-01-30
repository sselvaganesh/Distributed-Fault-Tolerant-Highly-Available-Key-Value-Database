# cs457-cs557-pa4-ssudala1

----------------------------------------------------------

Programming Language Opted: GO
RPC Adopted: Protobuf
File names: client.go; replica.go; ip_address.go; cassandra.proto; cassandra.pb.go; replica.txt
Total files: 6
----------------------------------------------------------

To compile the program:
-----------------------

Using Bash:
-----------

	1. Set the GOPATH environment variable as below
		1.1 Set the current working directory to "cs457-cs557-pa4-ssudala1-1" folder
		1.2 Execute the below command in bash
			export GOPATH=`pwd`

	2. Get the protobuf package
		2.1 Execute command "go get -u github.com/golang/protobuf/protoc-gen-go"		

	3. We can directly run the programs with the below commands.
		go run Replicas/replica.go <ReplicaName> <PortNumber> <ReplicaConfigFileName> <0/1> <1/2>
				Note:   4th Parameter: 0=Replica Initialized by Client & 1=Replica Reboot to Load the Persistent Storage Values
					5th Parameter: 1=Read-Repair Mode & 2=Hinted Hand-Off Mode
		go run Client/client.go <ReplicaConfigFileName> 
	
	(Or)

	4. Create an executable for replica.go, client.go program
		4.1 Set the current working directory to "cs457-cs557-pa4-ssudala1-1" folder
		4.2 Execute the below commands			
			go build Replicas/replica.go
			go build Client/client.go  

	5. Then invoke the executables with the necessary inputs
		5.1 For Replica, Name=Replica1 ; Port=3333; ConfigFileName=replica.txt; 0=InitializeReplicaUsingClient 1=ReadRepairMode
			./replica Replica1 3333 Replica.txt 0 1
		5.2 For Client, ConfigFileName=Replica.txt
			./client Replica.txt


Run the program:
----------------

3. Run the program by executables created
	Set the current working directory to "cs457-cs557-pa4-ssudala1-1" folder
	3.1 Execute the below command in bash for Branch, controller
		go run Replicas/replica.go <ReplicaName> <PortNumber> <ReplicaConfigFileName> <0/1> <1/2>
		go run Client/client.go <ReplicaConfigFileName> 


Using makefile:
--------------

	Go doesnot require makefile to create the executables as we can use "go run" command to run the program directly which will inturn take care of creating objects.


----------------------------------------------------------

Implementation:
---------------

Implemented Assignment4 using Protobuf as mentioned in the assignment description

	Overview:
	--------
	1. Start the replicas as per the commands given above. All 4 Replicas.
	2. Keep the ReplicaConfigFileName under "Client" folder. (replica.txt)
	3. Start the Client as per the above command given above.
	4. It will acts as a Console UI to perform the GET/PUT requests.
	5. Options given under the client menu are,

		1. Initialize Replicas			// Need to Invoke this Option at the very first time to Initialize replicas
		2. Select Replica Coordinator		// To Switch the Replica Coordinator
		3. PUT Request				// Invoke PUT Requests. Give KEY, VALUE, CONSISTENCY Values under this menu as it asks
		4. GET Request				// Invokes GET Requests. Give KEY, CONSISTENCY values under this menu as it asks
		5. Erase Replica Persistent Storage	// This can be used to erase the replica persistant storage values - First time required
		6. Exit					// To exit from client


	
	Protobuf Message Defined:
	-------------------------
	1. InitReplicaCluster	- To initalize all the replicas from Client 
	2. ClientRead		- To issue a read request from client to replica coordinator
	3. ReplicaRead		- To issue a read request from replica coordinator to other replicas in the cluster
	4. ClientPut		- To issue a put request from client to replica coordinator
	5. ReplicaPut		- To issue a put request from replica coordinator to other replicas in the cluster
	6. Response		- To send a response from replica coordinator to client



	Note: 
	1. Need to keep the ReplicaConfigFileName under client folder.
	2. Inside the PUT/GET requests, value "RETURN" can be used to go the main menu.

----------------------------------------------------------
