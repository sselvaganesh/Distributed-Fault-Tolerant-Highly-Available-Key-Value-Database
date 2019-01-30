package main

import (
	"../IP_Address"
	"../Protobuf"
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//---------------------------------------------------------------------------//

//Constants Declaration
const maxBytes = 8192
const yes = "1"
const Client = "CLIENT"
const separator = "@#"
const consistencyQuorum = "QUORUM"
const consistencyOne = "ONE"
const constOne = 1

//Replica Config Details
type replica struct {
	Name       string
	IP         string
	Port       string
	TCPAddress *net.TCPAddr
}

var myConfig replica
var myReplicaCluster = make(map[string]replica)
var replicaNames = []string{}

//Flag to Identify If the Replica is Rebooting
var isReplicaRebooting = "0" // "0"-NO ; "1"-YES

//Key-Value Mapping
type keyConfig struct {
	MyValue          string
	Arrived          int64
	ReplicaAssigned1 string
	ReplicaAssigned2 string
	ReplicaAssigned3 string
}

type criticalSection struct {
	KeyValues map[uint32]keyConfig
	mtx       sync.Mutex
}

var KeyValueConfig criticalSection

//To Check the latest Key-Value Pair
type latestVal struct {
	Replica string
	Key     uint32
	Value   string
	Arrived int64
}

//Replica Initialized
var replicaInitialized = false

//Hints Structure
type hints struct {
	ReplicaName string
	Key         uint32
	Value       string
	Arrived     int64
}

//Log for Hinted Hand-Off
var hintCount = 0
var hintedHandOff = make(map[int]hints)

//Replica Modes
var readRepairMode = false    //1=Read Repair
var hintedHandOffMode = false //2=Hinted HandOff

//---------------------------------------------------------------------------//

func main() {

	//Get Public IP of the Server
	replicaIP := IP_Address.GetPublicIP()

	//Receive Input Parameters
	myConfig.Name = os.Args[1] //Replica Name
	myConfig.IP = replicaIP.String()
	myConfig.Port = os.Args[2] //Port

	//Get Replica Config File Name
	replicaConfigFile := os.Args[3] //Config. File Name

	//Get Replica Config File Name
	isReplicaRebooting = os.Args[4] //Replica Rebooting...?

	//Identify the Mode - Read Repair OR Hinted HandOff
	if os.Args[5] == "1" {
		readRepairMode = true
	} else if os.Args[5] == "2" {
		hintedHandOffMode = true
	}

	//Print This Replica Details
	fmt.Println("------------------------------------------------")
	fmt.Println(myConfig.Name, ":", replicaIP, "-", myConfig.Port)
	if isReplicaRebooting == "0" {
		fmt.Print("Booting in ")
	} else {
		fmt.Print("Re-booting in ")
	}
	if readRepairMode {
		fmt.Println("READ-REPAIR MODE.")
	}
	if hintedHandOffMode {
		fmt.Println("HINTED HAND-OFF MODE.")
	}
	fmt.Println("------------------------------------------------")

	//Allocate Memory
	KeyValueConfig.KeyValues = make(map[uint32]keyConfig)

	//Create Replica Storage File
	fileName := myConfig.Name + "Storage.txt"
	fileId, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("File Error", err)
	}
	storageWriter := bufio.NewWriter(fileId)

	//Identify Other Replicas in the Cluster
	if isReplicaRebooting == yes {

		//Do Replica Setup Using ConfigFile
		ClusterSetup(replicaConfigFile)

		//Byte Order Partition
		ByteOrderPartition()

		//Load Value For the Keys From Persistent Storage
		ReloadValue(fileName)

		replicaInitialized = true
	}

	//Receive Request from Client / Other Replicas
	ReceiverHandler(storageWriter)

	fmt.Println("Replica Closing...")

}

//---------------------------------------------------------------------------//

func ReceiverHandler(storageWriter *bufio.Writer) {

	//Listener IP Address Resolver
	myListener, err := net.ResolveTCPAddr("tcp", myConfig.IP+":"+myConfig.Port)
	if err != nil {
		log.Fatal(err)
	}

	//Replica Listening
	replicaConn, err := net.ListenTCP("tcp", myListener)
	if err != nil {
		log.Fatal(err)
	}

	//Accept Input Request
	for {

		replicaSocket, err := replicaConn.AcceptTCP()

		if err != nil {
			fmt.Println("RECEIVER: Error while accepting request.!", err)
		}

		//Process Each Request
		go ReplicaReceiverHandler(replicaSocket, storageWriter)

	}

}

//---------------------------------------------------------------------------//

func ReplicaReceiverHandler(replicaSocket *net.TCPConn, storageWriter *bufio.Writer) {

	//Read Branch Input as Received
	inpReqBuff := make([]byte, maxBytes)

	_, err := replicaSocket.Read(inpReqBuff)

	//If No request to process, return
	if err == io.EOF {
		fmt.Println("RECEIVER: Request received as EOF..!!!!!")
		return
	}

	if err != nil {
		fmt.Println("RECEIVER: Error while reading request.!", err)
		return
	}

	//Un-Marshal the Input Request
	requestMsg := new(cassandra.InputRequest)
	proto.Unmarshal(inpReqBuff, requestMsg)

	//1. Replica Init Message
	if replicaInitMsg := requestMsg.GetInitReplica(); replicaInitMsg != nil {

		if replicaInitialized {
			fmt.Println("Replica already Initialized.")
			return
		}

		//Initialize Replica
		InitializeCluster(*replicaInitMsg)

		//Map Key Values For the Replicas
		ByteOrderPartition()

		replicaInitialized = true

	}

	if !replicaInitialized {
		fmt.Println("Replica Not Initialized. Request Cannot be processed.")
		return
	}

	//2. PUT Request - From Client
	if clientPutMsg := requestMsg.GetClientPut(); clientPutMsg != nil {

		//If not enough replicas are UP, Send Exception to the Client
		if !CheckReplicaStatus(clientPutMsg.Input.Key, clientPutMsg.Input.Consistency.String()) {
			NotEnoughReplicaMsg(clientPutMsg.Input.Key, replicaSocket)
			return
		}

		//Process the Request
		ProcessClientPutRequest(clientPutMsg, storageWriter, replicaSocket)

	}

	//3. PUT Request - From Replica Coordinator
	if replicaPutMsg := requestMsg.GetReplicaPut(); replicaPutMsg != nil {

		//Write it to Persistent Storage
		WriteToStorage(replicaPutMsg.GetInput(), storageWriter)

		//Update In-Memory Value
		KeyValueConfig.UpdateValue(replicaPutMsg.Input.GetKey(), replicaPutMsg.Input.GetValue(), replicaPutMsg.Input.TimeInSeconds)

		key := replicaPutMsg.Input.GetKey()
		fmt.Println("Replica PUT:", "Key:", key, "Value:", KeyValueConfig.KeyValues[key].MyValue, "Time:", KeyValueConfig.KeyValues[key].Arrived)

		// **** Hinted HandsOff ****
		if hintedHandOffMode {
			HintedHandsOff(replicaPutMsg)
		}

	}

	//4. GET Request From CLIENT
	if replicaClientReadMsg := requestMsg.GetClientRead(); replicaClientReadMsg != nil {

		//If not enough replicas are UP, Send Exception to the Client
		if !CheckReplicaStatus(replicaClientReadMsg.Key, replicaClientReadMsg.Consistency.String()) {
			NotEnoughReplicaMsg(replicaClientReadMsg.Key, replicaSocket)
			return
		}

		//Process the Request
		ProcessClientReadRequest(replicaClientReadMsg, replicaSocket)

	}

	//5. Replica Read Request
	if replicaReadReadMsg := requestMsg.GetReplicaRead(); replicaReadReadMsg != nil {

		//Read the Key-Value from memory
		ReplicaRead(*replicaReadReadMsg, replicaSocket)

	}

}

//---------------------------------------------------------------------------//

func ProcessClientPutRequest(clientPutMsg *cassandra.ClientPut, storageWriter *bufio.Writer, replicaSocket *net.TCPConn) {

	//Get key Value
	keyValueRcvd := clientPutMsg.Input.GetKey()

	//Add Timestamp
	clientPutMsg.Input.Timestamp, _ = ptypes.TimestampProto(time.Now())
	clientPutMsg.Input.TimeInSeconds = clientPutMsg.Input.Timestamp.GetSeconds()

	//Count the successful PUT messages
	clientRespSent := false
	successCount := 0

	//if Key belongs to this Replica, Write the Request to the Persistent Storage
	if KeyBelongsToMe(keyValueRcvd) {

		//First Replica PUT Done
		successCount++

		//Write it to Persistent Storage
		WriteToStorage(clientPutMsg.GetInput(), storageWriter)

		//Update - UpdateValue
		KeyValueConfig.UpdateValue(keyValueRcvd, clientPutMsg.Input.GetValue(), clientPutMsg.Input.TimeInSeconds)

		//If Consistency level is set to ONE, Send Response to Client and Proceed
		if clientPutMsg.Input.GetConsistency().String() == consistencyOne {
			SendResponseToClient(keyValueRcvd, replicaSocket)
			clientRespSent = true
		}

	}

	//Send ReplicaPut Request to Remaining Replicas
	for _, eachReplica := range myReplicaCluster {

		if eachReplica.Name == KeyValueConfig.KeyValues[keyValueRcvd].ReplicaAssigned1 ||
			eachReplica.Name == KeyValueConfig.KeyValues[keyValueRcvd].ReplicaAssigned2 ||
			eachReplica.Name == KeyValueConfig.KeyValues[keyValueRcvd].ReplicaAssigned3 {

			replicaPutMessage := new(cassandra.InputRequest_ReplicaPut)
			replicaPutMessage.ReplicaPut = new(cassandra.ReplicaPut)
			replicaPutMessage.ReplicaPut.Input = new(cassandra.RequestParameter)
			replicaPutMessage.ReplicaPut.Input = clientPutMsg.GetInput()
			replicaPutMessage.ReplicaPut.Input.OriginReplica = myConfig.Name

			//Input Request Message
			replicaMsg := new(cassandra.InputRequest)
			replicaMsg.InputRequest = replicaPutMessage

			//Proto-buf Message
			protoReplicaPutMsg, _ := proto.Marshal(replicaMsg)

			//Send ReplicaPut Message
			connection, err := net.DialTCP("tcp", nil, eachReplica.TCPAddress)

			//If Failed, Make Hints
			if err != nil {

				if hintedHandOffMode {

					// Make Hints for Hinted-HandsOff
					MakeHints(eachReplica.Name, clientPutMsg)
				}

			} else { //Else Send PUT Request to Other Replicas

				connection.Write(protoReplicaPutMsg)
				successCount++

				//Check if Client Response Can be Sent

				//Consistency = ONE
				if clientPutMsg.Input.GetConsistency().String() == consistencyOne && !clientRespSent {

					//Send Response to Client as SUCCESS - ONE
					SendResponseToClient(keyValueRcvd, replicaSocket)
					clientRespSent = true
				}

				//Consistency = QUORUM
				if clientPutMsg.Input.GetConsistency().String() == consistencyQuorum && !clientRespSent && successCount > constOne {

					//Send Response to Client as SUCCESS - QUORUM Consistency
					SendResponseToClient(keyValueRcvd, replicaSocket)
					clientRespSent = true
				}

			}

		}

	}

}

//---------------------------------------------------------------------------//

func ProcessClientReadRequest(replicaClientReadMsg *cassandra.ClientRead, replicaSocket *net.TCPConn) {

	keyValueRcvd := replicaClientReadMsg.GetKey()

	//To Check the latest Value
	readRepairLog := make(map[string]latestVal)
	finalValOfThisKey := new(latestVal)

	//Check Key Belongs to this Replica
	if KeyBelongsToMe(keyValueRcvd) {

		if keyValues := KeyValueConfig.ReadValue(keyValueRcvd); keyValues.MyValue != "" {
			latestValOfThiskey := new(latestVal)
			latestValOfThiskey.Key = keyValueRcvd
			latestValOfThiskey.Value = keyValues.MyValue
			latestValOfThiskey.Arrived = keyValues.Arrived
			latestValOfThiskey.Replica = myConfig.Name
			finalValOfThisKey = latestValOfThiskey
			readRepairLog[myConfig.Name] = *latestValOfThiskey
		}

	}

	//Read Value from all Other Replicas
	for _, eachReplica := range myReplicaCluster {

		if eachReplica.Name == KeyValueConfig.KeyValues[keyValueRcvd].ReplicaAssigned1 ||
			eachReplica.Name == KeyValueConfig.KeyValues[keyValueRcvd].ReplicaAssigned2 ||
			eachReplica.Name == KeyValueConfig.KeyValues[keyValueRcvd].ReplicaAssigned3 {

			replicaReadMessage := new(cassandra.InputRequest_ReplicaRead)
			replicaReadMessage.ReplicaRead = new(cassandra.ReplicaRead)
			replicaReadMessage.ReplicaRead.Key = keyValueRcvd

			//Input Request Message
			replicaMsg := new(cassandra.InputRequest)
			replicaMsg.InputRequest = replicaReadMessage

			//Proto-buf Message
			protoReplicaReadMsg, _ := proto.Marshal(replicaMsg)

			//Send ReplicaPut Message
			connection, err := net.DialTCP("tcp", nil, eachReplica.TCPAddress)

			//If Failed, Make Hints for Read-Replica
			if err == nil {

				connection.Write(protoReplicaReadMsg)

				respBuff := make([]byte, maxBytes)
				connection.Read(respBuff)

				respMsg := new(cassandra.InputRequest)
				proto.Unmarshal(respBuff, respMsg)

				replicaResponse := respMsg.GetResponse()

				if replicaResponse.GetValue() != " " {

					latestValOfThiskey := new(latestVal)
					latestValOfThiskey.Key = keyValueRcvd
					latestValOfThiskey.Value = replicaResponse.GetValue()
					latestValOfThiskey.Replica = replicaResponse.GetOriginReplica()
					latestValOfThiskey.Arrived = replicaResponse.GetArrival()
					readRepairLog[latestValOfThiskey.Replica] = *latestValOfThiskey

					//Check If the other Replica value is latest
					if latestValOfThiskey.Arrived > finalValOfThisKey.Arrived {
						finalValOfThisKey = latestValOfThiskey
					}

				}

			}
		}

	}

	//Send the Final Value of this key to CLIENT
	clientResponse := new(cassandra.InputRequest_Response)
	clientResponse.Response = new(cassandra.Response)
	clientResponse.Response.Key = keyValueRcvd

	if finalValOfThisKey.Value != "" {
		clientResponse.Response.Value = finalValOfThisKey.Value
		clientResponse.Response.Status = true
		clientResponse.Response.RespMessage = "Value Retrieved Successfully.!"
	} else {
		clientResponse.Response.Status = false
		clientResponse.Response.RespMessage = "Unable to Locate the Key-Value Pair"

	}

	sendResponse := new(cassandra.InputRequest)
	sendResponse.InputRequest = clientResponse

	protoRespMsg, _ := proto.Marshal(sendResponse)
	replicaSocket.Write(protoRespMsg)

	fmt.Println("Client Read: ", "Key:", clientResponse.Response.Key, "Value:", clientResponse.Response.Value, "Time:", clientResponse.Response.Arrival)

	//Do Read Repair
	if readRepairMode {
		ReadRepair(*finalValOfThisKey, readRepairLog)
	}

}

//---------------------------------------------------------------------------//

func ReplicaRead(replicaReadReadMsg cassandra.ReplicaRead, replicaSocket *net.TCPConn) {

	//Key Value
	keyValueRcvd := replicaReadReadMsg.GetKey()

	//Read the Current Value
	keyValues := KeyValueConfig.ReadValue(keyValueRcvd)

	//Send Response to Client
	replicaResponse := new(cassandra.InputRequest_Response)
	replicaResponse.Response = new(cassandra.Response)
	replicaResponse.Response.Key = keyValueRcvd
	replicaResponse.Response.OriginReplica = myConfig.Name
	replicaResponse.Response.Value = keyValues.MyValue
	replicaResponse.Response.Arrival = keyValues.Arrived
	replicaResponse.Response.Status = true
	replicaResponse.Response.RespMessage = myConfig.Name + "Success"

	sendResponse := new(cassandra.InputRequest)
	sendResponse.InputRequest = replicaResponse

	protoRespMsg, _ := proto.Marshal(sendResponse)
	replicaSocket.Write(protoRespMsg)

	fmt.Println("Replica Read:", "Key:", keyValueRcvd, " Value:", keyValues.MyValue, "Time:", keyValues.Arrived)

}

//---------------------------------------------------------------------------//

func HintedHandsOff(replicaPutMsg *cassandra.ReplicaPut) {

	for hintKey, hint := range hintedHandOff {

		if replicaPutMsg.Input.OriginReplica == hint.ReplicaName {

			newReplicaPutMessage := new(cassandra.InputRequest_ReplicaPut)
			newReplicaPutMessage.ReplicaPut = new(cassandra.ReplicaPut)
			newReplicaPutMessage.ReplicaPut.Input = new(cassandra.RequestParameter)
			newReplicaPutMessage.ReplicaPut.Input.Key = hint.Key
			newReplicaPutMessage.ReplicaPut.Input.Value = hint.Value
			newReplicaPutMessage.ReplicaPut.Input.TimeInSeconds = hint.Arrived
			newReplicaPutMessage.ReplicaPut.Input.OriginReplica = myConfig.Name

			//Input Request Message
			replicaMsg := new(cassandra.InputRequest)
			replicaMsg.InputRequest = newReplicaPutMessage

			//Proto-buf Message
			protoReplicaPutMsg, _ := proto.Marshal(replicaMsg)

			//Send ReplicaPut Message
			connection, _ := net.DialTCP("tcp", nil, myReplicaCluster[hint.ReplicaName].TCPAddress)

			//Send Hinted HandsOff
			connection.Write(protoReplicaPutMsg)

			//Delete the Hint
			delete(hintedHandOff, hintKey)

			fmt.Println("Hinted-HandOff: Replica:", hint.ReplicaName, "Key:",hint.Key, "Value",
				hint.Value, "Time:", hint.Arrived)

		}

	}

}

//---------------------------------------------------------------------------//

func ReadRepair(finalValOfThisKey latestVal, readRepairLog map[string]latestVal) {

	for _, eachReplicaVal := range readRepairLog {

		if finalValOfThisKey.Arrived > eachReplicaVal.Arrived {

			//Stale Value Can be From Coordinator
			if eachReplicaVal.Replica == myConfig.Name {

				KeyValueConfig.UpdateValue(finalValOfThisKey.Key, finalValOfThisKey.Value, finalValOfThisKey.Arrived)

			} else {

				//Stale Value from Other Replicas

				replicaPutMessage := new(cassandra.InputRequest_ReplicaPut)
				replicaPutMessage.ReplicaPut = new(cassandra.ReplicaPut)
				replicaPutMessage.ReplicaPut.Input = new(cassandra.RequestParameter)

				replicaPutMessage.ReplicaPut.Input.Key = finalValOfThisKey.Key
				replicaPutMessage.ReplicaPut.Input.Value = finalValOfThisKey.Value
				replicaPutMessage.ReplicaPut.Input.OriginReplica = finalValOfThisKey.Replica
				replicaPutMessage.ReplicaPut.Input.TimeInSeconds = finalValOfThisKey.Arrived

				//Input Request Message
				replicaMsg := new(cassandra.InputRequest)
				replicaMsg.InputRequest = replicaPutMessage

				//Proto-buf Message
				protoReplicaPutMsg, _ := proto.Marshal(replicaMsg)

				//Send ReplicaPut Message
				connection, _ := net.DialTCP("tcp", nil, myReplicaCluster[eachReplicaVal.Replica].TCPAddress)
				connection.Write(protoReplicaPutMsg)

				fmt.Println("Read Repair:", "Replica:", eachReplicaVal.Replica, "Key:", finalValOfThisKey.Key,
				"Value:", finalValOfThisKey.Value, "Time:", finalValOfThisKey.Arrived)
			}

		}

	}

}

//---------------------------------------------------------------------------//

func SendResponseToClient(key uint32, replicaSocket *net.TCPConn) {

	replicaResponse := new(cassandra.InputRequest_Response)
	replicaResponse.Response = new(cassandra.Response)
	replicaResponse.Response.Key = key
	replicaResponse.Response.OriginReplica = myConfig.Name
	replicaResponse.Response.Status = true
	replicaResponse.Response.RespMessage = "Key-Value Pair is Successfully Stored..!"

	sendResponse := new(cassandra.InputRequest)
	sendResponse.InputRequest = replicaResponse

	protoRespMsg, _ := proto.Marshal(sendResponse)
	replicaSocket.Write(protoRespMsg)

	fmt.Println("Client PUT:", "Key:", key, "Value:", KeyValueConfig.KeyValues[key].MyValue, "Time:", KeyValueConfig.KeyValues[key].Arrived)

}

//---------------------------------------------------------------------------//

func MakeHints(replicaName string, clientPutMsg *cassandra.ClientPut) {

	// Make Hints for Hinted-HandsOff
	hintCount++
	newHint := new(hints)
	newHint.ReplicaName = replicaName
	newHint.Key = clientPutMsg.Input.GetKey()
	newHint.Value = clientPutMsg.Input.GetValue()
	newHint.Arrived = clientPutMsg.Input.GetTimeInSeconds()

	hintedHandOff[hintCount] = *newHint

	fmt.Println("Hint Logged:", "Key:", hintedHandOff[hintCount].Key, "Value:", "Time:", hintedHandOff[hintCount].Value, hintedHandOff[hintCount].Arrived)

}

//---------------------------------------------------------------------------//

func CheckReplicaStatus(key uint32, consistency string) bool {

	replicaAlive := 0

	//Am I the coordinator.?
	if KeyBelongsToMe(key) {
		replicaAlive++

		//Consistency = ONE
		if consistency == consistencyOne {
			return true
		}

	}

	//Check Other Replicas
	for _, eachReplica := range myReplicaCluster {

		if eachReplica.Name == KeyValueConfig.KeyValues[key].ReplicaAssigned1 ||
			eachReplica.Name == KeyValueConfig.KeyValues[key].ReplicaAssigned2 ||
			eachReplica.Name == KeyValueConfig.KeyValues[key].ReplicaAssigned3 {

			//Send Replica Test message
			connection, err := net.DialTCP("tcp", nil, eachReplica.TCPAddress)

			//Replica UP and Running.....
			if err == nil {
				replicaAlive++
				connection.Write([]byte("Testing Connection..!!!!"))
			}

			//For Consistency ONE Minimum 1 Replicas must be UP
			if consistency == consistencyOne && replicaAlive >= constOne {
				return true
			}

			//For Consistency QUORUM Minimum 2 Replicas must be UP
			if consistency == consistencyQuorum && replicaAlive > constOne {
				return true
			}

		}
	}

	//Not Enough Replicas are UP
	return false

}

//---------------------------------------------------------------------------//

func KeyBelongsToMe(key uint32) bool {

	if KeyValueConfig.KeyValues[key].ReplicaAssigned1 == myConfig.Name ||
		KeyValueConfig.KeyValues[key].ReplicaAssigned2 == myConfig.Name ||
		KeyValueConfig.KeyValues[key].ReplicaAssigned3 == myConfig.Name {
		return true
	}

	return false
}

//---------------------------------------------------------------------------//

func NotEnoughReplicaMsg(key uint32, replicaSocket *net.TCPConn) {

	//Send Response to Client
	replicaResponse := new(cassandra.InputRequest_Response)
	replicaResponse.Response = new(cassandra.Response)
	replicaResponse.Response.Key = key
	replicaResponse.Response.OriginReplica = myConfig.Name
	replicaResponse.Response.Status = false
	replicaResponse.Response.RespMessage = "Cannot Process This Request. Not Enough Replicas are UP for this request.!"

	sendResponse := new(cassandra.InputRequest)
	sendResponse.InputRequest = replicaResponse

	protoRespMsg, _ := proto.Marshal(sendResponse)
	replicaSocket.Write(protoRespMsg)

	fmt.Println("Replica Exception: Not Enough Replicas are UP...!!! ")

}

//---------------------------------------------------------------------------//

func (cs *criticalSection) UpdateValue(keyVal uint32, value string, timeValLatest int64) {

	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	currentKeyVal := KeyValueConfig.KeyValues[keyVal]

	//If the Value Receive with higher timestamp, then update the value, timestamp
	if timeValLatest > currentKeyVal.Arrived {

		//Update
		currentKeyVal.MyValue = value
		currentKeyVal.Arrived = timeValLatest
		KeyValueConfig.KeyValues[keyVal] = currentKeyVal

	}

}

//---------------------------------------------------------------------------//

func (cs *criticalSection) ReadValue(keyVal uint32) keyConfig {

	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	toUpdateVal := KeyValueConfig.KeyValues[keyVal]

	return toUpdateVal
}

//---------------------------------------------------------------------------//

func InitializeCluster(replicaInitMsg cassandra.InitReplicaCluster) {

	allReplicas := replicaInitMsg.GetAllReplica()

	for _, thisReplica := range allReplicas {

		//If details of Same Replica
		if thisReplica.Name == myConfig.Name {

			myConfig.IP = thisReplica.Ip
			myConfig.Port = thisReplica.Port

		} else {

			newReplica := new(replica)
			newReplica.Name = thisReplica.Name
			newReplica.IP = thisReplica.Ip
			newReplica.Port = thisReplica.Port
			newReplica.TCPAddress, _ = net.ResolveTCPAddr("tcp", newReplica.IP+":"+newReplica.Port)

			//Add it to the Cluster Configuration
			myReplicaCluster[newReplica.Name] = *newReplica

		}

		replicaNames = append(replicaNames, thisReplica.Name)

	}

	fmt.Println(myConfig.Name, "Initialized From Client Request")

}

//---------------------------------------------------------------------------//

func ClusterSetup(replicaConfigFile string) {

	file, err := os.Open("../Client/" + replicaConfigFile)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		os.Exit(1)
	}

	//Read Config File Content
	fileContent := bufio.NewReader(file)
	fileBuff, _, err := fileContent.ReadLine()

	for err == nil {

		replicaLine := string(fileBuff)
		replicaDtl := strings.Split(replicaLine, " ")

		//If details of Same Replica
		if replicaDtl[0] == myConfig.Name {

			myConfig.IP = replicaDtl[1]
			myConfig.Port = replicaDtl[2]

		} else {

			newReplica := new(replica)
			newReplica.Name = replicaDtl[0]
			newReplica.IP = replicaDtl[1]
			newReplica.Port = replicaDtl[2]
			newReplica.TCPAddress, _ = net.ResolveTCPAddr("tcp", newReplica.IP+":"+newReplica.Port)

			//Add it to the Cluster Configuration
			myReplicaCluster[newReplica.Name] = *newReplica

		}

		replicaNames = append(replicaNames, replicaDtl[0])

		fileBuff, _, err = fileContent.ReadLine()

	}

	fmt.Println(myConfig.Name, "Initialized Using Configure File")

}

//---------------------------------------------------------------------------//

func ByteOrderPartition() {

	for i := 0; i <= 255; i++ {

		newKeyValueConfig := new(keyConfig)

		if i <= 63 {

			newKeyValueConfig.ReplicaAssigned1 = replicaNames[0]
			newKeyValueConfig.ReplicaAssigned2 = replicaNames[1]
			newKeyValueConfig.ReplicaAssigned3 = replicaNames[2]

		} else if (i >= 64) && (i <= 127) {

			newKeyValueConfig.ReplicaAssigned1 = replicaNames[1]
			newKeyValueConfig.ReplicaAssigned2 = replicaNames[2]
			newKeyValueConfig.ReplicaAssigned3 = replicaNames[3]

		} else if (i >= 128) && (i <= 191) {

			newKeyValueConfig.ReplicaAssigned1 = replicaNames[2]
			newKeyValueConfig.ReplicaAssigned2 = replicaNames[3]
			newKeyValueConfig.ReplicaAssigned3 = replicaNames[0]

		} else {

			newKeyValueConfig.ReplicaAssigned1 = replicaNames[3]
			newKeyValueConfig.ReplicaAssigned2 = replicaNames[0]
			newKeyValueConfig.ReplicaAssigned3 = replicaNames[1]

		}

		newKeyValueConfig.MyValue = ""

		//Add it the Replica Mapping
		KeyValueConfig.KeyValues[uint32(i)] = *newKeyValueConfig
	}

}

//---------------------------------------------------------------------------//

func WriteToStorage(putMsg *cassandra.RequestParameter, storageWriter *bufio.Writer) {

	//Format String
	data := fmt.Sprint(putMsg.GetKey()) + separator +
		putMsg.GetValue() + separator +
		fmt.Sprint(putMsg.GetTimeInSeconds()) + "\n"

	//Write it to File
	storageWriter.WriteString(data)
	storageWriter.Flush()

}

//---------------------------------------------------------------------------//

func ReloadValue(fileName string) {

	fileId, err := os.Open(fileName)

	//File Not Found, Exit
	if err != nil {
		log.Fatal(err)
	}

	//Read Content
	fileBuf := bufio.NewReader(fileId)
	fileContent, _, err := fileBuf.ReadLine()

	for err == nil {

		eachLine := string(fileContent)
		data := strings.Split(eachLine, separator)

		//Save Value in In-Memory
		key, _ := strconv.Atoi(data[0])
		timeVal, _ := strconv.Atoi(data[2])

		updateKeyValue := KeyValueConfig.KeyValues[uint32(key)]

		//Load the Latest Value
		if int64(timeVal) > updateKeyValue.Arrived {
			updateKeyValue.MyValue = data[1]
			updateKeyValue.Arrived = int64(timeVal)
			KeyValueConfig.KeyValues[uint32(key)] = updateKeyValue

			fmt.Println(key, KeyValueConfig.KeyValues[uint32(key)].MyValue, KeyValueConfig.KeyValues[uint32(key)].Arrived)
		}

		fileContent, _, err = fileBuf.ReadLine()

	}

}

//---------------------------------------------------------------------------//
