package main

import (
	"../Protobuf"
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

//--------------------------------------------------------//

//Constants Declaration
const Client = "CLIENT"
const maxBytes = 8192

type replica struct {
	Name       string
	IP         string
	Port       string
	TCPAddress *net.TCPAddr
}

var loadAllReplicas = []*cassandra.InitReplicaCluster_Replica{}

var replicaConn = []*replica{}

//
var totalReplicas uint32 = 0
var replicaIndex = 0

//--------------------------------------------------------//

func main() {

	//Receive Replica Config Name
	replicaFileName := os.Args[1]

	//Identify the Replicas in the Cluster
	ReplicaClusterSetup(replicaFileName)

	//Get user Input
	Process()

	//Close Client
	fmt.Println("Client Closing...")

}

//--------------------------------------------------------//

func Process() {

	scanner := bufio.NewScanner(os.Stdin)

	//Show Console Menu
	DisplayMenu()

	for scanner.Scan() {

		menu := scanner.Text()

		switch menu {

		case "1":
			InitReplicas()

		case "2":
			DecideCoordinator()

		case "3":
			ProcessPutRequest()

		case "4":
			ProcessReadRequest()

		case "5":
			ResetReplicaStorage()

		case "6":
			return

		default:
			fmt.Println("Not a valid option")

		}

		DisplayMenu()
	}

	if scanner.Err() != nil {
		fmt.Println("Error while reading the input.")
	}

}

//--------------------------------------------------------//

func InitReplicas() {

	//
	initReplicaMsg := new(cassandra.InputRequest_InitReplica)
	initReplicaMsg.InitReplica = new(cassandra.InitReplicaCluster)
	initReplicaMsg.InitReplica.AllReplica = loadAllReplicas

	//Complete String message for InitReplica
	replicaMsg := new(cassandra.InputRequest)
	replicaMsg.InputRequest = initReplicaMsg

	protoInitReplicaMsg, err := proto.Marshal(replicaMsg)

	if err != nil {
		log.Fatal("Marshalling Error @ InitReplica: ", err)
	}

	//Send InitReplica Message to All the Replicas
	for _, thisReplica := range replicaConn {

		thisReplicaChannel, err := net.DialTCP("tcp", nil, thisReplica.TCPAddress)

		if err != nil {
			fmt.Println("Connection Error. ", thisReplica.Name, err)
		} else {
			thisReplicaChannel.Write(protoInitReplicaMsg)
			fmt.Println(thisReplica.Name, "Initialized.!")

		}

	}

	fmt.Println("--------------------------------------------")
}

//--------------------------------------------------------//

func DecideCoordinator() {

	fmt.Println("-------------- Select Replica Coordinator --------------")
	for i, replicaName := range replicaConn {
		i++
		fmt.Println(i, ".", replicaName.Name)
	}
	fmt.Print("Enter Replica Number: ")

	//Accept KEY Value
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {

		if scanner.Text() == "RETURN" {
			return
		}

		replicaSlt := scanner.Text()
		val, err := strconv.Atoi(replicaSlt)

		if val < 1 || val > 4 || err != nil {

			fmt.Println("Error: Invalid Selection.")

			fmt.Println("-------------- Select Replica Coordinator --------------")
			for i, replicaName := range replicaConn {
				i++
				fmt.Println(i, ".", replicaName.Name)
			}
			fmt.Print("Enter Replica Number: ")

		} else {
			replicaIndex = val - 1
			fmt.Println("Replica Coordinator:", replicaConn[replicaIndex].Name)
			fmt.Println("--------------------------------------------")
			return
		}

	}
}

//--------------------------------------------------------//

func ProcessPutRequest() {

	fmt.Println("------------- PUT Request --------------------")

	//Accept Values
	keyString := " "
	value := " "
	consistency := " "

	scanner := bufio.NewScanner(os.Stdin)

	//KEY
	fmt.Print("Enter Key (0~255): ")
	for scanner.Scan() {

		if scanner.Text() == "RETURN" {
			return
		}

		keyString = scanner.Text()
		val, err := strconv.Atoi(keyString)

		if val < 0 || val > 255 || err != nil {
			fmt.Println("Error: Not a valid KEY.")
			fmt.Print("Enter Key (0~255): ")
		} else {
			break
		}

	}

	//VALUE
	fmt.Print("Enter Value: ")
	for scanner.Scan() {
		value = scanner.Text()

		if scanner.Text() == "RETURN" {
			return
		}

		if value != " " {
			break
		} else {
			fmt.Println("Error: Not a valid VALUE. ")
			fmt.Print("Enter Value: ")
		}

	}

	//CONSISTENCY
	fmt.Print("Enter Consistency Level (ONE/QUORUM) : ")
	for scanner.Scan() {
		consistency = scanner.Text()

		if scanner.Text() == "RETURN" {
			return
		}

		if !(consistency == "ONE" || consistency == "QUORUM") {
			fmt.Println("Error: Not a valid CONSISTENCY.")
			fmt.Print("Enter Consistency Level (ONE/QUORUM) : ")
		} else {
			break
		}

	}

	keyVal, _ := strconv.Atoi(keyString)
	PutRequest(uint32(keyVal), value, consistency)

}

//--------------------------------------------------------//

func PutRequest(keyValue uint32, value string, consistency string) {

	//Built PUT Message Request
	putMessage := new(cassandra.InputRequest_ClientPut)
	putMessage.ClientPut = new(cassandra.ClientPut)
	putMessage.ClientPut.Input = new(cassandra.RequestParameter)
	putMessage.ClientPut.Input.Key = keyValue

	if consistency == "ONE" {
		putMessage.ClientPut.Input.Consistency = cassandra.RequestParameter_ONE
	} else if consistency == "QUORUM" {
		putMessage.ClientPut.Input.Consistency = cassandra.RequestParameter_QUORUM

	}

	putMessage.ClientPut.Input.OriginReplica = Client
	putMessage.ClientPut.Input.Value = value

	//Make Input Request
	replicaMsg := new(cassandra.InputRequest)
	replicaMsg.InputRequest = putMessage

	//Protobuf Message
	protoPutMsg, err := proto.Marshal(replicaMsg)

	if err != nil {
		fmt.Println("Marshalling Error @ PUT Request: ", err)
		return
	}

	//Make Connection
	channel, err := net.DialTCP("tcp", nil, replicaConn[replicaIndex].TCPAddress)

	if err != nil {
		fmt.Println("Connection Error. ", err)
		return
	} else {
		channel.Write(protoPutMsg) //Send Request

		//ReadResponse
		respBuff := make([]byte, maxBytes)
		_, err := channel.Read(respBuff)

		if err != nil {
			fmt.Println("Error while Reading response. ", err)
			return
		} else {

			//Display Response
			respMsg := new(cassandra.InputRequest)
			proto.Unmarshal(respBuff, respMsg)

			replicaResponse := respMsg.GetResponse()

			fmt.Println("===> PUT Request Response")
			fmt.Println("Key =", keyValue, "; Value =", value, "; Consistency =", consistency, "; Coordinator =", replicaConn[replicaIndex].Name)
			fmt.Println("Status:", replicaResponse.GetStatus(), "; Message:", replicaResponse.GetRespMessage())
			fmt.Println("--------------------------------------------")
		}

	}

}

//--------------------------------------------------------//

func ProcessReadRequest() {

	fmt.Println("------------- GET Request --------------------")

	//Accept KEY Value
	scanner := bufio.NewScanner(os.Stdin)

	keyString := " "
	consistency := " "

	fmt.Print("Enter Key (0~255): ")
	for scanner.Scan() {

		if scanner.Text() == "RETURN" {
			return
		}

		keyString = scanner.Text()
		val, err := strconv.Atoi(keyString)

		if val < 0 || val > 255 || err != nil {
			fmt.Println("Error: Not a valid KEY.")
			fmt.Print("Enter Key (0~255): ")
		} else {
			break
		}

	}

	//CONSISTENCY
	fmt.Print("Enter Consistency Level (ONE/QUORUM) : ")
	for scanner.Scan() {
		consistency = scanner.Text()

		if scanner.Text() == "RETURN" {
			return
		}

		if !(consistency == "ONE" || consistency == "QUORUM") {
			fmt.Println("Error: Not a valid CONSISTENCY.")
			fmt.Print("Enter Consistency Level (ONE/QUORUM) : ")
		} else {
			break
		}

	}

	//
	key, _ := strconv.Atoi(keyString)

	//Initiate Read Request
	ReadRequest(uint32(key), consistency)

}

//--------------------------------------------------------//

func ReadRequest(key uint32, consistency string) {

	readMessage := new(cassandra.InputRequest_ClientRead)
	readMessage.ClientRead = new(cassandra.ClientRead)
	readMessage.ClientRead.Key = key

	if consistency == "ONE" {
		readMessage.ClientRead.Consistency = cassandra.ClientRead_ONE
	} else if consistency == "QUORUM" {
		readMessage.ClientRead.Consistency = cassandra.ClientRead_QUORUM
	}

	//Make Input Request
	clientReadMsg := new(cassandra.InputRequest)
	clientReadMsg.InputRequest = readMessage

	//Protobuf Message
	protoReadMsg, err := proto.Marshal(clientReadMsg)

	if err != nil {
		fmt.Println("Marshalling Error @ PUT Request: ", err)
		return
	}

	//Make Connection
	channel, err := net.DialTCP("tcp", nil, replicaConn[replicaIndex].TCPAddress)

	if err != nil {
		fmt.Println("Connection Error. ", err)
	} else {

		//Send Request
		channel.Write(protoReadMsg)

		//ReadResponse
		respBuff := make([]byte, maxBytes)
		_, err := channel.Read(respBuff)

		if err != nil {
			fmt.Println("Error while Reading response. ", err)
			return
		} else {

			//Display Response
			respMsg := new(cassandra.InputRequest)
			proto.Unmarshal(respBuff, respMsg)

			replicaResponse := respMsg.GetResponse()

			//fmt.Println("--------------------------------------------")
			fmt.Println("GET Request: Key =", replicaResponse.GetKey())
			if replicaResponse.GetStatus() {
				fmt.Println("Value:", replicaResponse.GetValue())
			}
			fmt.Println("Replica Coordinator:", replicaConn[replicaIndex].Name)
			fmt.Println("Request Status:", replicaResponse.GetStatus())
			fmt.Println("Response Msg:", replicaResponse.GetRespMessage())
			fmt.Println("--------------------------------------------")
		}

	}

}

//--------------------------------------------------------//

func ResetReplicaStorage() {

	for _, thisReplica := range replicaConn {

		fileName := "../Replicas/" + thisReplica.Name + "Storage.txt"

		err := os.Truncate(fileName, 0)
		if err != nil {
			fmt.Println("Error: ", err)
		} else {
			fmt.Println(thisReplica.Name, "Storage Erased.!")
		}

	}

	fmt.Println("--------------------------------------------")

}

//--------------------------------------------------------//

func ReplicaClusterSetup(replicaFileName string) {

	//
	file, err := os.Open(replicaFileName)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		os.Exit(1)
	}

	//Read Config File Content
	fileContent := bufio.NewReader(file)
	fileBuff, _, err := fileContent.ReadLine()

	for err == nil {

		var err1 error

		replicaLine := string(fileBuff)
		replicaDtl := strings.Split(replicaLine, " ")

		//Create a Branch and TCP Connection for every instance
		newReplica := new(replica)
		newReplica.Name = replicaDtl[0]
		newReplica.IP = replicaDtl[1]
		newReplica.Port = replicaDtl[2]
		newReplica.TCPAddress, err1 = net.ResolveTCPAddr("tcp", newReplica.IP+":"+newReplica.Port)

		if err1 != nil {
			fmt.Println("TCP Connection between Client and", replicaDtl[0], " FAILED.")
			log.Fatal(err1)
		}

		//Load the Branch Details in the InitBranch Structure
		newReplica1 := new(cassandra.InitReplicaCluster_Replica)
		newReplica1.Name = replicaDtl[0]
		newReplica1.Ip = replicaDtl[1]
		newReplica1.Port = replicaDtl[2]

		loadAllReplicas = append(loadAllReplicas, newReplica1)

		//Consolidate Branches
		replicaConn = append(replicaConn, newReplica)

		totalReplicas++

		fileBuff, _, err = fileContent.ReadLine()

	}

}

//--------------------------------------------------------//

func DisplayMenu() {

	fmt.Println("1. Initialize Replicas")
	fmt.Println("2. Select Replica Coordinator")
	fmt.Println("3. PUT Request")
	fmt.Println("4. GET Request")
	fmt.Println("5. Erase Replica Persistent Storage")
	fmt.Println("6. Exit")
	fmt.Print("Enter Your Option: ")

}

//--------------------------------------------------------//
