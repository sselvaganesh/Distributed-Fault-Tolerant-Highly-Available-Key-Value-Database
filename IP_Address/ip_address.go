package IP_Address

import (
	"log"
	"net"
)

//Retrieve Public IP
func GetPublicIP() net.IP {

	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	publicIP := conn.LocalAddr().(*net.UDPAddr)

	return publicIP.IP
}