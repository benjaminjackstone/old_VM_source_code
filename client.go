package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

//this is an empty struct to give to the interface
type Nothing struct{}

type Message struct {
	User   string
	Target string
	Msg    string
}

type CClient struct {
	Username string
	Address  string
	Client   *rpc.Client
}

func (c *CClient) List() {
	var reply []string
	var none Nothing
	c.Client = c.Connect()
	err := c.Client.Call("Server.List", none, &reply)
	if err != nil {
		log.Printf("Error listing users: %q\n", err)
	}
	for i := range reply {
		log.Println(reply[i])
	}
}

func (c *CClient) Say(elements []string) {
	var reply Nothing
	c.Client = c.Connect()
	//say needs atg least 2 paramaters.  the first is 'say' the rest are the message
	if len(elements) >= 2 {
		//actual message
		mess := strings.Join(elements[1:], " ")
		message := Message{
			User:   c.Username,
			Target: elements[1],
			Msg:    mess,
		}
		err := c.Client.Call("Server.Say", message, &reply)
		if err != nil {
			log.Printf("Error sending: %q", err)
		}
	} else {
		log.Println("Type 'say', then your message")
	}
}

func (c *CClient) Tell(elements []string) {
	var reply Nothing
	c.Client = c.Connect()
	//tell needs at least 3 paramaters. 'tell' target and message
	if len(elements) > 2 {
		mess := strings.Join(elements[2:], " ")
		message := Message{
			User:   c.Username,
			Target: elements[1],
			Msg:    mess,
		}

		err := c.Client.Call("Server.Tell", message, &reply)
		if err != nil {
			log.Printf("Error sending message: %q", err)
		}
	} else {
		log.Println("Use: 'tell' <user> <msg>")
	}
}

func (c *CClient) Shutdown() {
	//rpc interface requirements
	var request Nothing
	var reply Nothing
	c.Client = c.Connect()
	err := c.Client.Call("Server.Shutdown", request, &reply)
	if err != nil {
		log.Printf("Error exiting: %q", err)
	}
}

func (c *CClient) Quit() {
	var reply Nothing
	c.Client = c.Connect()
	err := c.Client.Call("Server.Quit", c.Username, &reply)
	if err != nil {
		log.Printf("Error logging out: %q", err)
	}
	log.Println("Goodbye :)")
}

func (c *CClient) Connect() *rpc.Client {
	var err error
	if c.Client == nil {
		c.Client, err = rpc.DialHTTP("tcp", c.Address)
		if err != nil {
			log.Panicf("Error getting connected: %q", err)
		}
	}

	return c.Client
}

func (c *CClient) Register() {
	var reply string
	c.Client = c.Connect()
	err := c.Client.Call("Server.Register", c.Username, &reply)
	if err != nil {
		log.Printf("Error registering this user: %q", err)
	} else {
		log.Printf(": %s", reply)
	}
}

func (c *CClient) CheckMessages() {
	var reply []string
	c.Client = c.Connect()
	for {
		err := c.Client.Call("Server.CheckMessages", c.Username, &reply)
		if err != nil {
			log.Fatalln("Goodbye :)")
		}

		for i := range reply {
			log.Println(reply[i])
		}
		//sleep for a second so the cpu doesn't get weighed down
		time.Sleep(time.Second)
	}
}

func readlines(c *CClient) {
	for {
		//read the messages and split on newline
		buffer := bufio.NewReader(os.Stdin)
		str, err := buffer.ReadString('\n')
		line := ""
		if err != nil {
			log.Printf("Error: %q\n", err)
		}
		//strip the whitespace
		line = strings.TrimSpace(str)
		//separate the words into a slice and index == keywords
		elements := strings.Fields(line)
		if strings.HasPrefix(line, "list") {
			c.List()
		} else if strings.HasPrefix(line, "tell") {
			c.Tell(elements)
		} else if strings.HasPrefix(line, "say") {
			c.Say(elements)
		} else if strings.HasPrefix(line, "quit") {
			c.Quit()
			break //break from the go routine

		} else if strings.HasPrefix(line, "help") { //help command instead of making a functino
			log.Println("\n\n" + "'say'  <msg> 		to send a global message\n" + "'tell' <user> <msg> 	to tell a specific user something\n" + "'shutdown' 		to shut entire server off \n" + "'quit' 			to logoff \n")
		} else if strings.HasPrefix(line, "shutdown") {
			c.Shutdown()
			break
		} else {
			log.Println("\n\nError: " + "'" + elements[0] + "'" + " not a valid command. \n" + "Please type 'help' for a list of commands. \n")
		}
	}
}

func main() {
	var c *CClient = &CClient{}
	var host string
	var port string
	flag.StringVar(&c.Username, "user", "fred", "username")
	flag.StringVar(&host, "host", "localhost", "host connection")
	flag.StringVar(&port, "port", "3410", "port connection")

	flag.Parse()
	if !flag.Parsed() {
		log.Panicf("Error parsing client input")
	}
	//if len of the host is more than 0 than the user tried or accidentally provided a host, same 		with port
	if len(host) != 0 {
		c.Address = net.JoinHostPort(host, port)
	} else {
		c.Address = net.JoinHostPort(host, port)
	}
	c.Register()
	go c.CheckMessages() //launch my go routine in the background to constantly check messages
	readlines(c)         // split main function in to two parts
}
