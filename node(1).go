package main

import (
	"bufio"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type Globals struct {
	Port          string
	Successor     string
	Predecessor   string
	Active        bool
	FingerTable   []string
	Next          int
	nSuccessors   []string
	NextSuccessor int
}

type Nothing struct{}

type Bucket struct {
	Values map[string]string
}

type Server chan *Bucket

var gVars = &Globals{
	Port:          "",
	nSuccessors:   make([]string, 20),
	NextSuccessor: 1,
}

var dataBucket = &Bucket{
	Values: make(map[string]string),
}

var server = Server(make(chan *Bucket, 1))

func (s Server) port(line string) error {
	port := appendLocalHost(line)
	if len(port) < 1 {
		log.Println(line, "is not a valid port or address")
		return nil
	}
	gVars.Port = port
	log.Println(gVars.Port)
	return nil
}

func (s Server) Get(key string, reply *string) error {
	if value, present := dataBucket.Values[key]; present {
		*reply = "[" + key + "]  ===>  [" + value + "]" + "   Hosted on: " + appendLocalHost(gVars.Port)
		return nil
	}
	*reply = "The data does not exist"
	return nil
}

func splitKeyIP(line string) (string, string) {
	list := strings.Fields(line)
	key := list[0]
	ip := list[1]
	return key, ip
}

func (s Server) getRequest(line string) error {
	reply := ""
	list := strings.Fields(line)
	key := list[0]
	ip := gVars.Port
	s.FindSuccessor(hashString(key), &ip)
	if err := s.call(ip, "Server.Get", key, &reply); err != nil {
		log.Println(err)
		return nil
	}
	log.Println(reply)
	return nil
}

func (s Server) GetPredecessor(_ string, reply *string) error {
	*reply = gVars.Predecessor
	return nil
}

func (s Server) GetSuccessorList(_ Nothing, reply *[]string) error {
	*reply = gVars.nSuccessors
	return nil
}

func (s Server) fixSuccessor() {
	gVars.nSuccessors = gVars.nSuccessors[1:]
	gVars.nSuccessors = append(gVars.nSuccessors, gVars.Successor)
}

func (s Server) stabilize() error {
	x := ""
	if err := s.call(gVars.Successor, "Server.GetPredecessor", "", &x); err != nil {
		gVars.Successor = gVars.nSuccessors[0]
		gVars.nSuccessors = gVars.nSuccessors[1:]

		log.Println("Stabilize", err)
		return nil
	}
	if x != "" && between(hashString(gVars.Port), hashString(x), hashString(gVars.Successor), false) {
		log.Print("Stabilize: Successors List Changed")
		gVars.Successor = x
	}
	s.notifyRequest()
	var eh Nothing
	s.call(gVars.Successor, "Server.GetSuccessorList", eh, &gVars.nSuccessors)
	s.fixSuccessor()
	return nil
}

func (s Server) create(_ string) error {
	if gVars.Active == true {
		return nil
	}
	gVars.Active = true
	log.Print("NewNode: Creating Ring")
	gVars.Predecessor = ""
	gVars.Successor = gVars.Port
	rpc.Register(server)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", gVars.Port)
	if err != nil {
		log.Fatal("Error! ", err)
	}
	gVars.Active = true
	go http.Serve(listener, nil)
	log.Print("Starting to listen on port ", gVars.Port)
	go func() {
		for {
			s.stabilize()
			s.checkPredecessorRequest()

			time.Sleep(time.Second / 3)
		}
	}()
	go func() {
		for {
			time.Sleep(time.Second)
			var reply Nothing
			s.FixFingers(&reply, &reply)
		}
	}()
	return nil
}

func appendLocalHost(s string) string {
	if strings.HasPrefix(s, ":") {
		return "127.0.0.1" + s
	} else if strings.Contains(s, ":") {
		return s
	} else {
		return ""
	}
}

func (s Server) FindSuccessor(id *big.Int, reply *string) error {
	if between(hashString(gVars.Port), id, hashString(gVars.Successor), true) {
		*reply = gVars.Successor
		return nil
	} else {
		n1 := s.ClosestProceedingNode(id)
		s.call(n1, "Server.FindSuccessor", id, reply)
		return nil
	}
}

func (s Server) join(line string) error {
	s.create(line)
	reply := ""
	ip := appendLocalHost(line)
	if err := s.call(ip, "Server.FindSuccessor", hashString(gVars.Port), &reply); err != nil {
		log.Println(err)
		return nil
	}
	log.Println(reply)
	gVars.Successor = reply
	go func() {
		time.Sleep(3 * time.Second)
		s.getAllRequest()
	}()
	return nil

}

func (s Server) help(line string) error {
	log.Print("Valid commands:")
	log.Print("port <n>")
	log.Print("join <address>")
	log.Print("put <key>, <value>, <address>(optional)")
	log.Print("putrandom <n>")
	log.Print("get <key>")
	log.Print("delete <key>")
	log.Print("dump")
	log.Print("dumpkey <key>")
	log.Print("dumpaddr <address>")
	log.Print("dumpall")
	log.Print("quit")
	return nil
}

func (s Server) quit(line string) error {
	log.Println("Quit")
	s.putAllRequest("")
	os.Exit(0)
	return nil
}

func (s Server) Delete(args []string, reply *string) error {
	key := args[0]
	sender := args[1]
	delete(dataBucket.Values, key)
	if _, present := dataBucket.Values[key]; !present {
		*reply = key + " deleted from " + appendLocalHost(gVars.Port) + "'s bucket"
		log.Println("["+key+"] was deleted from your bucket by", sender)
		return nil
	}
	*reply = "Could not delete."
	return nil
}

func (s Server) deleteRequest(line string) error {
	reply := ""
	list := strings.Fields(line)
	key := list[0]
	ip := gVars.Port
	args := []string{key, appendLocalHost(gVars.Port)}
	if err := s.call(ip, "Server.Delete", args, &reply); err != nil {
		log.Println(err)
		return nil
	}
	log.Println(reply)
	return nil
}

func randString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphabet[b%byte(len(alphabet))]
	}
	return string(bytes)
}

func (s Server) putRandom(line string) error {
	list := strings.Fields(line)
	n, err := strconv.Atoi(list[0])
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key := randString(5)
		value := randString(5)
		line = key + " " + value
		s.putRequest(line)
	}
	return nil
}

func (s Server) GetAll(_ string, reply *map[string]string) error {
	*reply = dataBucket.Values
	return nil
}

func (s Server) getAllRequest() {
	var reply map[string]string
	err := s.call(gVars.Successor, "Server.GetAll", "", &reply)
	if err != nil {
		log.Println("Could not retrieve data from successor")
	}
	for key, value := range reply {
		log.Println(key)
		if between(hashString(gVars.Port), hashString(key), hashString(gVars.Successor), false) {
			dataBucket.Values[key] = value
		}
	}

}

func (s Server) Put(args []string, reply *string) error {
	key := args[0]
	value := args[1]
	dataBucket.Values[key] = value
	if dataBucket.Values[key] == value {
		*reply = value + " Added to " + appendLocalHost(gVars.Port) + "'s bucket"
		return nil
	}
	*reply = "Could not add"
	return nil
}

func (s Server) putRequest(line string) error {
	reply := ""
	list := strings.Fields(line)
	key := list[0]
	value := list[1]
	ip := ""
	s.FindSuccessor(hashString(key), &ip)
	args := []string{key, value, appendLocalHost(gVars.Port)}
	if err := s.call(ip, "Server.Put", args, &reply); err != nil {
		log.Println(err)
		return nil
	}
	log.Println(reply)
	return nil
}

func (s Server) CheckPredecessor(_ string, reply *bool) error {
	*reply = true
	return nil
}

func (s Server) checkPredecessorRequest() {
	if gVars.Predecessor != "" {
		//log.Print("checkPredecessorRequest: this shit is being ran")
		reply := false
		if err := s.call(gVars.Predecessor, "Server.CheckPredecessor", "", reply); err != nil {
			log.Println("Could not contact predecessor")
			gVars.Predecessor = ""
		}
	}
}

func (s Server) Notify(n1 string, reply *string) error {
	if gVars.Predecessor == "" || between(hashString(gVars.Predecessor), hashString(n1), hashString(gVars.Port), false) {
		gVars.Predecessor = n1
		log.Print("Notify: Predecessor is ", gVars.Predecessor)
		*reply = "You are now my predecessor, from " + gVars.Port
	}
	return nil
}

func (s Server) notifyRequest() error {
	reply := ""
	ip := appendLocalHost(gVars.Successor)
	if err := s.call(ip, "Server.Notify", gVars.Port, &reply); err != nil {
		log.Println("Notfify:", err)
		return nil
	} else {

	}
	return nil
}

func (s Server) call(address string, method string, request interface{}, reply interface{}) error {

	conn, err := rpc.DialHTTP("tcp", appendLocalHost(address))

	if err != nil {
		return err
	}

	defer conn.Close()
	conn.Call(method, request, reply)

	return nil
}

func (s Server) dump(_ string) error {
	fmt.Println("Current: ", gVars.Port)
	fmt.Println("Listening: ", gVars.Active)
	fmt.Println("Successor: ", gVars.Successor)
	fmt.Println("Predecessor: ", gVars.Predecessor)
	log.Print("Data: \n")
	for k, v := range dataBucket.Values {
		log.Print("Key:", k, " Value:", v, "\n")
	}
	z := keySize - 10
	for z < keySize+1 {
		log.Println("Finger", z, "is:", gVars.FingerTable[z])
		z += 1
	}
	return nil
}

func (s Server) putAllRequest(_ string) {
	reply := ""
	s.call(gVars.Successor, "Server.PutAll", dataBucket.Values, &reply)
}

func (s Server) PutAll(data map[string]string, _ *string) error {
	for key, value := range data {
		dataBucket.Values[key] = value
	}
	return nil
}

func (s Server) FixFingers(request, _ *Nothing) error {
	gVars.FingerTable[1] = gVars.Successor
	gVars.Next = gVars.Next + 1
	if gVars.Next > keySize {
		gVars.Next = 1
	}
	reply := ""
	err := s.FindSuccessor(jump(gVars.Next), &reply)
	if err != nil {
		log.Println("Fix fingers dun goofed")
		return nil
	}
	gVars.FingerTable[gVars.Next] = reply
	for gVars.Next+1 < keySize && between(hashString(gVars.Port), jump(gVars.Next+1), hashString(reply), false) {
		gVars.Next += 1
		gVars.FingerTable[gVars.Next] = reply
	}
	return nil
}

func (s Server) ClosestProceedingNode(id *big.Int) string {
	for i := keySize; i > 1; i-- {
		if between(hashString(gVars.Port), hashString(gVars.FingerTable[i]), id, false) {
			return gVars.FingerTable[i]
		}
	}
	return gVars.Port
}

func (s Server) testRing(in string) error {
	log.Println(":3410", in, ":3416", between(hashString("127.0.0.1:3410"), hashString(appendLocalHost(in)), hashString("127.0.0.1:3416"), false))
	return nil
}

func main() {
	addrin := flag.String("a", ":3410", "You need a server and or port")
	flag.Parse()
	address := *addrin
	gVars.Port = appendLocalHost(address)
	gVars.FingerTable = make([]string, keySize+1)
	gVars.Next = 0
	fmt.Println(gVars.Port)
	if len(gVars.Port) < 1 {
		log.Fatal(*addrin, " is not a valid address")
	}
	elt := &Bucket{
		Values: make(map[string]string),
	}
	server <- elt

	m := map[string]func(string) error{
		"help":      server.help,
		"port":      server.port,
		"join":      server.join,
		"create":    server.create,
		"put":       server.putRequest,
		"putrandom": server.putRandom,
		"quit":      server.quit,
		"get":       server.getRequest,
		"delete":    server.deleteRequest,
		"t":         server.testRing,
		"dump":      server.dump,
	}

	for {
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if err != nil {
			log.Fatal("Can't read string", err)
		}
		if line == "" {
			server.help("")
		} else {
			str := strings.Fields(line)
			line = strings.Join(str[1:], " ")
			if _, ok := m[str[0]]; ok {
				fmt.Println()
				m[str[0]](line)
			}
		}
	}
}
