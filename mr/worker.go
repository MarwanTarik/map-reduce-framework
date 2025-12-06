package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := TaskReply{}
		ok := call("Coordinator.GetTask", TaskArgs{}, &reply)

		if !ok {
			break
		}

		switch reply.TaskType {
		case Map:
			HandleMap(mapf, reply)

			doneArgs := TaskDoneArgs{TaskType: Map, TaskID: reply.TaskID}
			doneReply := TaskDoneReply{}
			call("Coordinator.TaskDone", &doneArgs, &doneReply)
		case Reduce:
			HandleReduce(reducef, reply)

			doneArgs := TaskDoneArgs{TaskType: Reduce, TaskID: reply.TaskID}
			doneReply := TaskDoneReply{}
			call("Coordinator.TaskDone", &doneArgs, &doneReply)
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			return
		}
	}
}

func HandleMap(mapf func(string, string) []KeyValue, reply TaskReply) error {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
		return nil
	}

	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
		return nil
	}

	kva := mapf(reply.Filename, string(content))

	buckets := make([][]KeyValue, reply.NReduce)

	for _, kv := range kva {
		reduceID := ihash(kv.Key) % reply.NReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}

	for reduceID, bucket := range buckets {
		tempFile := fmt.Sprintf("mr-tmp-%d-%d", reply.TaskID, reduceID)

		ofile, err := os.OpenFile(tempFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open %v", tempFile)
			return nil

		}

		enc := json.NewEncoder(ofile)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode kv")
			}
		}
		ofile.Close()
	}

	return nil
}

func HandleReduce(reducef func(string, []string) string, reply TaskReply) error {
	intermediate := []KeyValue{}

	for mapID := 0; mapID < reply.NMap; mapID++ {
		tempFile := fmt.Sprintf("mr-tmp-%d-%d", mapID, reply.TaskID)

		file, err := os.Open(tempFile)
		if err != nil {
			log.Fatalf("cannot open %v", tempFile)
			return nil
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	keyValues := make(map[string][]string)

	for _, kv := range intermediate {
		keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
	}

	outputFile := fmt.Sprintf("mr-out-%d", reply.TaskID)
	ofile, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("cannot create %v", outputFile)
		return nil
	}

	for key, values := range keyValues {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	ofile.Close()
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
