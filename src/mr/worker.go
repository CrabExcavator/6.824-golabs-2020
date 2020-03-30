package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for true {
		reply := getJob()
		if reply.Close == true {
			log.Print("close")
			break
		}
		if reply.JobType == "map" {
			x, _ := os.Open(reply.Filename)
			defer x.Close()
			content, _ := ioutil.ReadAll(x)
			kva := mapf(reply.Filename, string(content))
			sort.Sort(ByKey(kva))

			var fileHandle []*os.File
			for i := 0; i < reply.NReduce; i++ {
				x, err := os.OpenFile( /*"./map/map-"+*/ reply.Filename+"-"+strconv.Itoa(i), os.O_APPEND|os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
				if err != nil {
					log.Fatal(err)
				}
				fileHandle = append(fileHandle, x)
			}

			for _, kv := range kva {
				bucket := ihash(kv.Key) % reply.NReduce
				line := kv.Key + " " + kv.Value + "\n"
				file := fileHandle[bucket]
				file.WriteString(line)
			}

			for _, x := range fileHandle {
				x.Close()
			}
			jobDone("map", reply.FileID)
		} else if reply.JobType == "reduce" {
			var fileHandle []*os.File
			for _, fileName := range reply.Filenames {
				x, err := os.Open( /*"./map/map-" +*/ fileName + "-" + strconv.Itoa(reply.Bucket))
				if err != nil {
					log.Fatal(err)
				}
				fileHandle = append(fileHandle, x)
			}

			var intermediaKV []KeyValue
			for _, file := range fileHandle {
				br := bufio.NewReader(file)
				for true {
					line, _, err := br.ReadLine()
					if err == io.EOF {
						break
					}
					s := strings.Split(string(line), " ")
					intermediaKV = append(intermediaKV, KeyValue{Key: s[0], Value: s[1]})
				}
			}

			sort.Sort(ByKey(intermediaKV))
			oname := "mr-out-" + strconv.Itoa(reply.Bucket)
			ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediaKV) {
				j := i + 1
				for j < len(intermediaKV) && intermediaKV[j].Key == intermediaKV[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediaKV[k].Value)
				}
				output := reducef(intermediaKV[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediaKV[i].Key, output)

				i = j
			}

			ofile.Close()

			for _, file := range fileHandle {
				file.Close()
			}
			jobDone("reduce", reply.Bucket)
		} else if reply.JobType == "wait" {
			time.Sleep(time.Second * 1)
		}
	}
}

func jobDone(jobType string, jobID int) {
	args := JobDoneRequest{
		JobType: jobType,
		JobID:   jobID,
	}
	reply := JobDoneReply{}
	call("Master.HandleJobDone", &args, &reply)
}

func getJob() *JobReply {
	args := JobRequest{}
	reply := JobReply{}

	call("Master.GetJob", &args, &reply)

	//log.Print(reply)

	return &reply
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	//log.Print(reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
