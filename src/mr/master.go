package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type timer struct {
	startTime time.Time
}

func (t *timer) start() {
	t.startTime = time.Now()
}

func (t *timer) count(td time.Duration) bool {
	timeInterval := time.Since(t.startTime)
	return timeInterval >= td
}

type jobStatus struct {
	finish  bool
	running bool
	clock   timer
}

func newJobStatus() *jobStatus {
	return &jobStatus{
		finish: false,
	}
}

type Master struct {
	// Your definitions here.
	files   []string
	nReduce int

	currentMapJob    int
	currentReduceJob int

	mapDoneArray    []jobStatus
	reduceDoneArray []jobStatus
	mapDone         bool
	reduceDone      bool

	done bool

	mutex sync.Mutex
}

func (m *Master) getJobID() int {
	out := -1
	if m.mapDone {
		for i, x := range m.reduceDoneArray {
			if x.finish == false {
				if x.running == false {
					m.reduceDoneArray[i].running = true
					m.reduceDoneArray[i].clock.start()
					return i
				} else if x.clock.count(time.Second * 10) {
					m.reduceDoneArray[i].clock.start()
					return i
				}
			}
		}
	} else {
		for i, x := range m.mapDoneArray {
			if x.finish == false {
				if x.running == false {
					m.mapDoneArray[i].running = true
					m.mapDoneArray[i].clock.start()
					return i
				} else if x.clock.count(time.Second * 10) {
					m.mapDoneArray[i].clock.start()
					return i
				}
			}
		}
	}
	return out
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	reply.Key = "nihao"
	return nil
}

func (m *Master) GetJob(args *JobRequest, reply *JobReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	isClose := m.mapDone && m.reduceDone
	if isClose {
		m.done = true
		reply.Close = isClose
	} else {
		jobID := m.getJobID()
		if jobID == -1 {
			reply.Close = false
			reply.JobType = "wait"
		} else if m.mapDone {
			reply.Close = isClose
			reply.JobType = "reduce"
			reply.Bucket = jobID
			reply.Filenames = m.files
		} else {
			reply.Close = isClose
			reply.JobType = "map"
			reply.NReduce = m.nReduce
			reply.FileID = jobID
			reply.Filename = m.files[jobID]
		}
	}
	return nil
}

func (m *Master) HandleJobDone(args *JobDoneRequest, reply *JobDoneReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if args.JobType == "map" {
		m.mapDoneArray[args.JobID].finish = true
		m.currentMapJob++
		if m.currentMapJob == len(m.files) {
			m.mapDone = true
		}
	} else {
		m.reduceDoneArray[args.JobID].finish = true
		m.currentReduceJob++
		if m.currentReduceJob == m.nReduce {
			m.reduceDone = true
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.done

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:            files,
		nReduce:          nReduce,
		currentMapJob:    0,
		currentReduceJob: 0,
		mapDone:          false,
		reduceDone:       false,
		done:             false,
	}
	m.mapDoneArray = make([]jobStatus, len(files))
	m.reduceDoneArray = make([]jobStatus, nReduce)

	// Your code here.

	m.server()
	return &m
}
