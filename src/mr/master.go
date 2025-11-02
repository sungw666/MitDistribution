package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Master struct {
	// Your definitions here.
	mmap map[string]int  // -1 未开始 1开始 2完成
	rtask []int
	NReduce int
	nextId int
	lock sync.Mutex
	mdone bool
	rdone bool
	f2id map[string]int//映射是第几个map任务
	mstart map[string]time.Time
	rstart []time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }
func (m *Master) TaskFinish(args *MrArgs, reply *MrReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if args.TaskType == 1{
		m.mmap[args.TaskName] =2 
	}else if args.TaskType == 2{
		m.rtask[args.TaskId] =2 
	}
	return nil
}
func (m *Master) GiveTask(args *MrArgs, reply *MrReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if args.WorkerId==0{
		reply.WorkerId=m.nextId
		m.nextId++
	}
	rdo,mdo := false,false
	//map
	if !m.mdone{
		for i,v:= range m.mmap{
			if v == -1{
				m.mstart[i]=time.Now()
				reply.TaskName = i
				reply.MapId = m.f2id[i]
				reply.TaskType = 1
				reply.NReduce = m.NReduce
				m.mmap[i]=1;//已分配
				mdo=true
				break
			}
		}
		if mdo == false{
			//没有没在做的任务了，睡眠一下
			// time.Sleep(3 * time.Second)不能持有锁睡眠，不然这3秒谁都干不了事情
			mIsFinished(m)
		}
	}else if m.mdone && !m.rdone{
		for i,v:= range m.rtask{
			if v == -1{
				m.rstart[i]=time.Now()
				reply.TaskId = i
				reply.TaskType = 2
				reply.NReduce = m.NReduce
				m.rtask[i]=1
				rdo=true
				break
			}
		}
		if rdo == false{
			//没有任务了睡眠一下
			// time.Sleep(3 * time.Second)
			rIsFinished(m)
		}
	}
	return nil
}
//到这里已经有锁了
func  mIsFinished(m *Master){
	for i,v :=range m.mmap{
		if v!=2 {
			//先删掉不然会重复做一个任务
		    //现在同一个map或reduce任务生成的文件名字和内容一样了
			//同时两个worker在做也没关系
			// 重复执行浪费资源了
			if v ==1{
				if !m.mstart[i].IsZero()&&(time.Now().Sub(m.mstart[i])>5*time.Second){
					m.mmap[i]=-1
					m.mstart[i]=time.Time{}
				}
			}
			return
		}
	}
	m.mdone=true
}
func  rIsFinished(m *Master){
	for i,v :=range m.rtask{
		if v!=2 {
			//立刻重复执行浪费
			// if v==1{
			// 	m.rtask[i]=-1
			// }
			if v ==1{
				if !m.rstart[i].IsZero()&&(time.Now().Sub(m.rstart[i])>5*time.Second){
					m.rtask[i]=-1
					m.rstart[i]=time.Time{}
				}
			}
			return
		}
	}
	m.rdone=true
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
	ret := false
	m.lock.Lock()
	defer m.lock.Unlock()
	// Your code here.
	if(m.mdone && m.rdone){
		ret=true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mmap=make(map[string]int)
	// Your code here.
	for _,v:= range files{
		m.mmap[v]=-1
	}
	m.NReduce=nReduce
	m.nextId=1
	m.rtask=make([]int,nReduce)//默认零值
	for i,_ := range m.rtask{
		m.rtask[i]=-1//未开始
	}
	m.f2id=make(map[string]int)
	for i,v:= range files{
		m.f2id[v]=i
	}
	m.mstart=make(map[string]time.Time)
	m.rstart=make([]time.Time,nReduce)
	m.server()
	m.mdone=false
	m.rdone=false
	return &m
}
