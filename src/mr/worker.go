package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "path/filepath"
import "sort"
// import "strconv"
import "io/ioutil"
import "encoding/json"
import "time"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(reply *MrReply,id int,mapf func(string, string) []KeyValue){
	filename:=reply.TaskName
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// var ofile []*os.File，没有初始化
	ofile:=make([]*os.File,reply.NReduce)
	//创建并打开该文件
	for i:=0;i<reply.NReduce;i++{
		// ofile[i], _ = os.Create(fmt.Sprintf("mr-%d-%d-%d",id,n,i))
		// ofile[i], _ = os.Create(fmt.Sprintf("mr-%s-%d",reply.TaskName,i))文件名带/匹配不了
		// ofile[i], _ = os.Create(fmt.Sprintf("mr-%d-%d",reply.MapId,i))
		ofile[i], err = ioutil.TempFile("", "mr-tmp-*")
		if err != nil {
			panic(err)
		}
		defer ofile[i].Close()
	}
	for _, kv := range intermediate {
		TaskId:=ihash(kv.Key)%reply.NReduce
		enc := json.NewEncoder(ofile[TaskId])
		err := enc.Encode(&kv)
		if err !=nil{
			log.Fatalf("cannot read %v", filename)
		}
	}
	for i:=0;i<reply.NReduce;i++{
		err = os.Rename(ofile[i].Name(),fmt.Sprintf("mr-%d-%d",reply.MapId,i))
		if err != nil {
			// rename 失败也需清理临时文件
			os.Remove(ofile[i].Name())
			panic(err)
		}
	}
}
func doReduce(reply *MrReply,id int,reducef func(string, []string) string){
	 // 找到所有 mr-*-3 的文件 来自GPT哈哈哈
	pattern := fmt.Sprintf("mr-*-%d", reply.TaskId)
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("glob error: %v", err)
	}
	kvbox:=[]KeyValue{}
	for _,file:=range files{
		f, err := os.Open(file)
		if err!=nil{
			log.Fatalf("glob error: %v", err)
		}
		//来自示例
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvbox = append(kvbox, kv)
		}
	}
	// ofile, err := os.Create(fmt.Sprintf("mr-out-%d",reply.TaskId))
	// if err!=nil{
	// 	log.Fatalf("cannot create output: %v", err)
	// }
	if len(kvbox)==0{
		return
	}
	var ofile *os.File
	ofile, err = ioutil.TempFile("", "mr-tmp-*")
	defer ofile.Close()
	if err != nil {
		panic(err)
	}
	sort.Sort(ByKey(kvbox))
	k:=0
	// var kvStore []KeyValue
	for i,_:= range kvbox{
		if kvbox[i].Key!=kvbox[k].Key{
			// var kv KeyValue
			// kv.Key=kvbox[k].Key
			// kv.Value=strconv.Itoa(i-k)//to do
			// kvStore=append(kvStore,kv)
			values := []string{}
			for j := k; j < i; j++ {
				// values = append(values, kvbox[k].Value)
				values = append(values, kvbox[j].Value)
			}
			output := reducef(kvbox[k].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", kvbox[k].Key, output)
			k=i
		}
	}
	values := []string{}
	for j := k; j < len(kvbox); j++ {
		values = append(values, kvbox[j].Value)
	}
	output := reducef(kvbox[k].Key, values)
	fmt.Fprintf(ofile, "%v %v\n", kvbox[k].Key, output)
	// var kv KeyValue
	// kv.Key=kvbox[len(kvbox)-1].Key
	// kv.Value=strconv.Itoa(len(kvbox)-k)
	// kvStore=append(kvStore,kv)
	// ofile, err := os.Create(fmt.Sprintf("mr-out-%d",reply.TaskId))
	// if err!=nil{
	// 	log.Fatalf("glob error: %v", err)
	// }
	// // enc := json.NewEncoder(ofile)
	// for _,v:=range kvStore{
	// 	// err := enc.Encode(&v)
	// 	fmt.Fprintf(ofile, "%v %v\n", v.Key, v.Value)
	// 	// if err !=nil{
	// 	// 	log.Fatalf("glob error: %v", err)
	// 	// }
	// }
	// ofile.Close()
	err = os.Rename(ofile.Name(),fmt.Sprintf("mr-out-%d",reply.TaskId))
	if err != nil {
		// rename 失败也需清理临时文件
		os.Remove(ofile.Name())
		panic(err)
	}
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	id:=0
	for {
		reply,ret:=CallforTask(id)
		if !ret{
			break
		}
		if id==0{
			if reply.WorkerId !=0{
				id=reply.WorkerId
			}else{
				log.Fatalf("have not id")
			}
		}
		if reply.TaskName !="" && reply.TaskType == 1{
			doMap(&reply,id,mapf)
			//发送完成rpc
			args := MrArgs{}
			args.TaskType=1;
			args.TaskName=reply.TaskName
			CallforTaskFinish(args)
		}else if reply.TaskType == 2 {
			doReduce(&reply,id,reducef)
			args := MrArgs{}
			args.TaskType=2;
			args.TaskId=reply.TaskId
			CallforTaskFinish(args)
		}else{
			time.Sleep(time.Second)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }
func CallforTask(id int) (MrReply,bool){

	// declare an argument structure.
	args := MrArgs{}

	// fill in the argument(s).
	args.WorkerId = id

	// declare a reply structure.
	reply := MrReply{}

	// send the RPC request, wait for the reply.
	ret:=call("Master.GiveTask", &args, &reply)

	// reply.Y should be 100.
	//fmt.Printf("reply.Y %v\n", reply.Y)
	return reply,ret
}
func CallforTaskFinish(args MrArgs) {
	// fill in the argument(s).
	reply := MrReply{}

	// send the RPC request, wait for the reply.
	ret:=call("Master.TaskFinish", &args, &reply)
	if !ret{
		log.Fatal("dialing:taskfinish")
	}
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
		// log.Fatal("dialing:", err)
		//链接不上就退出
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
