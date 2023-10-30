package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

var BackupNReduce int

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

var mapfunc func(string, string) []KeyValue
var reducefunc func(string, []string) string

func mapProcess(number, nReduce int, filename string) {

	// fmt.Printf("NUMBER %d FILENAME %s nReduce %d\n", number, filename, nReduce)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close() // 在函数退出时关闭文件

	// 读取文件内容
	data, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	keys := mapfunc(filename, string(data))

	tmpFileArray := []*os.File{}

	for i := 0; i < nReduce; i++ {

		tmpFileName := "tmpfile-" + strconv.Itoa(number) + "-" + strconv.Itoa(i)

		tmpFile, err := ioutil.TempFile("", tmpFileName)

		// fmt.Printf("CREATE FILE: %s\n", tmpFileName)

		if err != nil {
			fmt.Println("Create tmpFile Error: ", err)
			return
		}
		tmpFileArray = append(tmpFileArray, tmpFile)
	}

	// fmt.Printf("CREAT END\n")

	for _, k := range keys {

		x := ihash(k.Key) % nReduce

		encoder := json.NewEncoder(tmpFileArray[x])
		if err := encoder.Encode(k); err != nil {
			fmt.Println("Write Error: ", err)
			return
		}
	}

	for i := 0; i < nReduce; i++ {

		// 定义目标文件路径
		name := "./mr-tmp-" + strconv.Itoa(number) + "-" + strconv.Itoa(i)

		// 将临时文件原子性地重命名为目标文件
		err = os.Rename(tmpFileArray[i].Name(), name)
		if err != nil {
			fmt.Println("Rename Error: ", err)
			return
		}
		tmpFileArray[i].Close()
	}

}

func reduceProcess(x int) {

	toFind := "mr-tmp-*-" + strconv.Itoa(x)
	files, err := filepath.Glob(toFind)
	if err != nil {
		fmt.Println("Find Error:", err)
		return
	}

	intermediate := []KeyValue{}
	for _, file := range files {

		f, err := os.Open(file) // 打开文件
		// fmt.Println("OPEN FILE: %s", file)
		if err != nil {
			fmt.Println("Open Error:", err)
			continue // 继续处理下一个文件
		}

		scanner := bufio.NewScanner(f)

		// 遍历文件的每一行
		for scanner.Scan() {
			line := scanner.Text()

			tmp := KeyValue{}
			if err := json.Unmarshal([]byte(line), &tmp); err != nil {
				fmt.Println("解析 JSON 错误:", err)
				return
			}
			intermediate = append(intermediate, tmp)
		}

		// 检查扫描是否出错
		if err := scanner.Err(); err != nil {
			fmt.Println("Error scanning file:", err)
		}
		f.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(x)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducefunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	mapfunc, reducefunc = mapf, reducef
	// uncomment to send the Example RPC to the coordinator.
	CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	args := ExampleArgs{0, Task{-1, 0, ""}}
	reply := ExampleReply{}

	for {
		ok := call("Coordinator.Example", &args, &reply)
		if ok {

			// fmt.Printf("reply.TaskType %v reply.Args %v\n", reply.TaskType, reply.Args)
			args.LastType = reply.TaskType
			args.LastTask = reply.Args

			switch reply.TaskType {

			case 0:
				args.LastType = -1
				args.LastTask = Task{-1, 0, ""}
				time.Sleep(time.Second)
			case 1:
				mapProcess(reply.Args.Number, reply.Args.NReduce, reply.Args.FileName)
			case 2:
				x, _ := strconv.Atoi(reply.Args.FileName)
				reduceProcess(x)
			}

			if reply.TaskType == -1 {
				break
			}

			reply.TaskType = 0
			reply.Args = Task{0, 0, ""}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
