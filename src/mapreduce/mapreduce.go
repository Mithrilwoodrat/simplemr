package mapreduce

import (
	"fmt"
	"encoding/json"
	"log"
	"os"
	"sort"
	"bufio"
)

type KeyValue struct {
	Key   string
	Value string
}

type MapReduce struct {
	nMap        int
	nReduce     int
	file        string // input file name
	doneChannel chan bool
}

func InitMapReduce(nmap int, nreduce int, fname string) *MapReduce {
	mr := new(MapReduce)
	mr.nMap = nmap
	mr.nReduce = nreduce
	mr.file = fname
	mr.doneChannel = make(chan bool)
	return mr
}

func (mr *MapReduce) Split() {
	fname := mr.file
	log.Printf("Split %s\n", fname)
	f_in, err := os.Open(fname)
	if err != nil {
		log.Fatal("Split: ", err)
	}
	defer f_in.Close()
	f_stat, err := f_in.Stat()
	if err != nil {
		log.Fatal("Split: ", err)
	}
	size := f_stat.Size()
	chunk := size / int64(mr.nMap)
	chunk += 1

	buf := make([]byte, size)
	f_in.Read(buf)

	for i := 0; i < mr.nMap; i++ {
		f_out, err := os.Create(mapName(fname, i))
		defer f_out.Close()
		if err != nil {
			log.Fatal("Split: ", err)
		}
		next := i + 1
		start := int64(i) * chunk
		end := int64(next) * chunk
		if end >= size {
			end = size
		}
		f_out.Write(buf[start:end])
	}
}

func (mr *MapReduce) Merge() {
	log.Println("start merge")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := mergeName(mr.file, i)
		log.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)
		defer file.Close()
		check_err(err, "Merge")
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv);
			if err != nil {
				break;
			}
			kvs[kv.Key] = kv.Value
		}
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	name := "mrtmp." + mr.file
	file, err := os.Create(name)
	defer file.Close()
	check_err(err, "Merge: Create")
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	log.Println("Merge Result", name)
}

func Sequential(nmap int,
	nreduce int,
	fname string,
	Map func(string, string) []KeyValue,
	Reduce func(string, []string) string) *MapReduce {
	mr := InitMapReduce(nmap, nreduce, fname)
	mr.Split() // first split input file into number of nmap files
	log.Println("start map tasks")
	for i := 0; i < nmap; i++ {
		DoMap(i, fname, nreduce, Map)
	}
	for i := 0; i < nreduce; i++ {
		DoReduce(i, fname, nmap, Reduce)
	}
	mr.Merge()
	return mr
}

func Parallel(nmap int,
	nreduce int,
	fname string,
	Map func(string, string) []KeyValue,
	Reduce func(string, []string) string) *MapReduce {
	mr := InitMapReduce(nmap, nreduce, fname)
	mr.Split() // first split input file into number of nmap files
	log.Println("start map tasks")
	var mapChan, reduceChan = make(chan int, mr.nMap), make(chan int, mr.nReduce)
	for i := 0; i < nmap; i++ {
		go func(i int) {
			DoMap(i, fname, nreduce, Map)
			mapChan <- i
		}(i)
	}
	
	for i := 0; i < nmap; i++ {
		<- mapChan
	}
	fmt.Println("Map is Done")
	
	for i := 0; i < nreduce; i++ {
		go func (i int) {
			DoReduce(i, fname, nmap, Reduce)
			reduceChan <- i
		}(i)
	}
	
	for i := 0; i < nreduce; i++ {
		<- reduceChan
	}
	
	fmt.Println("Reduce is Done")
	
	mr.Merge()
	return mr
}


func DoMap(jobNumber int, fileName string,
	nReduce int,
	Map func(string, string) []KeyValue) {
	name := mapName(fileName, jobNumber)
	file, err := os.Open(name)
	defer file.Close()
	check_err(err, "DoMap")
	f_stat, err := file.Stat()
	check_err(err, "DoMap")
	size := f_stat.Size()
	log.Printf("DoMap: read split %s %d\n", name, size)
	buff := make([]byte, size)
	_, err = file.Read(buff)
	check_err(err, "DoMap")
	res := Map(name, string(buff))
	for r := 0; r < nReduce; r++ {
		file, err = os.Create(reduceName(fileName, jobNumber, r))
		defer file.Close()
		check_err(err, "DoMap: create")
		enc := json.NewEncoder(file)
		for _, kv := range res {
			//hash key to int, to determin which result file output k/v
			if ihash(kv.Key)%uint32(nReduce) == uint32(r) {
				err := enc.Encode(&kv)
				check_err(err, "DoMap: marshall")
			}
		}
	}
	log.Printf("DoMap[Done]: read split %s %d\n", name, size)
}

func DoReduce(jobNumber int, fileName string, nMap int,
	Reduce func(string, []string) string) {
	kvs := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		name := reduceName(fileName, i, jobNumber)
		log.Printf("DoReduce: read %s\n", name)
		file, err := os.Open(name)
		defer file.Close()
		check_err(err, "DoReduce")
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			//eof
			if err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	name := mergeName(fileName, jobNumber)
	file, err := os.Create(name)
	defer file.Close()
	check_err(err, "DoReduce: create")
	enc := json.NewEncoder(file)
	for _, k := range keys {
		res := Reduce(k, kvs[k])
		enc.Encode(KeyValue{k, res})
	}
	log.Printf("DoMap[Done]: gen file %s\n", name)
}
