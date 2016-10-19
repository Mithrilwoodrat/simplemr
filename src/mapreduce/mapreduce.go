package mapreduce

import (
	"log"
	"os"
	"strconv"
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

func MapName(fileName string, MapJob int) string {
	return "mrtmp." +  fileName + "-" + strconv.Itoa(MapJob)
}

func (mr *MapReduce) Split(fname string) {
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
	
	for i := 0;i<mr.nMap;i++ {
		f_out, err := os.Create(MapName(fname, i))
		defer f_out.Close()
		if err != nil {
			log.Fatal("Split: ", err);
		}
		next := i+1
		start :=  int64(i) * chunk
		end := int64(next) * chunk
		if end >= size {
			end = size
		}
		f_out.Write(buf[start:end])
	}
}

func RunSingle(nmap int, nreduce int, fname string,
	Map func(string, string) []KeyValue,
	Reduce func(string, []string) string) {
	mr := new(MapReduce)
	mr.nMap = nmap
	mr.nReduce = nreduce
	mr.file = fname
	mr.doneChannel = make(chan bool)

	mr.Split(mr.file)
}
	
