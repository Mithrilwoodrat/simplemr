package mapreduce

import (
	"log"
	"strconv"
	"hash/fnv"
)

func check_err(err error, func_name string) {
	if err != nil {
		log.Fatal(func_name, err)
	}
}

func mapName(fileName string, MapJob int) string {
	return "mrtmp." +  fileName + "-" + strconv.Itoa(MapJob)
}

func reduceName(fileName string, mapTask int, reduceTask int) string {
	return "mrtmp." + fileName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func mergeName(fileName string, reduceTask int) string {
	return "mrtmp." + fileName + "-res-" + strconv.Itoa(reduceTask)
}

//Fowler–Noll–Vo hash function
func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
