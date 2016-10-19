package main

import (
	"mapreduce"
	"os"
	"strings"
	"unicode"
	"strconv"
)

func Divide(c rune) bool {
	return !unicode.IsLetter(c)
}

func Map(document string, value string) (res []mapreduce.KeyValue){
	words := strings.FieldsFunc(value, Divide)
	for _, word := range words {
		res = append(res, mapreduce.KeyValue{word, "1"})
	}
	return res
}

func Reduce(key string, values []string) string {
	var sum int = 0
	for _, value := range values {
		tmp, _ := strconv.Atoi(value)
		sum += tmp
	}
	return strconv.Itoa(sum)
}

func main() {
	mapreduce.RunSingle(5, 3, os.Args[1], Map, Reduce)
}
