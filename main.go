package main

import (
    "github.com/myrp556/mapreduce_demo/mapreduce"
    "container/list"
    "unicode"
    "strconv"
    "strings"
    "time"
    "math"
)

func wordCountMapFunc(m int, value string) *list.List {
    notLetter := func(r rune) bool {
        return !unicode.IsLetter(r)
    }
    fields := strings.FieldsFunc(value, notLetter)

    lis := list.New()
    for _, word := range fields {
        kv := mapreduce.KeyValue{Key: word, Value: "1"}
        lis.PushBack(kv)
    }
    return lis
}

func wordCountReduceFunc(r int, key string, values *list.List) string {
    count := 0
    for kv:=values.Front(); kv!=nil; kv=kv.Next() {
        value := kv.Value.(string)
        intValue, _ := strconv.Atoi(value)
        count += intValue
    }
    return strconv.Itoa(count)
}

func sortMapFunc(m int, value string) *list.List {
    fields := strings.Fields(value)
    //log.Println(value, fields)

    lis := list.New()
    str := ""
    for _, word := range fields {
        //kv := mapreduce.KeyValue{Key: word, Value: "1"}
        if len(str) > 0 {
            str = str + " "
        }
        str = str + word
    }
    kv := mapreduce.KeyValue{Key: strconv.Itoa(m/2), Value: str}
    lis.PushBack(kv)
    return lis
}

func hasRemain(arr [][]int) bool {
    for _, a := range arr {
        if len(a) > 0 {
            return true
        }
    }
    return false
}

func getRemain(arr [][]int) int {
    target := -1
    for i, a := range arr {
        if len(a)>0 && (target<0 || a[0]<arr[target][0]) {
            target = i
        }
    }

    if target>=0 {
        ret := arr[target][0]
        arr[target] = arr[target][1:]
        return ret
    }
    return 0
}

func sortReduceFunc(r int, key string, values *list.List) string {
    ret := ""
    var toMerge [][]int
    for kv:=values.Front(); kv!=nil; kv=kv.Next() {
        //key := kv.Key.(string)
        value := kv.Value.(string)
        //intValue, _ := strconv.Atoi(value)
        fields := strings.Fields(value)
        var lis []int
        for _, v := range fields {
            intV, _ := strconv.Atoi(v)
            lis = append(lis, intV)
        }
        toMerge = append(toMerge, lis)
        //toMerge.PushBack(lis)
        //count += intValue
    }
    for hasRemain(toMerge) {
        v := getRemain(toMerge)
        if len(ret)>0 {
            ret = ret + " "
        }
        ret += strconv.Itoa(v)
    }
    return ret
}

func wordCount() {
    go mapreduce.RunMaster("localhost:7000")
    time.Sleep(200 * time.Microsecond)
    go mapreduce.RunWorker("localhost:7000", "localhost:7001", wordCountMapFunc, wordCountReduceFunc, -1)
    go mapreduce.RunWorker("localhost:7000", "localhost:7002", wordCountMapFunc, wordCountReduceFunc, -1)
    time.Sleep(1 * time.Second)
    job := mapreduce.Job{
        Name: "count",
        NMap: 2,
        NReduce: 1,
        InputPath: "./words",
        WriteKey: true,
        Clearup: true,
        Split: mapreduce.LenSplit,
        Partition: mapreduce.HashPartition,
    }
    mapreduce.SubmitJob("localhost:7000", job)
    <-mapreduce.FinishChannel
}

func sort() {
    go mapreduce.RunMaster("localhost:7000")
    time.Sleep(200 * time.Microsecond)
    go mapreduce.RunWorker("localhost:7000", "localhost:7001", sortMapFunc, sortReduceFunc, -1)
    go mapreduce.RunWorker("localhost:7000", "localhost:7002", sortMapFunc, sortReduceFunc, -1)
    time.Sleep(1 * time.Second)
    //l := 1
    lines := mapreduce.GetFileLineNum("./nums")
    turn := 0
    width := 0
    inputFile := "./nums"
    for {
        name := "sort" + strconv.Itoa(turn)
        width = int(math.Pow(2, float64(turn)))
        chunks := lines / width
        if lines % width>0 {
            chunks ++
        }
        mergeChunks := chunks/2
        if chunks%2 >0 {
            mergeChunks ++
        }
        job := mapreduce.Job{
            Name: name,
            NMap: chunks,
            NReduce: mergeChunks,
            InputPath: inputFile,
            WriteKey: true,
            Clearup: true,
            Split: mapreduce.RowSplit,
            Partition: mapreduce.PosPartition,
            WriteValueLines: true,
        }
        mapreduce.SubmitJob("localhost:7000", job)
        <-mapreduce.FinishChannel

        turn ++
        inputFile = "./tmp/mrtmp." + name + ".values"
        if width*2 >= lines {
            break
        }
    }
}

func main() {
    wordCount()
    //sort()
}
