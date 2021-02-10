package mapreduce

import (
    "container/list"
    "encoding/json"
    "sort"
    "os"
)

type KeyValue struct {
    Key string
    Value string
}

var FinishChannel chan bool

func hashPartitionMap(m int, nReduce int, jobName string, res *list.List) {
    for r:=0; r<nReduce; r++ {
        reduceFile := createReduceFile(jobName, m, r)
        encoder := json.NewEncoder(reduceFile)
        for e:=res.Front(); e!=nil; e=e.Next() {
            kv := e.Value.(KeyValue)
            if hash(kv.Key)%uint32(nReduce) == uint32(r) {
                err := encoder.Encode(&kv)
                if err != nil {
                    ERROR("write map res to reduce file encode error: %v", err)
                    return
                }
            }
        }
        reduceFile.Close()
    }
}

func posPartitionMap(m int, rReduce int, jobName string, res *list.List) {
    reduceFile := createReduceFile(jobName, m, 0)
    encoder := json.NewEncoder(reduceFile)
    for e:=res.Front(); e!=nil; e=e.Next() {
        kv := e.Value.(KeyValue)
        err := encoder.Encode(&kv)
        if err != nil {
            ERROR("write map res to reduce file encode error: %v", err)
            return
        }
    }
    reduceFile.Close()
}

func readReduceFile(jobName string, m int, r int, kvMap *map[string]*list.List) {
    name := reduceFileName(jobName, m, r)
    reduceFile, err := os.Open(name)
    if err != nil {
        ERROR("decodeReduceFile %s error: %v", jobName, err)
        return
    }
    decoder := json.NewDecoder(reduceFile)
    for {
        var kv KeyValue
        err := decoder.Decode(&kv)
        if err != nil {
            break
        }

        _, ok := (*kvMap)[kv.Key]
        if !ok {
            (*kvMap)[kv.Key] = list.New()
        }
        (*kvMap)[kv.Key].PushBack(kv.Value)
    }
    reduceFile.Close()
}

func decodeReduceFile(jobName string, nMap int, r int, partition PartitionType) ([]string, *map[string]*list.List) {
    kvMap := make(map[string]*list.List)
    switch partition {
    case HashPartition:
        for m:=0; m<nMap; m++ {
            readReduceFile(jobName, m, r, &kvMap)
        }
    case PosPartition:
        readReduceFile(jobName, r*2, 0, &kvMap)
        readReduceFile(jobName, r*2+1, 0, &kvMap)
    }

    var keys []string
    for key := range kvMap {
        keys = append(keys, key)
    }
    sort.Strings(keys)

    return keys, &kvMap
}

func doMap(jobName string, m int, nReduce int, partition PartitionType, mapFunc func(int, string) *list.List) {
    DEBUG("do map for job %v m=%d nReduce=%d", jobName, m, nReduce)
    str, ok := readMapString(jobName, m)
    if ok {
        res := mapFunc(m, str)
        switch partition {
        case HashPartition:
            hashPartitionMap(m, nReduce, jobName, res)
        case PosPartition:
            posPartitionMap(m, nReduce, jobName, res)
        }
    } else {
        ERROR("no map data read")
    }
}

func doReduce(jobName string, r int, nMap int, partition PartitionType, reduceFunc func(int, string, *list.List) string) {
    DEBUG("do reduce for job %v nMap=%d r=%d", jobName, nMap, r)
    keys, kvMap := decodeReduceFile(jobName, nMap, r, partition)
    
    mergeFile := createMergeKVFile(jobName, r)
    encoder := json.NewEncoder(mergeFile)
    for _, key := range keys {
        res := reduceFunc(r, key, (*kvMap)[key])
        encoder.Encode(KeyValue{key, res})
    }

    mergeFile.Close()
}

func mergeResult(jobName string, nReduce int, withKey bool) ([]string, *map[string]string) {
    keys, kvMap := readMergeKV(jobName, nReduce)
    DEBUG("KV len=%d", len(keys))
    writeKVFile(jobName, keys, kvMap, withKey)
    return keys, kvMap
}

func SubmitJob(masterAddress string, job Job) bool {
    /*
    if FinishChannel != nil {
        close(FinishChannel)
    }
    */
    FinishChannel = make(chan bool)

    args := &SubmitArgs{Job: job}
    reply := &SubmitReply{}
    ok := call(masterAddress, "Master.Submit", args, reply)
    if ok {
        INFO("submit %v to %s", job, masterAddress)
        return true
    } else {
        ERROR("submit %v to %s failed", job, masterAddress)
        close(FinishChannel)
        return false
    }
}
