package mapreduce

import (
    "os"
    "net/rpc"
    "hash/fnv"
)

type JobType string
type SplitType string
type PartitionType string

const (
    MapJob      JobType = "MapJob"
    ReduceJob   JobType = "ReduceJob"
)

const (
    LenSplit    SplitType = "LenSplit"
    RowSplit    SplitType = "RowSplit"
)

const (
    HashPartition    PartitionType = "HashPartition"
    PosPartition      PartitionType = "PosPartition"
)

type WorkerInfo struct {
    address string
    idel bool
}

type Job struct {
    Name string
    InputPath string
    NMap int
    NReduce int
    WriteKey bool
    Clearup bool
    Split SplitType
    Partition PartitionType
    WriteValueLines bool
}

type JobArgs struct {
    Type JobType
    Name string
    InputFile string
    JobNum int
    OtherNum int
    Partition PartitionType
}

type JobReply struct {
    OK bool
}

type ShutdownArgs struct {
}

type ShutdownReply struct {
    OK bool
    NJob int
}

type SubmitArgs struct {
    Job Job
}

type SubmitReply struct {
    OK bool
}

type RegisterArgs struct {
    WorkerAddress string
}

type RegisterReply struct {
    OK bool
}

type ReportArgs struct {
    WorkerAddress string
    Type JobType
}

type ReportReply struct {
    OK bool
}

func hash(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}

func removeFile(fileName string) {
    err := os.Remove(fileName)
    if err != nil {
        ERROR("removeFile %s error: %v", fileName, err)
    }
}

func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
    DEBUG("call %s", rpcname)
    c, errx := rpc.Dial("tcp", srv)

    if errx != nil {
        ERROR("call error1: %v", errx)
        return false
    }
    defer c.Close()

    err := c.Call(rpcname, args, reply)
    if err != nil {
        ERROR("call error2: %v", err)
        return false
    }

    return true
}


