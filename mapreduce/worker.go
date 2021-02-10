package mapreduce

import (
    "net"
    "net/rpc"
    "container/list"
    "os"
)

type Worker struct {
    Address string
    Map func(int, string) *list.List
    Reduce func(int, string, *list.List) string
    nRPC int
    nJob int
    listener net.Listener
    masterAddress string
}

func (worker *Worker) DoJob(args *JobArgs, res *JobReply) error {
    INFO("worker %s do %v %v", worker.Address, args.Type, args.Partition)
    switch args.Type {
    case MapJob:
        doMap(args.Name, args.JobNum, args.OtherNum, args.Partition, worker.Map)
    case ReduceJob:
        doReduce(args.Name, args.JobNum, args.OtherNum, args.Partition, worker.Reduce)
    }
    res.OK = true
    report(worker.masterAddress, worker.Address, args.Type)
    return nil
}

func (worker *Worker) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
    INFO("shutdown worker %s", worker.Address)
    defer os.Exit(0)
    res.NJob = worker.nJob
    res.OK = true
    worker.nRPC = 1
    worker.nJob -= 1
    return nil
}

func register(masterAddress string, workerAddress string) {
    args := &RegisterArgs {WorkerAddress: workerAddress}
    reply := &RegisterReply {}
    DEBUG("register worker to %s", masterAddress)
    ok := call(masterAddress, "Master.Register", args, reply)
    if !ok {
        ERROR("worker call register to %s failed", masterAddress)
    }
}

func report(masterAddress string, workerAddress string, jobType JobType) {
    args := &ReportArgs {WorkerAddress: workerAddress, Type: jobType}
    reply := &ReportReply {}
    DEBUG("report worker to %s", masterAddress)
    ok := call(masterAddress, "Master.Report", args, reply)
    if !ok {
        ERROR("worker call report to %s failed", masterAddress)
    }
}

func RunWorker(masterAddress string, workerAddress string,
    mapFunc func(int, string) *list.List,
    reduceFunc func(int, string, *list.List) string,
    nRPC int) {
    worker := &Worker{Address: workerAddress, masterAddress: masterAddress}
    worker.Map = mapFunc
    worker.Reduce = reduceFunc
    worker.nRPC = nRPC

    rpcs := rpc.NewServer()
    rpcs.Register(worker)
    listen, err := net.Listen("tcp", workerAddress)
    if err != nil {
        ERROR("listen worker error: %v", err)
        return
    }

    DEBUG("worker %s start listen...", workerAddress)
    worker.listener = listen
    register(masterAddress, workerAddress)
    
    for worker.nRPC !=0 {
        conn, err := worker.listener.Accept()
        if err != nil {
            ERROR("worker listener accept error: %v", err)
            break
        } else {
            DEBUG("worker connect to %s", masterAddress)
            worker.nRPC -= 1
            go rpcs.ServeConn(conn)
            worker.nJob += 1
        }
    }
    worker.listener.Close()
    INFO("worker %s exit", workerAddress)
}

