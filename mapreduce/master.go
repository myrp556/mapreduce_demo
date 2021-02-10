package mapreduce

import (
    "net"
    "net/rpc"
    "os"
    "os/signal"
    "container/list"
)

var ServerReady chan bool

type Master struct {
    Address string

    idelWorkerChannel chan string
    submitChannel chan bool
    mapDoneChannel chan bool
    reduceDoneChannel chan bool
    stopChannel chan bool

    listener net.Listener
    job Job
    workers map[string] *WorkerInfo
    alive bool
}

func createMaster(address string) *Master {
    master := &Master{Address: address}
    master.idelWorkerChannel = make(chan string, 100)
    master.mapDoneChannel = make(chan bool, 100)
    master.reduceDoneChannel = make(chan bool, 100)
    master.submitChannel = make(chan bool)
    master.stopChannel = make(chan bool)
    master.alive = true

    master.workers = make(map[string] *WorkerInfo)
    return master
}

func (master *Master) listenOnExit() {
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, os.Interrupt)
    go func() {
        for s := range ch {
            INFO("inpterrupt %v, exit", s)
            master.killWorkers()
            os.Exit(1)
        }
    } ()
}

func (master *Master) Shutdown(args *ShutdownArgs, reply *ShutdownReply) error {
    DEBUG("shutdown master %s", master.Address)
    defer os.Exit(0)
    close(master.stopChannel)
    master.listener.Close()
    master.killWorkers()
    return nil
}

func (master *Master) killWorkers() *list.List {
    lis := list.New()
    for _, worker := range master.workers {
        INFO("close worker %s", worker.address)
        args := &ShutdownArgs {}
        reply := &ShutdownReply {}
        ok := call(worker.address, "Worker.Shutdown", args, reply)
        if ok {
            lis.PushBack(reply.NJob)
        } else {
            ERROR("close worker %s failed", worker.address)
        }
    }
    return lis
}

func (master *Master) getIdelWorker() string {
    workerAddress := <-master.idelWorkerChannel
    master.workers[workerAddress].idel = false
    return workerAddress
}

func (master *Master) Submit(args *SubmitArgs, res *SubmitReply) error {
    master.job = args.Job
    master.submitChannel <- true
    res.OK = true
    return nil
}

func (master *Master) workerReady(address string) bool {
    if worker, ok:=master.workers[address]; ok && !worker.idel{
        worker.idel = true
        master.idelWorkerChannel <- address
        return true
    }
    return false
}

func (master *Master) Register(args *RegisterArgs, res *RegisterReply) error {
    workerAddress := args.WorkerAddress
    master.workers[workerAddress] = &WorkerInfo{address: workerAddress, idel: false}
    master.workerReady(workerAddress)
    res.OK = true
    DEBUG("%s register",  workerAddress)
    return nil
}

func (master *Master) Report(args *ReportArgs, res *ReportReply) error {
    workerAddress := args.WorkerAddress
    master.workerReady(workerAddress)
    res.OK = true
    DEBUG("%s report", workerAddress)
    return nil
}

func RunMaster(address string) {
    ServerReady = make(chan bool)
    master := createMaster(address)
    master.startRPCServer()
    master.listenOnExit()
    close(ServerReady)
    for {
        master.run()
    }
}

func (master *Master) run() {
    INFO("waitting for job...")
    <-master.submitChannel

    job := master.job
    INFO("run job %s", job.Name)
    splitInputToMapFiles(job.InputPath, job.Name, job.NMap, job.Split)
    master.doJob()
    keys, kvMap := mergeResult(job.Name, job.NReduce, job.WriteKey)

    if job.WriteValueLines {
        writeValueLines(job.Name, keys, kvMap)
    }
    if job.Clearup {
        clearupFiles(job.Name, job.Partition, job.NMap, job.NReduce, false)
    }

    INFO("job %s finished", job.Name)
    close(FinishChannel)
}

func (master *Master) callMap(worker string, index int) {
    jobArgs := &JobArgs {Name: master.job.Name, InputFile: master.job.InputPath, Type: MapJob, JobNum: index, OtherNum: master.job.NReduce, Partition: master.job.Partition}
    reply := &JobReply {}
    DEBUG("call map to %v", worker)
    if !call(worker, "Worker.DoJob", jobArgs, reply) {
        master.workerReady(worker)
    }
    master.mapDoneChannel <- true
}

func (master *Master) callReduce(worker string, index int) {
    jobArgs := &JobArgs {Name: master.job.Name, InputFile: master.job.InputPath, Type: ReduceJob, JobNum: index, OtherNum: master.job.NMap, Partition: master.job.Partition}
    reply := &JobReply {}
    DEBUG("call reduce to %v", worker)
    if !call(worker, "Worker.DoJob", jobArgs, reply) {
        master.workerReady(worker)
    }
    master.reduceDoneChannel <- true
}

func (master *Master) doJob() bool {
    // map
    for i:=0; i<master.job.NMap; i++ {
        worker := master.getIdelWorker()
        go master.callMap(worker, i)
    }
    // wait all map done
    for i:=0; i<master.job.NMap; i++ {
        <-master.mapDoneChannel
    }
    INFO("map has finished")

    // reduce
    for i:=0; i<master.job.NReduce; i++ {
        worker := master.getIdelWorker()
        go master.callReduce(worker, i)
    }
    // wait all reduce done
    for i:=0; i<master.job.NReduce; i++ {
        <-master.reduceDoneChannel
    }
    INFO("reduce has finished")

    return true
}

func (master *Master) startRPCServer() bool {
    rpcs := rpc.NewServer()
    rpcs.Register(master)
    listen, err := net.Listen("tcp", master.Address)
    if err != nil {
        ERROR("register master address=%s failed: %v", master.Address, err)
        return false
    }
    master.listener = listen

    go func() {
        INFO("master %s start listen...", master.Address)
        loop:
        for master.alive {
            select {
            case <-master.stopChannel:
                INFO("stop master listen")
                break loop
            default:
                // do nothing
            }

            conn, err := master.listener.Accept()
            if err != nil {
                DEBUG("master %s accept error: %v", master.Address, err)
            } else {
                go func() {
                    DEBUG("master %s accept connect", master.Address)
                    rpcs.ServeConn(conn)
                    conn.Close()
                } ()
            }
        }
        INFO("master %s listen close")
    } ()

    return true
}


