package mapreduce

import (
    "os"
    "os/exec"
    "path"
    "strconv"
    "bufio"
    "encoding/json"
    "sort"
    "fmt"
    "strings"
)

var outputDir string = "./tmp/"

func GetFileLineNum(fileName string) int {
    res, err := exec.Command("wc", "-l", fileName).Output()
    if err != nil {
        ERROR("%v", err)
        return 0
    }
    lines, _ := strconv.Atoi(strings.Split(string(res), " ")[0])
    return lines
}

func saveFilePath(fileName string) string {
    if _, err := os.Stat(outputDir); os.IsNotExist(err) {
        os.Mkdir(outputDir, os.ModePerm)
    }
    return path.Join(outputDir, fileName)
}

func mapFileName(fileName string, numMap int) string {
    fileName = saveFilePath(fileName)
    dir := path.Dir(fileName)
    name := path.Base(fileName)
    return path.Join(dir, "mrtmp." + name + "-" + strconv.Itoa(numMap))
}

func reduceFileName(fileName string, numMap int, numReduce int) string {
    //fileName = saveFilePath(fileName)
    mapFile := mapFileName(fileName, numMap)
    dir := path.Dir(mapFile)
    name := path.Base(mapFile)
    return path.Join(dir, name + "-" + strconv.Itoa(numReduce))
}

func mergeFileName(fileName string, numReduce int) string {
    fileName = saveFilePath(fileName)
    dir := path.Dir(fileName)
    name := path.Base(fileName)
    return path.Join(dir, "mrtmp." + name + "-res-" + strconv.Itoa(numReduce))
}

func resultFileName(fileName string) string {
    fileName = saveFilePath(fileName)
    dir := path.Dir(fileName)
    name := path.Base(fileName)
    return path.Join(dir, "mrtmp." + name)
}

func valueLinesFileName(fileName string) string {
    fileName = saveFilePath(fileName)
    dir := path.Dir(fileName)
    name := path.Base(fileName)
    return path.Join(dir, "mrtmp." + name + ".values")
}

func createMapFile(fileName string, numMap int) *os.File {
    outputFile, err := os.Create(mapFileName(fileName, numMap))
    if err != nil {
        ERROR("create map file %v %d error: %v", fileName, numMap, err)
        return nil
    }
    return outputFile
}

func createReduceFile(fileName string, numMap int, numReduce int) *os.File {
    outputFile, err := os.Create(reduceFileName(fileName, numMap, numReduce))
    if err != nil {
        ERROR("create reduce file %v %d %d error: %v", fileName, numMap, numReduce, err)
        return nil
    }
    return outputFile
}

func createMergeKVFile(fileName string, numReduce int) *os.File {
    outputFile, err := os.Create(mergeFileName(fileName, numReduce))
    if err != nil {
        ERROR("create merge file %v %d error: %v", fileName, numReduce, err)
        return nil
    }
    return outputFile
}

func createResultFile(fileName string) *os.File {
    outputFile, err := os.Create(resultFileName(fileName))
    if err != nil {
        ERROR("create result file %v error: %v", fileName, err)
        return nil
    }
    return outputFile
}

func createValueLinesFile(fileName string) *os.File {
    outputFile, err := os.Create(valueLinesFileName(fileName))
    if err != nil {
        ERROR("create value lines file %v error: %v", fileName, err)
        return nil
    }
    return outputFile
}

func splitInputToMapFiles(fileName string, jobName string, nMap int, splitType SplitType) {
    inputFile, err := os.Open(fileName)
    if err != nil {
        ERROR("split open file error: %v", err)
        return
    }
    defer inputFile.Close()

    var chunkSize int

    switch splitType {
    case LenSplit:
        stat, err := inputFile.Stat()
        if err != nil {
            ERROR("split file stat error: %v", err)
            return
        }
        fileSize := int(stat.Size())
        chunkSize = fileSize / nMap + 1

    case RowSplit:
        lines := GetFileLineNum(fileName)
        DEBUG("%s %d lines", fileName, lines)
        chunkSize = lines / nMap
        if lines % nMap >0 {
            chunkSize += 1
        }
    }

    //outputFile := createMapFile(fileName, 0)
    outputFile := createMapFile(jobName, 0)
    writer := bufio.NewWriter(outputFile)
    sizeCount := 0
    fileCount := 1

    scanner := bufio.NewScanner(inputFile)
    for scanner.Scan() {
        if sizeCount >= int(chunkSize) * fileCount {
            writer.Flush()
            outputFile.Close()
            //outputFile = createMapFile(fileName, fileCount)
            outputFile = createMapFile(jobName, fileCount)
            writer = bufio.NewWriter(outputFile)
            fileCount += 1
        }
        line := scanner.Text() + "\n"
        writer.WriteString(line)
        switch splitType {
        case LenSplit:
            sizeCount += len(line)
        case RowSplit:
            sizeCount += 1
        }
    }
    writer.Flush()
    outputFile.Close()
}

func readMapString(jobName string, num int) (string, bool) {
    name := mapFileName(jobName, num)
    file, err := os.Open(name)
    if err != nil {
        ERROR("readMapString: open map file %s error: %v", name, err)
        return "", false
    }
    stat, err := file.Stat()
    if err != nil {
        ERROR("readMapString: get file %s stat error: %v", name, err)
        return "", false
    }

    size := stat.Size()
    data := make([]byte, size)
    _, err = file.Read(data)
    if err != nil {
        ERROR("readMapString: read file %s error: %v", name, err)
        return "", false
    }
    file.Close()

    return string(data), true
}

func readMergeKV(jobName string, nReduce int) ([]string, *map[string]string) {
    kvMap := make(map[string]string)
    for r:=0; r<nReduce; r++ {
        name := mergeFileName(jobName, r)
        //DEBUG("readMergeKV %s", name)
        file, err := os.Open(name)
        if err != nil {
            ERROR("readReduceKV: read file %s error: %v", name, err)
            return nil, nil
        }
        decoder := json.NewDecoder(file)
        for {
            var kv KeyValue
            err := decoder.Decode(&kv)
            if err != nil {
                break
            }
            kvMap[kv.Key] = kv.Value
        }
        file.Close()
    }

    var keys []string
    for key := range kvMap {
        keys = append(keys, key)
    }
    sort.Strings(keys)
    
    return keys, &kvMap
}

func writeKVFile(jobName string, keys []string, kvMap *map[string]string, withKey bool) {
    file := createResultFile(jobName)

    writer := bufio.NewWriter(file)
    for _, key := range keys {
        if withKey {
            fmt.Fprintf(writer, "%s\t%s\n", key, (*kvMap)[key])
        } else {
            fmt.Fprintf(writer, "%s\n", (*kvMap)[key])
        }
    }
    writer.Flush()
    file.Close()
}

func writeValueLines(jobName string, keys []string, kvMap *map[string]string) {
    file := createValueLinesFile(jobName) 

    writer := bufio.NewWriter(file)
    for _, key := range keys {
        vs := (*kvMap)[key]
        fields := strings.Fields(vs)
        for _, word := range fields {
            fmt.Fprintf(writer, "%s\n", word)
        }
    }
    writer.Flush()
    file.Close()
}

func clearupFiles(jobName string, partition PartitionType, nMap int, nReduce int, removeRes bool) {
    for m:=0; m<nMap; m++ {
        removeFile(mapFileName(jobName, m))
        switch partition {
        case HashPartition:
            for r:=0; r<nReduce; r++ {
                removeFile(reduceFileName(jobName, m, r))
            }
        case PosPartition:
            removeFile(reduceFileName(jobName, m, 0))
        }
    }
    for r:=0; r<nReduce; r++ {
        removeFile(mergeFileName(jobName, r))
    }
    if removeRes {
        removeFile(resultFileName(jobName))
    }
}


