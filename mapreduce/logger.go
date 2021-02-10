package mapreduce

import (
    "log"
    "os"
    "fmt"
)

var (
    Debug *log.Logger
    Info  *log.Logger
    Error *log.Logger
    Warn  *log.Logger
)

var OpenLog bool = true
var OpenDebug bool = true

func init() {
    Debug = log.New(os.Stdout, "[DEBUG] ", log.Ldate|log.Ltime)
    Info = log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime)
    Error = log.New(os.Stderr, "[ERROR] ", log.Ldate|log.Ltime)
    Warn = log.New(os.Stderr, "[WARN] ", log.Ldate|log.Ltime)
}

func INFO(pattern string, args ...interface{}) {
    if OpenLog {
        Info.Println(fmt.Sprintf(pattern, args...))
    }
}

func WARN(pattern string, args ...interface{}) {
    Warn.Println(fmt.Sprintf(pattern, args...))
}

func ERROR(pattern string, args ...interface{}) {
    Error.Println(fmt.Sprintf(pattern, args...))
}

func DEBUG(pattern string, args ...interface{}) {
    if OpenLog && OpenDebug {
        Debug.Println(fmt.Sprintf(pattern, args...))
    }
}
