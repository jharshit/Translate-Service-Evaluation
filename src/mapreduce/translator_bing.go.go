package mapreduce

import "testing"
import "fmt"
import "container/list"
import "strings"
import "os"
import "bufio"
import "log"
import "sort"
import "strconv"
import "unicode"
import btr "github.com/theplant/bingtranslator/translator"

const (
	nMap    = 10
	nReduce = 1
)

var i = 0

func MapFunc(value string) *list.List {
	fs := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsGraphic(r)
	})
	
	var l = new(list.List)
	for _, v := range fs {
		i++
		btr.SetCredentials("roshni123", "QNHEFX5jVnpOj1tN2VzdAODT7MuxhQs+cmI0LsacAPk=")
		
		res, err := btr.Translate("en", "hi", v, btr.INPUT_TEXT)
		if err != nil {
		    fmt.Println(err)
		}
		
		l.PushBack(KeyValue{Key: i, Value: res[0].Text})
	}	
	
	return l
}

func ReduceFunc(key int, values *list.List) string {
	var result []string
	//j :=0
	for e := values.Front(); e != nil; e = e.Next() {
		value := e.Value.(string)
		result = append(result, value)	
	}
	var strs string = strings.Join(result,"\n")
	return strs
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 RPC.
func checkWorker(t *testing.T, l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *MapReduce {
	file := "sample.txt"
	master := port("master")
	mr := MakeMapReduce(nMap, nReduce, file, master)
	return mr
}

func main(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1)
	}
	// Wait until MR is done
	<-mr.DoneChannel
	checkWorker(t, mr.stats)
	fmt.Printf("  ...Execution completed\n")
}


