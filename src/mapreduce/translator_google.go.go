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
import "net/http"
import "encoding/json"
import "io/ioutil"

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
		result := strings.Replace(v, " ", "%20", -1)
		i++
		
		res, err := http.Get("https://www.googleapis.com/language/translate/v2?key=AIzaSyCyiRziUVHjy6QK9_s5TJJHLuga4vLLPsQ&source=en&target=fr&q="+result)
		if err != nil {
			log.Fatal(err)
		}
		
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Fatal(err)
		}
		var data interface{}
		err = json.Unmarshal(body, &data)
    	if err != nil {
       		panic(err.Error())
    	}
		
		md, _ := data.(map[string]interface{})
		md1, _ := md["data"].(map[string]interface{})		
		md2 := md1["translations"].([]interface {})
		md3, _ := md2[0].(interface{})
		md4, _ := md3.(map[string]interface{})
		md5 := md4["translatedText"]
    	
		l.PushBack(KeyValue{Key: i, Value: md5.(string)})
	}	
	
	return l
}

func ReduceFunc(key int, values *list.List) string {
	var result []string
	
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


