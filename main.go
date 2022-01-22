package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"os"
	"otus_hw15/appsinstalled"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type appsInstalled struct {
	DevType  string
	DevId    string
	Lat      float64
	Lon      float64
	Apps     []uint32
	FileName string
}

type fileStats struct {
	sync.Mutex
	stats map[string]int
}

var NormalErrRate = 0.01
var ErrParseLine = errors.New("can not parse line")

func parseLine(line string, filename string) (appsInstalled, error) {
	lineParts := strings.Split(line, "\t")
	if len(lineParts) != 5 {
		return appsInstalled{"", "", 0, 0, []uint32{0}, filename}, ErrParseLine
	}
	// dev_type, dev_id, lat, lon, raw_apps
	if lineParts[0] == "" || lineParts[1] == "" {
		return appsInstalled{"", "", 0, 0, []uint32{0}, filename}, ErrParseLine
	}
	strApps := strings.Split(lineParts[4], ",")
	apps := make([]uint32, len(strApps))
	for i, val := range strApps {
		val32, _ := strconv.Atoi(val)
		apps[i] = uint32(val32)
	}

	lat, _ := strconv.ParseFloat(lineParts[2], 64)
	lon, _ := strconv.ParseFloat(lineParts[2], 64)
	appsInst := appsInstalled{
		lineParts[0],
		lineParts[1],
		lat,
		lon,
		apps,
		filename,
	}
	return appsInst, nil
}

func readFile(
	filename string,
	channels map[string]chan *appsInstalled,
	wg *sync.WaitGroup,
	stats *fileStats,
	logger *log.Logger,
) {

	file, err := os.Open(filename)
	if err != nil {
		logger.Fatal(err)
	}

	zr, err := gzip.NewReader(file)
	if err != nil {
		logger.Fatal(err)
	}

	defer file.Close()
	defer zr.Close()

	nSuccess := 0
	nErrors := 0
	scanner := bufio.NewScanner(zr)
	for scanner.Scan() {
		text := scanner.Text()
		apps, err := parseLine(text, filename)
		if err != nil {
			nErrors += 1
			logger.Println(err)
			continue
		} else {
			nSuccess += 1
			channels[apps.DevType] <- &apps
		}
	}
	err = scanner.Err()
	if err != nil {
		logger.Println("Scanner Error")
	}
	stats.Lock()
	nSendingErrors := stats.stats[filename]
	delete(stats.stats, filename)
	stats.Unlock()

	nErrors += nSendingErrors
	nSuccess -= nSendingErrors
	if nSuccess == 0 {
		logger.Println("All errors. Failed load")
	} else {
		errRate := float64(nErrors) / float64(nSuccess)
		if errRate < NormalErrRate {
			logger.Printf("Acceptable error rate (%f). Successful load", errRate)
		} else {
			logger.Printf("High error rate (%f > %f). Failed load", errRate, NormalErrRate)
		}
	}

	dir, fn := filepath.Split(filename)
	err = os.Rename(filename, fmt.Sprintf("%s/.%s", dir, fn))
	if err != nil {
		logger.Println(err)
	}

	defer wg.Done()
	return
}

func writeInMemcached(
	memClient *memcache.Client,
	channel chan *appsInstalled,
	stats *fileStats,
	maxRetry int,
	timeRetry int,
	logger *log.Logger,
) {
	for val := range channel {
		key := fmt.Sprintf("%s:%s", val.DevType, val.DevId)
		strVal := appsinstalled.UserApps{
			Apps: val.Apps, Lat: &val.Lat, Lon: &val.Lon,
		}
		var err error
		for i := 0; i < maxRetry; i++ {
			err = memClient.Set(&memcache.Item{Key: key, Value: []byte(strVal.String())})
			if err == nil {
				break
			}
			time.Sleep(time.Duration(timeRetry) * time.Second)
		}

		if err != nil {
			logger.Printf("sending failed: %s", err)
			stats.Lock()
			stats.stats[val.FileName] += 1
			stats.Unlock()
		}
		logger.Printf("set %s in %s \n", strVal.String(), val.DevId)
	}
}

func main() {
	channelSize := flag.Int("queue_size", 10, "set channel size")
	maxRetry := flag.Int("max_retry", 3, "set count retry putting in memcached")
	timeRetry := flag.Int("time_retry", 1, "set time between retry putting in memcached")
	pattern := flag.String("pattern", "/home/dmitrii/GolandProjects/otus_hw15/data/appsinstalled/*.tsv.gz", "pattern to glob")
	idfa := flag.String("idfa", "127.0.0.1:33013", "memchache addr to idfa")
	gaid := flag.String("gaid", "127.0.0.1:33014", "memchache addr to gaid")
	adid := flag.String("adid", "127.0.0.1:33015", "memchache addr to adid")
	dvid := flag.String("dvid", "127.0.0.1:33016", "memchache addr to dvid")
	logName := flag.String("log", "", "name to log file")
	memTimeout := flag.Int("timeout", 1, "memclient timeout")

	flag.Parse()

	var logger log.Logger
	if *logName != "" {
		f, err := os.OpenFile(*logName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		logger = *log.New(f, "", log.Ldate|log.Ltime)
	} else {
		logger = *log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}

	var wg sync.WaitGroup
	log.Println("start")
	deviceMemc := map[string]string{
		"idfa": *idfa,
		"gaid": *gaid,
		"adid": *adid,
		"dvid": *dvid,
	}
	// канал для ошибок
	stats := fileStats{}

	// заведем 4 канала
	channels := map[string]chan *appsInstalled{}
	for device, addr := range deviceMemc {
		channels[device] = make(chan *appsInstalled, *channelSize)
		mc := memcache.New(addr)
		mc.Timeout = time.Duration(*memTimeout) * time.Second
		go writeInMemcached(mc, channels[device], &stats, *maxRetry, *timeRetry, &logger)
	}

	fileNames, err := filepath.Glob(*pattern)
	if err != nil {
		logger.Fatal(err)
	}

	for _, fn := range fileNames {
		logger.Println("start read file")
		wg.Add(1)
		go readFile(fn, channels, &wg, &stats, &logger)
	}

	wg.Wait()
}
