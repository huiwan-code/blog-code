package main

import (
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var thread int
var dlDir string
var retryTimes int
var downloadUrl string
var blockSize int

var pwd, _ = os.Getwd()
var metaRootDir = pwd + "/meta/"
func init() {
	flag.StringVar(&downloadUrl, "downloadUrl", "", "下载地址")
	flag.IntVar(&thread, "thread", 5, "下载线程数")
	flag.StringVar(&dlDir, "dlDir", pwd + "/downloads", "下载存放位置")
	flag.IntVar(&retryTimes, "retryTimes", 5, "下载重试次数")
}

type ResourceInfo struct {
	Url						string
	FileSize				int
	FileName				string
	EtagId					string
	LastModified			time.Time
	supportBreakPoint   	bool
	undownloadBlockList		[]*Block
	DownloadedBlockList     []*Block
	sync.RWMutex

}

type Block struct {
	Start 		int
	End 		int
}

var jobCh = make(chan *Block)
var wg = sync.WaitGroup{}
func (ri *ResourceInfo) getFileName() {
	u := strings.Split(ri.Url, "/")
	ri.FileName = u[len(u)-1]
}

// 通过head获取资源信息
func (ri *ResourceInfo) getResourceInfo() (err error) {
	req, err := http.NewRequest("HEAD", ri.Url, nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	ri.FileSize, err = strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return
	}
	ar := resp.Header.Get("Accept-Ranges")
	if ar != "" {
		ri.supportBreakPoint = true
	}
	ri.EtagId = resp.Header.Get("ETag")
	lastModify := resp.Header.Get("Last-Modified")
	if lastModify != "" {
		ri.LastModified, err = time.Parse(time.RFC1123, lastModify)
	}
	return
}

func (ri *ResourceInfo) addUndownloadBlock(start, end int) {
	if start >= end {
		return
	}
	if end - start <= blockSize {
		ri.undownloadBlockList = append(ri.undownloadBlockList, &Block{start, end})
	} else {
		ri.undownloadBlockList = append(ri.undownloadBlockList, &Block{start, start + blockSize})
		ri.addUndownloadBlock(start + blockSize + 1, end)
	}
}

func (ri *ResourceInfo) updateMeta() {
	ri.Lock()
	defer ri.Unlock()
	metaDir := getMetaDir(ri.Url)
	metaFile := filepath.Join(metaDir, "metaData.json")
	b, err := json.Marshal(ri)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(metaFile, b, 0644)
	if err != nil {
		panic(err)
	}
}

func (ri *ResourceInfo) calcUndownloadBlockList() error {

	hashedUrl := hashUrl(ri.Url)
	metaDirPath := metaRootDir + hashedUrl
	// 填充之前已下载的部分
	if pathIsExist(metaDirPath) {
		metaFile := filepath.Join(metaDirPath, "metaData.json")
		cacheRes := new(ResourceInfo)
		metaContent, err := ioutil.ReadFile(metaFile)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(metaContent, cacheRes); err != nil {
			return err
		}
		if cacheRes.LastModified.Unix() == ri.LastModified.Unix() && cacheRes.EtagId == ri.EtagId {
			ri.DownloadedBlockList = cacheRes.DownloadedBlockList
		}
	}
	blockList := [][2]int{[2]int{0, ri.FileSize - 1}}
	// 对已下载的block按start进行排序
	sortBlockList(ri.DownloadedBlockList, 0, len(ri.DownloadedBlockList) - 1)
	for _, block := range ri.DownloadedBlockList {
		lastBlock := blockList[len(blockList) - 1]
		blockList = blockList[:len(blockList) - 1]
		if lastBlock[0] < block.Start {
			blockList = append(blockList, [2]int{lastBlock[0], block.Start - 1})
		}
		if lastBlock[1] > block.End {
			blockList = append(blockList, [2]int{block.End + 1, lastBlock[1]})
		}
	}
	for _, item := range blockList {
		ri.addUndownloadBlock(item[0], item[1])
	}
	return nil
}
// 获取url的md5值，检查之前是否已下载了一半，用于断点续传
func hashUrl(url string) string {
	hashData := md5.Sum([]byte(url))
	return fmt.Sprintf("%x", hashData)
}

func pathIsExist (path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func main() {
	flag.Parse()
	if downloadUrl == "" {
		panic("未指定下载url")
	}
	err := ensureDir(dlDir)
	if err != nil {
		panic(err)
	}

	err = ensureDir(metaRootDir)
	if err != nil {
		panic(err)
	}
	ri := new(ResourceInfo)
	ri.Url = downloadUrl
	ri.getFileName()
	err = ri.getResourceInfo()
	if err != nil {
		fmt.Println(err)
		return
	}
	blockSize = int(math.Ceil(float64(ri.FileSize) / float64(thread)))
	if !ri.supportBreakPoint {
		fmt.Println("资源不支持多线程下载，正在退出")
		return
	}
	err = ri.calcUndownloadBlockList()
	if err != nil {
		panic(err)
	}
	fmt.Println(ri)
	go assignJob(ri)
	for i:=0; i < thread; i++ {
		wg.Add(1)
		go ri.partDownload()
	}
	wg.Wait()
	// merge the file
	ri.mergeFile()
}


func assignJob(ri *ResourceInfo) {
	for _,block := range ri.undownloadBlockList {
		jobCh <- block
	}
	close(jobCh)
}

func (ri *ResourceInfo) mergeFile() {
	retPath := filepath.Join(dlDir, ri.FileName)
	f, err := os.OpenFile(retPath, os.O_CREATE|os.O_WRONLY, 0666)
	defer f.Close()
	if err != nil {
		panic(err)
	}
	sortBlockList(ri.DownloadedBlockList, 0, len(ri.DownloadedBlockList) - 1)
	for _, block := range ri.DownloadedBlockList {
		metaDir := getMetaDir(ri.Url)
		fileName := fmt.Sprintf("%d-%d", block.Start, block.End)
		tmpFile := filepath.Join(metaDir, fileName)
		tf, err := os.Open(tmpFile)
		if err != nil {
			panic(err)
		}
		_, err = io.Copy(f, tf)
		if err != nil {
			panic(err)
		}
	}
}
func (ri *ResourceInfo) partDownload() {
	defer wg.Done()
	for block := range jobCh {
		for i:=0; i < retryTimes; i++ {
			len, err := getRange(ri.Url, block.Start, block.End)
			fmt.Printf("len: %d, start: %d, end: %d, start-end: %d\n", len, block.Start, block.End, (block.End - block.Start))
			if err != nil {
				fmt.Println(err)
				if e, ok := err.(net.Error); ok && e.Temporary(){
					continue
				}
			}
			if int(len) != (block.End - block.Start + 1) {
				continue
			} else {
				ri.DownloadedBlockList = append(ri.DownloadedBlockList, block)
				ri.updateMeta()
			}
			break
		}
	}
}

func getRange(url string, start, end int) (len int64, err error){
	metaDir := getMetaDir(url)
	fileName := fmt.Sprintf("%d-%d", start, end)
	partFile := filepath.Join(metaDir, fileName)
	fmt.Println(partFile)
	f, err := os.Create(partFile)
	if err != nil {
		return
	}
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	len, err = io.Copy(f, resp.Body)
	return
}
// helper
func sortBlockList(blockList []*Block, left, right int) {
	if left >= right {
		return
	}
	selectedBlock := blockList[right]
	pos := left
	for i := left; i < right; i++ {
		if selectedBlock.Start > blockList[i].Start {
			blockList[pos], blockList[i] = blockList[i], blockList[pos]
			pos ++
		}
	}
	blockList[pos], blockList[right] = blockList[right], blockList[pos]
	if pos - 1 > left {
		sortBlockList(blockList, left, pos - 1)
	}
	if pos + 1 < right {
		sortBlockList(blockList, pos + 1, right)
	}
}

func getMetaDir(url string) string {
	hashedUrl := hashUrl(url)
	metaDirPath := metaRootDir + hashedUrl
	if !pathIsExist(metaDirPath) {
		err := os.MkdirAll(metaDirPath, 0777)
		if err != nil {
			panic(err)
		}
	}
	return metaDirPath
}

func ensureDir(dir string) error {
	if !pathIsExist(dir) {
		err := os.MkdirAll(dir, 0777)
		return err
	}
	return nil
}
