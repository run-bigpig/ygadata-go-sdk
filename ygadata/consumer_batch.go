package ygadata

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Auth struct {
	Project string `json:"project"`
	User    string `json:"user"`
	Sign    string `json:"sign"`
}

type UploadData struct {
	Auth
	Content string `json:"content"`
}

type BatchConsumer struct {
	serverUrl     string // 接收端地址
	project       string
	user          string
	sk            string
	timeout       time.Duration // 网络请求超时时间, 单位毫秒
	compress      bool          // 是否数据压缩
	bufferMutex   *sync.RWMutex
	cacheMutex    *sync.RWMutex // 缓存锁
	buffer        []UploadData
	batchSize     int
	cacheBuffer   []UploadData // 缓存
	cacheCapacity int          // 缓存最大容量
}

type BatchConfig struct {
	ServerUrl     string // 接收端地址
	Project       string // 项目ID
	User          string // 用户名
	Sk            string // 秘钥
	BatchSize     int    // 批量上传数目
	Timeout       int    // 网络请求超时时间, 单位毫秒
	Compress      bool   // 是否数据压缩
	AutoFlush     bool   // 自动上传
	Interval      int    // 自动上传间隔，单位秒
	CacheCapacity int    // 缓存最大容量
}

const (
	DefaultTimeOut       = 30000 // 默认超时时长 30 秒
	DefaultBatchSize     = 20    // 默认批量发送条数
	MaxBatchSize         = 200   // 最大批量发送条数
	DefaultInterval      = 30    // 默认自动上传间隔 30 秒
	DefaultCacheCapacity = 50
)

// NewBatchConsumer 创建 BatchConsumer
func NewBatchConsumer(serverUrl, project, user, sk string) (Consumer, error) {
	config := BatchConfig{
		ServerUrl: serverUrl,
		Project:   project,
		User:      user,
		Sk:        sk,
		Compress:  true,
	}
	return initBatchConsumer(config)
}

// NewBatchConsumerWithBatchSize 创建指定批量发送条数的 BatchConsumer

func NewBatchConsumerWithBatchSize(serverUrl, project, user, sk string, batchSize int) (Consumer, error) {
	config := BatchConfig{
		ServerUrl: serverUrl,
		Project:   project,
		User:      user,
		Sk:        sk,
		Compress:  true,
		BatchSize: batchSize,
	}
	return initBatchConsumer(config)
}

// NewBatchConsumerWithCompress 创建指定压缩形式的 BatchConsumer

func NewBatchConsumerWithCompress(serverUrl, project, user, sk string, compress bool) (Consumer, error) {
	config := BatchConfig{
		ServerUrl: serverUrl,
		Project:   project,
		User:      user,
		Sk:        sk,
		Compress:  compress,
	}
	return initBatchConsumer(config)
}

//创建指定配置的BatchConsumer

func NewBatchConsumerWithConfig(config BatchConfig) (Consumer, error) {
	return initBatchConsumer(config)
}

func initBatchConsumer(config BatchConfig) (Consumer, error) {
	if config.ServerUrl == "" {
		return nil, errors.New(fmt.Sprint("ServerUrl 不能为空"))
	}
	u, err := url.Parse(config.ServerUrl)
	if err != nil {
		return nil, err
	}
	u.Path = "/logagent"

	var batchSize int
	if config.BatchSize > MaxBatchSize {
		batchSize = MaxBatchSize
	} else if config.BatchSize <= 0 {
		batchSize = DefaultBatchSize
	} else {
		batchSize = config.BatchSize
	}

	var cacheCapacity int
	if config.CacheCapacity <= 0 {
		cacheCapacity = DefaultCacheCapacity
	} else {
		cacheCapacity = config.CacheCapacity
	}

	var timeout int
	if config.Timeout == 0 {
		timeout = DefaultTimeOut
	} else {
		timeout = config.Timeout
	}
	c := &BatchConsumer{
		serverUrl:     u.String(),
		project:       config.Project,
		user:          config.User,
		sk:            config.Sk,
		timeout:       time.Duration(timeout) * time.Millisecond,
		compress:      config.Compress,
		bufferMutex:   new(sync.RWMutex),
		cacheMutex:    new(sync.RWMutex),
		batchSize:     batchSize,
		buffer:        make([]UploadData, 0, batchSize),
		cacheCapacity: cacheCapacity,
		cacheBuffer:   make([]UploadData, 0, cacheCapacity),
	}

	var interval int
	if config.Interval == 0 {
		interval = DefaultInterval
	} else {
		interval = config.Interval
	}
	if config.AutoFlush {
		go func() {
			ticker := time.NewTicker(time.Duration(interval) * time.Second)
			defer ticker.Stop()
			for {
				<-ticker.C
				_ = c.Flush()
			}
		}()
	}
	return c, nil
}

func (c *BatchConsumer) Add(d Data) error {
	c.bufferMutex.Lock()
	dstr, _ := json.Marshal(d)
	c.buffer = append(c.buffer, UploadData{Auth{Project: c.project, User: c.user, Sign: c.MakeSign(d)}, string(dstr)})
	c.bufferMutex.Unlock()

	if c.getBufferLength() >= c.batchSize || c.getCacheLength() > 0 {
		err := c.Flush()
		return err
	}

	return nil
}

func (c *BatchConsumer) Flush() error {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	c.bufferMutex.Lock()
	defer c.bufferMutex.Unlock()

	if len(c.buffer) == 0 && len(c.cacheBuffer) == 0 {
		return nil
	}

	defer func() {
		if len(c.cacheBuffer) > c.cacheCapacity {
			c.cacheBuffer = c.cacheBuffer[1:]
		}
	}()

	if len(c.cacheBuffer) == 0 || len(c.buffer) >= c.batchSize {
		for _, v := range c.buffer {
			c.cacheBuffer = append(c.cacheBuffer, v)
		}
		c.buffer = make([]UploadData, 0, c.batchSize)
	}
	err := c.uploadEvents()
	return err
}

func (c *BatchConsumer) uploadEvents() error {
	buffers := make([]UploadData, len(c.cacheBuffer))
	//取出将要上传的数据
	copy(buffers[:], c.cacheBuffer)
	//清除缓存的数据
	c.cacheBuffer = make([]UploadData, 0, c.cacheCapacity)
	for _, buffer := range buffers {
		jdata, err := json.Marshal(buffer)
		if err == nil {
			params := parseTime(jdata)
			go func() {
				for i := 0; i < 3; i++ {
					statusCode, code, msg, err := c.send(params)
					if statusCode == 200 && code == 10000 && err == nil {
						break
					} else {
						log.Println(params, "==>", msg)
						if i == 2 {
							log.Println(params, "-全部重试都失败,请检查")
						}
					}
				}
			}()
		}
	}
	return nil
}

func (c *BatchConsumer) FlushAll() error {
	for c.getCacheLength() > 0 || c.getBufferLength() > 0 {
		if err := c.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (c *BatchConsumer) Close() error {
	return c.FlushAll()
}

//上报数据

func (c *BatchConsumer) send(data string) (statusCode int, Code int, Msg string, err error) {
	var encodedData string
	data = fmt.Sprintf("data=%s", Base64Encode(data))
	var compressType = "gzip"
	if c.compress {
		encodedData, err = encodeData(data)
	} else {
		encodedData = data
		compressType = "none"
	}
	if err != nil {
		return 0, 0, "数据压缩失败", err
	}
	postData := bytes.NewBufferString(encodedData)

	var resp *http.Response
	req, _ := http.NewRequest("POST", c.serverUrl, postData)
	req.Header.Set("user-agent", "yga-go-sdk")
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	req.Header.Set("version", SdkVersion)
	req.Header.Set("lib", LibName)
	req.Header.Set("compress", compressType)
	client := &http.Client{Timeout: c.timeout}
	resp, err = client.Do(req)

	if err != nil {
		return 0, 0, "HTTP上报失败", err
	}

	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		var result struct {
			Status int
			Msg    string
		}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return resp.StatusCode, 1, "响应数据解析失败", err
		}
		return resp.StatusCode, result.Status, result.Msg, nil
	} else {
		return resp.StatusCode, -1, "获取响应失败", nil
	}
}

// Gzip 压缩
func encodeData(data string) (string, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)

	_, err := gw.Write([]byte(data))
	if err != nil {
		gw.Close()
		return "", err
	}
	gw.Close()

	return string(buf.Bytes()), nil
}

func (c *BatchConsumer) getBufferLength() int {
	c.bufferMutex.RLock()
	defer c.bufferMutex.RUnlock()
	return len(c.buffer)
}

func (c *BatchConsumer) getCacheLength() int {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	return len(c.cacheBuffer)
}

func (c *BatchConsumer) MakeSign(d Data) string {
	content, err := json.Marshal(d)
	if err != nil {
		log.Println("json编码失败", err)
		return ""
	}
	signstr := Md5([]byte(fmt.Sprintf("content=%s&project=%s&sk=%s&user=%s", CleanSpaces(string(content)), c.project, c.sk, c.user)))
	return signstr
}
