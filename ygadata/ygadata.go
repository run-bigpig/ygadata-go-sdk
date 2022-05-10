package ygadata

import (
	"encoding/json"
	"errors"
	"sync"
)

const (
	Track      = "track"
	User       = "user"
	UserSet    = "user_set"
	SdkVersion = "1.6.0"
	LibName    = "Golang"
)

// Data 数据信息
type Data struct {
	DeviceId   string `json:"#device_id,omitempty"`
	UserId     string `json:"#user_id,omitempty"`
	AppName    string `json:"#app_name,omitempty"`
	Platform   string `json:"#platform,omitempty"`
	Server     int    `json:"#server,omitempty"`
	Type       string `json:"#type"`
	Time       string `json:"#time"`
	EventName  string `json:"#event_name,omitempty"`
	Properties string `json:"#properties"`
}

// Consumer 为数据实现 IO 操作（写入磁盘或者发送到接收端）
type Consumer interface {
	Add(d Data) error
	Flush() error
	Close() error
}

type YgaAnalytics struct {
	consumer               Consumer
	superProperties        map[string]interface{}
	mutex                  *sync.RWMutex
	dynamicSuperProperties func() map[string]interface{}
}

// New 初始化 YgaAnalytics
func New(c Consumer) YgaAnalytics {
	return YgaAnalytics{
		consumer:        c,
		superProperties: make(map[string]interface{}),
		mutex:           new(sync.RWMutex)}
}

// GetSuperProperties 返回公共事件属性
func (yga *YgaAnalytics) GetSuperProperties() map[string]interface{} {
	result := make(map[string]interface{})
	yga.mutex.RLock()
	mergeProperties(result, yga.superProperties)
	yga.mutex.RUnlock()
	return result
}

// SetSuperProperties 设置公共事件属性
func (yga *YgaAnalytics) SetSuperProperties(superProperties map[string]interface{}) {
	yga.mutex.Lock()
	mergeProperties(yga.superProperties, superProperties)
	yga.mutex.Unlock()
}

// ClearSuperProperties 清除公共事件属性
func (yga *YgaAnalytics) ClearSuperProperties() {
	yga.mutex.Lock()
	yga.superProperties = make(map[string]interface{})
	yga.mutex.Unlock()
}

// SetDynamicSuperProperties 设置动态公共事件属性
func (yga *YgaAnalytics) SetDynamicSuperProperties(action func() map[string]interface{}) {
	yga.mutex.Lock()
	yga.dynamicSuperProperties = action
	yga.mutex.Unlock()
}

// GetDynamicSuperProperties 返回动态公共事件属性
func (yga *YgaAnalytics) GetDynamicSuperProperties() map[string]interface{} {
	result := make(map[string]interface{})
	yga.mutex.RLock()
	if yga.dynamicSuperProperties != nil {
		mergeProperties(result, yga.dynamicSuperProperties())
	}
	yga.mutex.RUnlock()
	return result
}

// Track 追踪一个事件
func (yga *YgaAnalytics) Track(DeviceId, UserId, AppName, Platform, Time, EventName string, Sever int, properties map[string]interface{}) error {
	return yga.track(DeviceId, UserId, AppName, Platform, Time, EventName, Sever, properties)
}

func (yga *YgaAnalytics) track(DeviceId, UserId, AppName, Platform, Time, EventName string, Sever int, properties map[string]interface{}) error {
	if len(EventName) == 0 {
		return errors.New("the event name must be provided")
	}

	// 获取设置的公共属性
	p := yga.GetSuperProperties()

	// 获取动态公共属性
	mergeProperties(p, yga.GetDynamicSuperProperties())

	mergeProperties(p, properties)

	return yga.add(DeviceId, UserId, AppName, Platform, Time, EventName, Track, Sever, p)
}

// UserSet 设置用户属性. 如果同名属性已存在，则用传入的属性覆盖同名属性.
func (yga *YgaAnalytics) UserSet(DeviceId, UserId, AppName, Platform, Time string, Sever int, properties map[string]interface{}) error {
	return yga.user(DeviceId, UserId, AppName, Platform, Time, Sever, UserSet, properties)
}

func (yga *YgaAnalytics) user(DeviceId, UserId, AppName, Platform, Time string, Sever int, DataType string, properties map[string]interface{}) error {
	if properties == nil {
		return errors.New("invalid params for " + DataType + ": properties is nil")
	}
	p := make(map[string]interface{})
	mergeProperties(p, properties)
	return yga.add(DeviceId, UserId, AppName, Platform, Time, User, DataType, Sever, p)
}

// Flush 立即开始数据 IO 操作
func (yga *YgaAnalytics) Flush() error {
	return yga.consumer.Flush()
}

// Close 关闭 YgaAnalytics
func (yga *YgaAnalytics) Close() error {
	return yga.consumer.Close()
}

func (yga *YgaAnalytics) add(DeviceId, UserId, AppName, Platform, Time, EventName, DataType string, Sever int, properties map[string]interface{}) error {
	if len(DeviceId) == 0 && len(UserId) == 0 {
		return errors.New("invalid paramters: device_id and user_id cannot be empty at the same time")
	}
	propertiesjson, _ := json.Marshal(properties)
	Data := Data{
		DeviceId:   DeviceId,
		UserId:     DeviceId,
		AppName:    AppName,
		Platform:   Platform,
		Time:       Time,
		EventName:  EventName,
		Type:       DataType,
		Server:     Sever,
		Properties: string(propertiesjson),
	}

	// 检查数据格式, 并将时间类型数据转为符合格式要求的字符串
	err := formatProperties(&Data, properties)
	if err != nil {
		return err
	}

	return yga.consumer.Add(Data)
}
