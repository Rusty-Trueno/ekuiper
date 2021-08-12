package kubeedge

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/kubeedge/constant"
	"github.com/lf-edge/ekuiper/internal/kubeedge/model"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"strings"
)

type Manager struct {
	Config
	rules   map[string]*Rule
	streams map[string]*Stream
	conn    MQTT.Client
	logger  *logrus.Entry
}

type Config struct {
	Qos     int      `yaml:"qos"`
	Servers []string `yaml:"servers"`
	Node    string   `yaml:"node"`
}

func New() *Manager {
	lg := logrus.WithField("pkg", "kubeedge")
	confPath := "kubeedge.yaml"
	confFile, err := conf.LoadConf(confPath)
	if err != nil {
		logrus.Errorf("read file failed, error is %v", err)
		return nil
	}

	var cfg Config
	err = yaml.Unmarshal(confFile, &cfg)
	if err != nil {
		logrus.Errorf("yaml unmarshal failed, error is %v", err)
		return nil
	}
	return &Manager{
		Config:  cfg,
		logger:  lg,
		streams: make(map[string]*Stream),
		rules:   make(map[string]*Rule),
	}
}

func (m *Manager) StartUp() {
	ctx := context.TODO()
	// init mqtt client
	err := m.initMqtt()
	if err != nil {
		m.logger.Errorf("init mqtt client failed, error is %v", err)
	}
	// get all members
	m.getAllMembers()
	// watch all streams
	m.watchAllStreams()
	// watch all rules
	m.watchAllRules()
	// watch all member status
	go m.watchMemberUpdate(ctx.Done())
	<-ctx.Done()
}

func (m *Manager) initMqtt() error {
	opts := MQTT.NewClientOptions().AddBroker(m.Servers[0]).SetCleanSession(true)

	tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
	opts.SetTLSConfig(tlsConfig)
	opts.OnConnect = func(client MQTT.Client) {
		m.logger.Infof("Connect succeed!\n")
	}
	opts.OnConnect = func(client MQTT.Client) {
		m.logger.Infof("Connect succeed!\n")
	}
	opts.OnConnectionLost = func(client MQTT.Client, err error) {
		m.logger.Infof("Connect lost: %v\n", err)
	}
	opts.AutoReconnect = true
	Client := MQTT.NewClient(opts)
	if token := Client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("connect error: %v\n", token.Error())
	} else {
		m.logger.Info("mqtt client connect succeed!")
	}
	m.conn = Client
	return nil
}

func (m *Manager) getAllMembers() {
	done := make(chan bool)
	var devices model.DeviceList
	if token := m.conn.Subscribe(
		constant.NodeETPrefix+m.Node+constant.DeviceETMemberResultSuffix,
		0,
		func(client MQTT.Client, message MQTT.Message) {
			err := json.Unmarshal(message.Payload(), &devices)
			if err != nil {
				m.logger.Errorf("json unmarshal err:%v\n", err)
			}
			m.logger.Printf("devices is %+v", devices)
			done <- true
		}); token.Wait() && token.Error() != nil {
		m.logger.Errorf("get all pots failed, err is %v", token.Error())
	}
	payload, _ := json.Marshal(struct{}{})
	if token := m.conn.Publish(constant.NodeETPrefix+m.Node+constant.DeviceETMemberGetSuffix, 0, false, payload); token.Wait() && token.Error() != nil {
		m.logger.Errorf("get all pots failed, err is %v", token.Error())
	}
	<-done
	for i := range devices.Devices {
		device := devices.Devices[i]
		id := device.Id
		if strings.HasPrefix(id, "rule-") {
			dd := m.getMemberDetail(id)
			Sql, Actions := formatRule(dd)
			rule := NewRule(id, Sql, Actions, m.conn)
			m.rules[id] = rule
		} else if strings.HasPrefix(id, "stream-") {
			dd := m.getMemberDetail(id)
			Datasource, Fields := formatStream(dd)
			stream := NewStream(id, Datasource, Fields, m.conn)
			m.streams[id] = stream
		}
	}
}

func (m *Manager) watchMemberUpdate(done <-chan struct{}) {
	var memberUpdate model.MemberUpdate
	if token := m.conn.Subscribe(
		constant.NodeETPrefix+m.Node+constant.DeviceETMemberUpdated,
		2,
		func(client MQTT.Client, message MQTT.Message) {
			err := json.Unmarshal(message.Payload(), &memberUpdate)
			if err != nil {
				m.logger.Errorf("json unmarshal failed, error is %v", err)
				return
			}
			m.logger.Infof("update msg is %+v", memberUpdate)
			// handle added event
			for i := range memberUpdate.Added {
				added := memberUpdate.Added[i]
				id := added.Id
				if strings.HasPrefix(id, "rule-") {
					dd := m.getMemberDetail(id)
					Sql, Actions := formatRule(dd)
					rule := NewRule(id, Sql, Actions, m.conn)
					m.rules[id] = rule
					go rule.Watch()
				} else if strings.HasPrefix(id, "stream-") {
					dd := m.getMemberDetail(id)
					Datasource, Fields := formatStream(dd)
					stream := NewStream(id, Datasource, Fields, m.conn)
					m.streams[id] = stream
					go stream.Watch()
				}
			}
			// handle removed event
			for i := range memberUpdate.Removed {
				removed := memberUpdate.Removed[i]
				id := removed.Id
				if strings.HasPrefix(id, "rule-") {
					rule := m.rules[id]
					go rule.UnWatch()
					delete(m.rules, id)
				} else if strings.HasPrefix(id, "stream-") {
					stream := m.streams[id]
					go stream.UnWatch()
					delete(m.streams, id)
				}
			}
		},
	); token.Wait() && token.Error() != nil {
		m.logger.Errorf("failed to subscribe, error is %v", token.Error())
	}
	<-done
}

func (m *Manager) getMemberDetail(id string) *model.DeviceTwinUpdate {
	var device model.DeviceTwinUpdate
	done := make(chan bool)
	if token := m.conn.Subscribe(
		constant.DeviceETPrefix+id+constant.TwinETGetResultSuffix,
		0,
		func(client MQTT.Client, message MQTT.Message) {
			err := json.Unmarshal(message.Payload(), &device)
			if err != nil {
				m.logger.Errorf("json unmarshal err:%v\n", err)
			}
			m.logger.Printf("\nmember-%s is %+v", id, device)
			done <- true
		},
	); token.Wait() && token.Error() != nil {
		m.logger.Errorf("get pot failed, err is %v", token.Error())
	}
	payload, _ := json.Marshal(struct{}{})
	if token := m.conn.Publish(constant.DeviceETPrefix+id+constant.TwinETGetSuffix, 0, false, payload); token.Wait() && token.Error() != nil {
		fmt.Errorf("get pot failed, err is %v", token.Error())
	}
	<-done
	return &device
}

func (m *Manager) watchAllRules() {
	for _, rule := range m.rules {
		go rule.Watch()
	}
}

func (m *Manager) watchAllStreams() {
	for _, stream := range m.streams {
		go stream.Watch()
	}
}

func formatStream(device *model.DeviceTwinUpdate) (Datasource string, Fields map[string]string) {
	Fields = make(map[string]string, 0)
	for k, v := range device.Twin {
		if k == "datasource" {
			Datasource = *v.Expected.Value
		} else {
			Fields[k] = v.Metadata.Type
		}
	}
	return Datasource, Fields
}

func formatRule(device *model.DeviceTwinUpdate) (Sql string, Actions []string) {
	Sql = *device.Twin["sql"].Expected.Value
	Actions = strings.Split(*device.Twin["actions"].Expected.Value, ",")
	return Sql, Actions
}
