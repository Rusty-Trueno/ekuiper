package kubeedge

import (
	"context"
	"encoding/json"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/internal/kubeedge/constant"
	"github.com/lf-edge/ekuiper/internal/kubeedge/model"
	"github.com/sirupsen/logrus"
)

type Field struct {
	value string
	_type string
}

type Stream struct {
	Name       string
	done       context.Context
	cancel     context.CancelFunc
	fields     []Field
	datasource string
	conn       MQTT.Client
}

func NewStream(Name, Datasource string, Fields []Field, conn MQTT.Client) *Stream {
	ctx, cancel := context.WithCancel(context.Background())
	return &Stream{
		Name:       Name,
		done:       ctx,
		cancel:     cancel,
		fields:     Fields,
		datasource: Datasource,
		conn:       conn,
	}
}

func (s *Stream) Watch() {
	logrus.Infof("stream %+v start watch", s)
	if token := s.conn.Subscribe(
		constant.DeviceETPrefix+s.Name+constant.TwinETDeltaSuffix,
		0,
		func(client MQTT.Client, message MQTT.Message) {
			var device model.DeviceTwinUpdate
			err := json.Unmarshal(message.Payload(), &device)
			if err != nil {
				logrus.Errorf("json unmarshal err:%v\n", err)
			}
			logrus.Printf("\nsync twins -> un sync twins is %+v", device.Delta)
			//for k, v := range device.Delta {
			//	switch k {
			//
			//	}
			//}
		}); token.Wait() && token.Error() != nil {
		logrus.Errorf("watch stream failed, error is %v", token.Error())
	}
	<-s.done.Done()
	logrus.Info("stop watch twins")
}

func (s *Stream) UnWatch() {
	s.cancel()
}
