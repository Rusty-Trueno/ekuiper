package kubeedge

import (
	"context"
	"encoding/json"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/internal/kubeedge/constant"
	"github.com/lf-edge/ekuiper/internal/kubeedge/model"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"

	"github.com/sirupsen/logrus"
)

var (
	typeMap = map[string]string{
		"int":     "bigint",
		"string":  "string",
		"float":   "float",
		"boolean": "boolean",
		"bytes":   "bytea",
	}
)

type Stream struct {
	Name            string
	done            context.Context
	cancel          context.CancelFunc
	fields          map[string]string
	datasource      string
	conn            MQTT.Client
	streamProcessor *processor.StreamProcessor
}

func NewStream(Name, Datasource string, Fields map[string]string, conn MQTT.Client, streamProcessor *processor.StreamProcessor) *Stream {
	ctx, cancel := context.WithCancel(context.Background())
	return &Stream{
		Name:            Name,
		done:            ctx,
		cancel:          cancel,
		fields:          Fields,
		datasource:      Datasource,
		conn:            conn,
		streamProcessor: streamProcessor,
	}
}

func (s *Stream) Watch() {
	logrus.Infof("stream %+v start watch\n", s)
	s.handleStreamOpt()
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
			for k, v := range device.Twin {
				switch k {
				case "datasource":
					s.datasource = *v.Expected.Value
				default:
					s.fields[k] = v.Metadata.Type
				}
			}
			s.handleStreamOpt()
		}); token.Wait() && token.Error() != nil {
		logrus.Errorf("watch stream failed, error is %v", token.Error())
	}
	<-s.done.Done()
	logrus.Info("stop watch twins")
}

func (s *Stream) UnWatch() {
	s.handleStreamDelete()
	s.cancel()
}

func (s *Stream) handleStreamDelete() {
	strs := strings.Split(s.Name, "-")
	name := strs[0] + "_" + strs[1]
	content, err := s.streamProcessor.DropStream(name, ast.TypeStream)
	if err != nil {
		logrus.Errorf("drop stream %s failed, error is %v", name, err)
	} else {
		logrus.Infof("drop stream %s sql succeed, content is %v", name, content)
	}
}

func (s *Stream) handleStreamOpt() {
	//check stream first
	strs := strings.Split(s.Name, "-")
	name := strs[0] + "_" + strings.Join(strs[1:], "_")
	fields := make([]string, 0)

	for k, v := range s.fields {
		field := ""
		field += k
		field += " "
		unit := typeMap[v]
		field += unit
		fields = append(fields, field)
	}

	datasoure := s.datasource

	sql := fmt.Sprintf("create stream %s (%s) with (datasource=\"%s\");", name, strings.Join(fields, ","), datasoure)
	logrus.Infof("stream sql is %s", sql)

	content, err := s.streamProcessor.ExecReplaceStream(sql, ast.TypeStream)
	if err != nil {
		logrus.Errorf("exec stream sql failed, error is %v", err)
	} else {
		logrus.Infof("exec stream sql succeed, content is %v", content)
	}

}
