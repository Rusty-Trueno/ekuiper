package kubeedge

import (
	"context"
	"encoding/json"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/internal/kubeedge/constant"
	"github.com/lf-edge/ekuiper/internal/kubeedge/model"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/sirupsen/logrus"
	"strings"
)

type Rule struct {
	Name          string
	done          context.Context
	cancel        context.CancelFunc
	sql           string
	actions       []string
	conn          MQTT.Client
	ruleProcessor *processor.RuleProcessor
}

func NewRule(Name, Sql string, Actions []string, conn MQTT.Client, ruleProcessor *processor.RuleProcessor) *Rule {
	ctx, cancel := context.WithCancel(context.Background())
	return &Rule{
		Name:          Name,
		done:          ctx,
		cancel:        cancel,
		sql:           Sql,
		actions:       Actions,
		conn:          conn,
		ruleProcessor: ruleProcessor,
	}
}

func (r *Rule) Watch() {
	r.handleRuleOpt()
	logrus.Infof("rule %+v start watch", r)
	if token := r.conn.Subscribe(
		constant.DeviceETPrefix+r.Name+constant.TwinETDeltaSuffix,
		0,
		func(client MQTT.Client, message MQTT.Message) {
			var device model.DeviceTwinUpdate
			err := json.Unmarshal(message.Payload(), &device)
			if err != nil {
				logrus.Errorf("json unmarshal err:%v\n", err)
			}
			logrus.Printf("\nsync twins -> un sync twins is %+v", device.Delta)
			for k, v := range device.Delta {
				switch k {
				case "sql":
					r.sql = v
				case "actions":
					r.actions = strings.Split(v, ",")
				}
			}
			r.handleRuleOpt()
		}); token.Wait() && token.Error() != nil {
		logrus.Errorf("watch stream failed, error is %v", token.Error())
	}
	<-r.done.Done()
	logrus.Info("stp watch twins")
}

func (r *Rule) UnWatch() {
	r.cancel()
}

func (r *Rule) handleRuleOpt() {
	logrus.Infof("rule sql is %s, actions is %v", r.sql, r.actions)
}
