package kubeedge

import (
	"context"
	"encoding/json"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/internal/kubeedge/constant"
	"github.com/lf-edge/ekuiper/internal/kubeedge/model"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/pkg/api"
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

type MqttAction struct {
	Server string `json:"server"`
	Topic  string `json:"topic"`
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
	logrus.Info("stop watch twins")
}

func (r *Rule) UnWatch() {
	r.dropRule()
	r.cancel()
}

func (r *Rule) handleRuleOpt() {
	logrus.Infof("rule sql is %s, actions is %v", r.sql, r.actions)
	strs := strings.Split(r.Name, "-")
	name := strs[0] + "_" + strings.Join(strs[1:], "_")
	// format rule json
	mqtt := MqttAction{
		Server: "tcp://8.140.116.56:1883",
		Topic:  r.actions[0],
	}
	ruleJson := api.Rule{
		Id:  name,
		Sql: r.sql,
		Actions: []map[string]interface{}{
			{
				"mqtt": mqtt,
			},
		},
	}
	ruleJsonStr, err := json.Marshal(ruleJson)
	if err != nil {
		logrus.Errorf("rule json marshal failed, error is %v", err)
		return
	}
	_, err = r.ruleProcessor.GetRuleByName(name)
	if err != nil {
		// not found rule, then create it
		rule, err := r.ruleProcessor.ExecCreate(name, string(ruleJsonStr))
		if err != nil {
			logrus.Errorf("Create rule error : %s.", err)
			return
		} else {
			logrus.Infof("Rule %s was created successfully, please use 'bin/kuiper getstatus rule %s' command to get rule status.", rule.Id, rule.Id)
		}
		// start the rule
		//Start the rule
		rs, err := createRuleState(rule)
		if err != nil {
			logrus.Errorf("create rule %s state failed, error is %v", name, err)
			return
		}
		err = doStartRule(rs, r.ruleProcessor)
		if err != nil {
			logrus.Errorf("start rule %s failed, error is %v", name, err)
			return
		}
		logrus.Infof("start rule %s succeed", name)
	} else {
		_, err := r.ruleProcessor.ExecUpdate(name, string(ruleJsonStr))
		if err != nil {
			logrus.Errorf("update rule %s failed, error is %v", name, err)
			return
		} else {
			logrus.Infof("Rule %s was updated successfully.", name)
		}

		err = restartRule(name, r.ruleProcessor)
		if err != nil {
			logrus.Errorf("restart rule %s failed, error is %v", name, err)
			return
		}
	}
}

func (r *Rule) dropRule() {
	strs := strings.Split(r.Name, "-")
	name := strs[0] + "_" + strings.Join(strs[1:], "_")
	deleteRule(name)
	content, err := r.ruleProcessor.ExecDrop(name)
	if err != nil {
		logrus.Errorf("delete rule %s failed, error is %v", name, err)
		return
	}
	logrus.Infof("delete rule %s succeed, content is %v", name, content)
}
