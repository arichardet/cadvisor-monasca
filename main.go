package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	kafka "github.com/Shopify/sarama"
	//monasca "github.com/arichardet/cadvisor-monasca/monasca"
)

var (
	broker        = flag.String("broker", "localhost:9092", "kafka broker")
	consumerTopic = flag.String("consumer_topic", "stats", "kafka consumer topic")
	producerTopic = flag.String("producer_topic", "monasca_metrics", "kafka producer topic")
)

// MonascaSpec struct
type MonascaSpec struct {
	MonascaTotal  MonascaTotalSpec
	MonascaUser   MonascaUserSpec
	MonascaSystem MonascaSystemSpec
}

// MonascaTotalSpec struct
type MonascaTotalSpec struct {
	Metric       MetricTotalSpec `json:"metric"`
	Meta         string          `json:"meta"`
	CreationTime string          `json:"creation_time"`
}

// MonascaUserSpec struct
type MonascaUserSpec struct {
	Metric       MetricUserSpec `json:"metric"`
	Meta         string         `json:"meta"`
	CreationTime string         `json:"creation_time"`
}

// MonascaSystemSpec struct
type MonascaSystemSpec struct {
	Metric       MetricSystemSpec `json:"metric"`
	Meta         string           `json:"meta"`
	CreationTime string           `json:"creation_time"`
}

// MetricTotalSpec struct
type MetricTotalSpec struct {
	Name       string         `json:"name"`
	Dimensions DimensionsSpec `json:"dimensions"`
	Timestamp  string         `json:"timestamp"`
	Value      float64        `json:"value"`
}

// MetricUserSpec struct
type MetricUserSpec struct {
	Name       string         `json:"name"`
	Dimensions DimensionsSpec `json:"dimensions"`
	Timestamp  string         `json:"timestamp"`
	Value      float64        `json:"value"`
}

// MetricSystemSpec struct
type MetricSystemSpec struct {
	Name       string         `json:"name"`
	Dimensions DimensionsSpec `json:"dimensions"`
	Timestamp  string         `json:"timestamp"`
	Value      float64        `json:"value"`
}

// DimensionsSpec struct
type DimensionsSpec struct {
	HostName               string `json:"hostname"`
	ContainerName          string `json:"containerName"`
	ContainerId            string `json:"containerId,omitempty"`
	ShippedProjectId       string `json:"shippedProjectId,omitempty"`
	ShippedServiceId       string `json:"shippedServiceId,omitempty"`
	ShippedEnvironmentId   string `json:"shippedEnvironmentId,omitempty"`
	ShippedProjectName     string `json:"shippedProjectName,omitempty"`
	ShippedEnvironmentName string `json:"shippedEnvironmentName,omitempty"`
	ShippedServiceName     string `json:"shippedServiceName,omitempty"`
}

type kafkaStorage struct {
	producer    kafka.AsyncProducer
	topic       string
	machineName string
}

// Creates a kafka consumer utilizing github.com/Shopify/sarama
func newKafkaConsumer(broker string) (kafka.Consumer, error) {
	config := kafka.NewConfig()
	consumer, err := kafka.NewConsumer([]string{broker}, config)

	if err != nil {
		return nil, err
	}
	return consumer, nil
}

// Creates a kafka async producer utilizing github.com/Shopify/sarama
func newKafkaAsyncProducer(brokers string) (kafka.AsyncProducer, error) {
	config := kafka.NewConfig()
	config.Producer.RequiredAcks = kafka.WaitForAll
	var brokerList = []string{brokers}
	producer, err := kafka.NewAsyncProducer(brokerList, config)

	if err != nil {
		return nil, err
	}
	return producer, err
}

// Consume kafka messages
func consumeMessages(producer kafka.AsyncProducer, consumer kafka.PartitionConsumer) error {
	select {
	case message := <-consumer.Messages():
		var dat map[string]interface{}
		if err := json.Unmarshal(message.Value, &dat); err != nil {
			fmt.Printf("Error with Unmarshal: %s\n", err)
		} else {
			monascaTS, monascaUS, monascaSS := createMonascaSpec(dat)
			mtm, mum, msm := marshalMonasca(monascaTS, monascaUS, monascaSS)

			produceMonascaMetrics(producer, dat, mtm, mum, msm)

		}
	case err := <-consumer.Errors():
		return err
	}
	return nil
}

func produceMonascaMetrics(producer kafka.AsyncProducer, dat map[string]interface{}, mtm []byte, mum []byte, msm []byte) {

	ks := &kafkaStorage{
		producer:    producer,
		topic:       *producerTopic,
		machineName: (dat["machine_name"]).(string),
	}
	ks.producer.Input() <- &kafka.ProducerMessage{
		Topic: ks.topic,
		Value: kafka.StringEncoder(mtm),
	}
	ks.producer.Input() <- &kafka.ProducerMessage{
		Topic: ks.topic,
		Value: kafka.StringEncoder(mum),
	}
	ks.producer.Input() <- &kafka.ProducerMessage{
		Topic: ks.topic,
		Value: kafka.StringEncoder(msm),
	}
}

func createMonascaSpec(dat map[string]interface{}) (*MonascaTotalSpec, *MonascaUserSpec, *MonascaSystemSpec) {
	ds := createDimensionsSpec(dat)
	var cpuUsage = dat["container_stats"].(map[string]interface{})["cpu"].(map[string]interface{})["usage"]

	mts := createMetricTotalSpec(ds, dat, cpuUsage.(map[string]interface{}))
	monascaTS := createMonascaTotalSpec(mts)

	mus := createMetricUserSpec(ds, dat, cpuUsage.(map[string]interface{}))
	monascaUS := createMonascaUserSpec(mus)

	mss := createMetricSystemSpec(ds, dat, cpuUsage.(map[string]interface{}))
	monascaSS := createMonascaSystemSpec(mss)

	return monascaTS, monascaUS, monascaSS

}

func createMonascaTotalSpec(metricSpec *MetricTotalSpec) *MonascaTotalSpec {
	ms := &MonascaTotalSpec{
		Metric:       *metricSpec,
		Meta:         "",
		CreationTime: (time.Now()).String(),
	}
	return ms
}

func createMonascaUserSpec(metricSpec *MetricUserSpec) *MonascaUserSpec {
	ms := &MonascaUserSpec{
		Metric:       *metricSpec,
		Meta:         "",
		CreationTime: (time.Now()).String(),
	}
	return ms
}

func createMonascaSystemSpec(metricSpec *MetricSystemSpec) *MonascaSystemSpec {
	ms := &MonascaSystemSpec{
		Metric:       *metricSpec,
		Meta:         "",
		CreationTime: (time.Now()).String(),
	}
	return ms
}

func createMetricTotalSpec(dimensionsSpec *DimensionsSpec, dat map[string]interface{}, cpuUsage map[string]interface{}) *MetricTotalSpec {
	mts := &MetricTotalSpec{
		Name:       "container.cpu.usage.total",
		Dimensions: *dimensionsSpec,
		Timestamp:  (dat["timestamp"]).(string),
		Value:      cpuUsage["total"].(float64),
	}
	return mts
}

func createMetricUserSpec(dimensionsSpec *DimensionsSpec, dat map[string]interface{}, cpuUsage map[string]interface{}) *MetricUserSpec {
	mus := &MetricUserSpec{
		Name:       "container.cpu.usage.user",
		Dimensions: *dimensionsSpec,
		Timestamp:  (dat["timestamp"]).(string),
		Value:      cpuUsage["user"].(float64),
	}
	return mus
}

func createMetricSystemSpec(dimensionsSpec *DimensionsSpec, dat map[string]interface{}, cpuUsage map[string]interface{}) *MetricSystemSpec {
	mss := &MetricSystemSpec{
		Name:       "container.cpu.usage.system",
		Dimensions: *dimensionsSpec,
		Timestamp:  (dat["timestamp"]).(string),
		Value:      cpuUsage["system"].(float64),
	}
	return mss
}

func createDimensionsSpec(dat map[string]interface{}) *DimensionsSpec {
	ds := &DimensionsSpec{
		HostName:      (dat["machine_name"]).(string),
		ContainerName: (dat["container_Name"]).(string),
	}
	_, ok := (dat["container_Id"])
	if ok {
		ds.ContainerId = (dat["container_Id"]).(string)
	}
	_, ok = dat["container_labels"].(map[string]interface{})
	if ok {
		labels := dat["container_labels"].(map[string]interface{})

		_, ok = labels["SHIPPED_PROJECT_ID"]
		if ok {
			ds.ShippedProjectId = labels["SHIPPED_PROJECT_ID"].(string)
		}

		_, ok = labels["SHIPPED_SERVICE_ID"]
		if ok {
			ds.ShippedServiceId = labels["SHIPPED_SERVICE_ID"].(string)
		}

		_, ok = labels["SHIPPED_ENVIRONMENT_ID"]
		if ok {
			ds.ShippedEnvironmentId = labels["SHIPPED_ENVIRONMENT_ID"].(string)
		}

		_, ok = labels["SHIPPED_PROJECT_NAME"]
		if ok {
			ds.ShippedProjectName = labels["SHIPPED_PROJECT_NAME"].(string)
		}

		_, ok = labels["SHIPPED_ENVIRONMENT_NAME"]
		if ok {
			ds.ShippedEnvironmentName = labels["SHIPPED_ENVIRONMENT_NAME"].(string)
		}

		_, ok = labels["SHIPPED_SERVICE_NAME"]
		if ok {
			ds.ShippedServiceName = labels["SHIPPED_SERVICE_NAME"].(string)
		}
	}
	return ds
}

func marshalMonasca(totalSpec *MonascaTotalSpec, userSpec *MonascaUserSpec, systemSpec *MonascaSystemSpec) ([]byte, []byte, []byte) {

	mtm, err := json.Marshal(totalSpec)
	if err != nil {
		fmt.Printf("Error with Marshal %s\n", err)
	}

	mum, err := json.Marshal(userSpec)
	if err != nil {
		fmt.Printf("Error with Marshal %s\n", err)
	}

	msm, err := json.Marshal(systemSpec)
	if err != nil {
		fmt.Printf("Error with Marshal %s\n", err)
	}
	return mtm, mum, msm
}

func main() {
	flag.Parse()

	master, err := newKafkaConsumer(*broker)
	if err != nil {
		fmt.Printf("Error creating consumer: %s\n", err)
	} else {
		consumer, err := master.ConsumePartition(*consumerTopic, 0, 0)
		if err != nil {
			fmt.Printf("Error consuming partition: %s\n", err)
		} else {
			producer, err := newKafkaAsyncProducer(*broker)
			if err != nil {
				fmt.Printf("Error creating Producer %s\n", err)
			} else {
				err = nil
				for i := 0; err == nil; i++ {
					err = consumeMessages(producer, consumer)
				}
				consumer.Close()
				master.Close()
			}
		}
	}
}
