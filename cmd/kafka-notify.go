package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.uber.org/multierr"
	"io"
	"log"
	"log/slog"
	"net"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

const (
	host  = "localhost"
	topic = "my-topic"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("failed to run app: %v", err)
	}
}

func run() (errReturned error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := createTopic(); err != nil {
		return err
	}

	if err := listTopics(); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)

	go func() {
		if err := readTopic(ctx); err != nil {
			slog.Error("readTopic()", "error", err)
		}

		wg.Done()
	}()

	wg.Wait()

	slog.Info("Shut down")
	return nil
}

func createTopic() (errReturned error) {
	conn, err := kafka.Dial("tcp", "0.0.0.0:9092")
	if err != nil {
		return fmt.Errorf("createTopic(): %v", err)
	}
	defer multierr.AppendInvoke(&errReturned, multierr.Close(conn))

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("createTopic(): %v", err)
	}

	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("createTopic(): %v", err)
	}
	defer multierr.AppendInvoke(&errReturned, multierr.Close(controllerConn))

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("createTopic(): %v", err)
	}

	return nil
}

func listTopics() (errReturned error) {
	conn, err := kafka.Dial("tcp", "0.0.0.0:9092")
	if err != nil {
		return fmt.Errorf("listTopics(): %v", err)
	}
	defer multierr.AppendInvoke(&errReturned, multierr.Close(conn))

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("listTopics(): %v", err)
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		slog.Info(k)
	}

	return nil
}

func readTopic(ctx context.Context) (errReturned error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"0.0.0.0:9092"},
		Topic:     topic,
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	defer multierr.AppendInvoke(&errReturned, multierr.Close(r))

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		m, err := r.FetchMessage(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("readTopic(): %v", err)
		}

		slog.Info(fmt.Sprintf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value)))
	}
}
