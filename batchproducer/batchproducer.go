package batchproducer

import (
	"errors"
	"log"
	"time"

	"github.com/timehop/go-kinesis"
)

// BatchProducer collects records individually and then sends them to Kinesis in
// batches in the background using PutRecords, with retries.
// A BatchProducer will do nothing until Start is called.
type BatchProducer interface {
	// Start starts the main goroutine. No need to call it using `go`.
	Start() error
	Stop() error

	// Add might block if the BatchProducer has a buffer and the buffer is full.
	// In order to prevent filling the buffer and eventually blocking indefinitely,
	// Add will fail and return an error if the BatchProducer is not started.
	Add(data []byte, partitionKey string) error

	// Setters
	SetMaxAttemptsPerRecord(int) error
	SetStatReceiver(BatchStatReceiver) error

	// This interval will be used to make a *best effort* attempt to send stats *approximately*
	// when this interval elapses. There’s no guarantee, however, since the main goroutine is
	// used to send the stats and therefore there may be some skew.
	SetStatInterval(time.Duration) error
}

// BatchStatReceiver defines an object that can accept stats.
type BatchStatReceiver interface {
	// Receive will be called by the main BatchProducer goroutine so it will block all batches from being
	// sent, so make sure it is either very fast or never blocks at all!
	Receive(BatchStat)
}

// BatchStat is a kind of a snapshot of activity and happenings. Some of its fields represent
// "moment-in-time" values e.g. BufferSize is the size of the buffer at the moment the Stat is
// sent. Other fields are cumulative since the last Stat, i.e. ErrorsSinceLastStat.
type BatchStat struct {
	// Moment-in-time stats
	BufferSize int

	// Cumulative stats
	KinesisErrorsSinceLastStat           int
	RecordsSentSuccessfullySinceLastStat int
	RecordsDroppedSinceLastStat          int
}

// BatchingKinesisClient is a subset of KinesisClient to ease mocking.
type BatchingKinesisClient interface {
	PutRecords(args *kinesis.RequestArgs) (resp *kinesis.PutRecordsResp, err error)
}

// NewBatchProducer creates and returns a BatchProducer.
// The BatchProducer that is returned will flush a batch to Kinesis whenever either
// the flushInterval occurs (if flushInterval > 0) or the batchSize is reached,
// whichever happens first. If the number of records in the buffer is equal to
// or greater than bufferSize then the Add method will block.
// TODO: this is too many args. Maybe instead have some defaults and add some setter methods for the less critical
// params
func NewBatchProducer(
	client BatchingKinesisClient,
	streamName string,
	bufferSize int,
	flushInterval time.Duration,
	batchSize int,
	logger *log.Logger,
) (BatchProducer, error) {
	if batchSize < 1 || batchSize > 500 {
		return nil, errors.New("batchSize must be between 1 and 500 inclusive")
	}

	if bufferSize < batchSize && flushInterval <= 0 {
		return nil, errors.New("If bufferSize < batchSize && flushInterval <= 0 then the buffer will eventually fill up and Add will block forever.")
	}

	if flushInterval > 0 && flushInterval < 50*time.Millisecond {
		return nil, errors.New("Are you crazy?")
	}

	batchProducer := batchProducer{
		client:               client,
		streamName:           streamName,
		flushInterval:        flushInterval,
		batchSize:            batchSize,
		maxAttemptsPerRecord: 10,
		logger:               logger,
		statInterval:         time.Second,
		currentStat:          new(BatchStat),
		records:              make(chan batchRecord, bufferSize),
		stop:                 make(chan interface{}),
	}

	return &batchProducer, nil
}

type batchProducer struct {
	client               BatchingKinesisClient
	streamName           string
	flushInterval        time.Duration
	batchSize            int
	maxAttemptsPerRecord int
	logger               *log.Logger
	running              bool
	consecutiveErrors    int
	currentDelay         time.Duration
	statInterval         time.Duration
	statReceiver         BatchStatReceiver
	currentStat          *BatchStat
	records              chan batchRecord
	stop                 chan interface{}
}

type batchRecord struct {
	data         []byte
	partitionKey string
	sendAttempts int
}

// from/for interface BatchProducer
func (b *batchProducer) Add(data []byte, partitionKey string) error {
	if !b.running {
		return errors.New("Cannot call Add when BatchProducer is not running (to prevent the buffer filling up and Add blocking indefinitely).")
	}
	b.records <- batchRecord{data: data, partitionKey: partitionKey}
	return nil
}

// from/for interface BatchProducer
func (b *batchProducer) Start() error {
	if b.running {
		return nil
	}

	go b.run()

	for !b.running {
		// Give the goroutine a chance to start before returning.
		time.Sleep(1 * time.Microsecond)
	}

	return nil
}

func (b *batchProducer) run() {
	flushTick := time.Tick(b.flushInterval)

	var statTick <-chan time.Time
	if b.statReceiver != nil {
		statTick = time.Tick(b.statInterval)
	}

	b.running = true
	defer func() { b.running = false }()

	for {
		select {
		case <-flushTick:
			b.sendBatch()
		case <-statTick:
			b.sendStats()
		case <-b.stop:
			break
		default:
			if len(b.records) >= b.batchSize {
				b.sendBatch()
			} else {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

// from/for interface BatchProducer
func (b *batchProducer) Stop() error {
	if b.running {
		b.stop <- true
	}
	return nil
}

func (b *batchProducer) SetMaxAttemptsPerRecord(v int) error {
	if b.running {
		return errors.New("Cannot set max attempts per record while BatchProducer is running.")
	}
	b.maxAttemptsPerRecord = v
	return nil
}

func (b *batchProducer) SetStatReceiver(sr BatchStatReceiver) error {
	if b.running {
		return errors.New("Cannot set BatchStatReceiver while BatchProducer is running.")
	}
	b.statReceiver = sr
	return nil
}

func (b *batchProducer) SetStatInterval(si time.Duration) error {
	if b.running {
		return errors.New("Cannot set stat interval while BatchProducer is running.")
	}
	b.statInterval = si
	return nil
}

func (b *batchProducer) sendBatch() {
	if len(b.records) == 0 {
		return
	}

	if b.consecutiveErrors == 1 {
		b.currentDelay = 50 * time.Millisecond
	} else if b.consecutiveErrors > 1 {
		b.currentDelay *= 2
	}

	if b.currentDelay > 0 {
		b.logger.Printf("Delaying the batch by %v because of %v consecutive errors", b.currentDelay, b.consecutiveErrors)
		time.Sleep(b.currentDelay)
	}

	records := b.takeRecordsFromBuffer()
	res, err := b.client.PutRecords(b.recordsToArgs(records))

	if err != nil {
		b.consecutiveErrors++
		b.currentStat.KinesisErrorsSinceLastStat++
		b.logger.Printf("Error occurred when sending PutRecords request to Kinesis stream %v: %v", b.streamName, err)
		b.returnRecordsToBuffer(records)
	} else {
		b.consecutiveErrors = 0
		b.currentDelay = 0
		succeeded := len(records) - res.FailedRecordCount

		b.currentStat.RecordsSentSuccessfullySinceLastStat += succeeded

		if res.FailedRecordCount > 0 {
			b.logger.Printf("Partial success when sending a PutRecords request to Kinesis stream %v: %v succeeded, %v failed. Re-enqueueing failed records.", b.streamName, succeeded, res.FailedRecordCount)
			b.returnSomeFailedRecordsToBuffer(res, records)
		}
	}
}

func (b *batchProducer) takeRecordsFromBuffer() []batchRecord {
	var size int
	bufferLen := len(b.records)
	if bufferLen >= b.batchSize {
		size = b.batchSize
	} else {
		size = bufferLen
	}

	result := make([]batchRecord, size)
	for i := 0; i < size; i++ {
		result[i] = <-b.records
	}
	return result
}

func (b *batchProducer) recordsToArgs(records []batchRecord) *kinesis.RequestArgs {
	args := kinesis.NewArgs()
	args.Add("StreamName", b.streamName)
	for _, record := range records {
		args.AddRecord(record.data, record.partitionKey)
	}
	return args
}

// TODO: perhaps we should use a deque internally as the buffer so we can return records to
// the front of the queue.
func (b *batchProducer) returnRecordsToBuffer(records []batchRecord) {
	for _, record := range records {
		// Not using b.Add because we want to preserve the value of record.sendAttempts.
		b.records <- record
	}
}

func (b *batchProducer) returnSomeFailedRecordsToBuffer(res *kinesis.PutRecordsResp, records []batchRecord) {
	for i, result := range res.Records {
		record := records[i]
		if result.ErrorCode != "" {
			record.sendAttempts++

			if record.sendAttempts < b.maxAttemptsPerRecord {
				b.logger.Printf("Re-enqueueing failed record to buffer for retry. Error code was: '%v' and message was '%v'", result.ErrorCode, result.ErrorMessage)
				// Not using b.Add because we want to preserve the value of record.sendAttempts.
				b.records <- record
			} else {
				b.currentStat.RecordsDroppedSinceLastStat++
				msg := "NOT re-enqueueing failed record for retry, as it has hit %v attempts, " +
					"which is the maximum. Error code was: '%v' and message was '%v'"
				b.logger.Printf(msg, record.sendAttempts, result.ErrorCode, result.ErrorMessage)
			}
		}
	}
}

func (b *batchProducer) sendStats() {
	b.currentStat.BufferSize = len(b.records)

	// TEMP TMEP TEMP
	// fmt.Printf("Sending stat: %+v\n", b.currentStat)

	// I considered running this as a goroutine, but I’m concerned about leaks. So instead, for now,
	// the provider of the BatchStatReceiver must ensure that it is either very fast or non-blocking.
	b.statReceiver.Receive(*b.currentStat)

	b.currentStat = new(BatchStat)
}
