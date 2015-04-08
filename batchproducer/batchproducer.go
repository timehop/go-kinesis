package batchproducer

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/timehop/go-kinesis"
)

// Producer collects records individually and then sends them to Kinesis in
// batches in the background using PutRecords, with retries.
// A Producer will do nothing until Start is called.
type Producer interface {
	// Start starts the main goroutine. No need to call it using `go`.
	Start() error

	// Stop signals the main goroutine to finish. Once this is called, Add will immediately start
	// returning errors (unless and until Start is called again). Stop will block until
	// all remaining records in the buffer have been sent.
	Stop() error

	// Add might block if the BatchProducer has a buffer and the buffer is full.
	// In order to prevent filling the buffer and eventually blocking indefinitely,
	// Add will fail and return an error if the BatchProducer is stopped or stopping. Note
	// that it’s critical to check the return value because the BatchProducer could have
	// died in the background due to a panic (or something).
	Add(data []byte, partitionKey string) error

	// Setters
	SetMaxAttemptsPerRecord(int) error
	SetStatReceiver(StatReceiver) error

	// This interval will be used to make a *best effort* attempt to send stats *approximately*
	// when this interval elapses. There’s no guarantee, however, since the main goroutine is
	// used to send the stats and therefore there may be some skew.
	SetStatInterval(time.Duration) error
}

// StatReceiver defines an object that can accept stats.
type StatReceiver interface {
	// Receive will be called by the main Producer goroutine so it will block all batches from being
	// sent, so make sure it is either very fast or never blocks at all!
	Receive(StatsFrame)
}

// StatsFrame is a kind of a snapshot of activity and happenings. Some of its fields represent
// "moment-in-time" values e.g. BufferSize is the size of the buffer at the moment the StatsFrame
// is sent. Other fields are cumulative since the last StatsFrame, i.e. ErrorsSinceLastStat.
type StatsFrame struct {
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

// New creates and returns a BatchProducer that will do nothing until its Start method is called.
// Once it is started, it will flush a batch to Kinesis whenever either
// the flushInterval occurs (if flushInterval > 0) or the batchSize is reached,
// whichever happens first.
// `bufferSize` is the size of the buffer that stores records before they are sent to the Kinesis
// stream. If the number of records in the buffer is equal to or greater than bufferSize then the
// Add method will block.
// `batchSize` is the max number of records in each batch request sent to Kinesis.
// TODO: this is too many args. Maybe instead have some defaults and add some setter methods for the less critical
// params
func New(
	client BatchingKinesisClient,
	streamName string,
	bufferSize int,
	flushInterval time.Duration,
	batchSize int,
	logger *log.Logger,
) (Producer, error) {
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
		currentStat:          new(StatsFrame),
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
	runningMu            sync.Mutex
	consecutiveErrors    int
	currentDelay         time.Duration
	statInterval         time.Duration
	statReceiver         StatReceiver
	currentStat          *StatsFrame
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
	if !b.isRunning() {
		return errors.New("Cannot call Add when BatchProducer is not running (to prevent the buffer filling up and Add blocking indefinitely).")
	}
	b.records <- batchRecord{data: data, partitionKey: partitionKey}
	return nil
}

// from/for interface BatchProducer
func (b *batchProducer) Start() error {
	if b.isRunning() {
		return nil
	}

	go b.run()

	for !b.isRunning() {
		time.Sleep(1 * time.Millisecond)
	}

	return nil
}

func (b *batchProducer) run() {
	flushTicker := &time.Ticker{}
	if b.flushInterval > 0 {
		flushTicker = time.NewTicker(b.flushInterval)
		defer flushTicker.Stop()
	}

	statTicker := &time.Ticker{}
	if b.statReceiver != nil && b.statInterval > 0 {
		statTicker = time.NewTicker(b.statInterval)
		defer statTicker.Stop()
	}

	b.setRunning(true)
	defer b.setRunning(false)

	for {
		select {
		case <-flushTicker.C:
			b.sendBatch()
		case <-statTicker.C:
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
	// TODO: Immediately stop accepting new records by Add, then block until all the records in the buffer have been sent
	if b.isRunning() {
		b.stop <- true
	}
	return nil
}

func (b *batchProducer) SetMaxAttemptsPerRecord(v int) error {
	if b.isRunning() {
		return errors.New("Cannot set max attempts per record while Producer is running.")
	}
	b.maxAttemptsPerRecord = v
	return nil
}

func (b *batchProducer) SetStatReceiver(sr StatReceiver) error {
	if b.isRunning() {
		return errors.New("Cannot set StatReceiver while Producer is running.")
	}
	b.statReceiver = sr
	return nil
}

func (b *batchProducer) SetStatInterval(si time.Duration) error {
	if b.isRunning() {
		return errors.New("Cannot set stat interval while Producer is running.")
	}
	b.statInterval = si
	return nil
}

func (b *batchProducer) setRunning(running bool) {
	b.runningMu.Lock()
	defer b.runningMu.Unlock()
	b.running = running
}

func (b *batchProducer) isRunning() bool {
	b.runningMu.Lock()
	defer b.runningMu.Unlock()
	return b.running
}

func (b *batchProducer) sendBatch() {
	if len(b.records) == 0 {
		return
	}

	// In the future, maybe this could be a RetryPolicy or something
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

	// I considered running this as a goroutine, but I’m concerned about leaks. So instead, for now,
	// the provider of the BatchStatReceiver must ensure that it is either very fast or non-blocking.
	b.statReceiver.Receive(*b.currentStat)

	b.currentStat = new(StatsFrame)
}
