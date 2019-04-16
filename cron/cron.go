package cron

import (
	"bufio"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"os/exec"
	"strings"
	"supercronic/crontab"
	"sync"
	"syscall"
	"time"
)

var (
	READ_BUFFER_SIZE = 64 * 1024
)

func startReaderDrain(wg *sync.WaitGroup, readerLogger *logrus.Entry, reader io.ReadCloser) {
	wg.Add(1)

	go func() {
		defer func() {
			if err := reader.Close(); err != nil {
				readerLogger.Errorf("failed to close pipe: %v", err)
			}
			wg.Done()
		}()

		bufReader := bufio.NewReaderSize(reader, READ_BUFFER_SIZE)

		for {
			line, isPrefix, err := bufReader.ReadLine()

			if err != nil {
				if strings.Contains(err.Error(), os.ErrClosed.Error()) {
					// The underlying reader might get
					// closed by e.g. Wait(), or even the
					// process we're starting, so we don't
					// log this.
				} else if err == io.EOF {
					// EOF, we don't need to log this
				} else {
					// Unexpected error: log it
					readerLogger.Errorf("failed to read pipe: %v", err)
				}

				break
			}

			readerLogger.Info(string(line))

			if isPrefix {
				readerLogger.Warn("last line exceeded buffer size, continuing...")
			}
		}
	}()
}

func runJob(cronCtx *crontab.Context, command string, jobLogger *logrus.Entry) error {
	jobLogger.Info("starting")

	cmd := exec.Command(cronCtx.Shell, "-c", command)

	// Run in a separate process group so that in interactive usage, CTRL+C
	// stops supercronic, not the children threads.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	env := os.Environ()
	for k, v := range cronCtx.Environ {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	var wg sync.WaitGroup

	stdoutLogger := jobLogger.WithFields(logrus.Fields{"channel": "stdout"})
	startReaderDrain(&wg, stdoutLogger, stdout)

	stderrLogger := jobLogger.WithFields(logrus.Fields{"channel": "stderr"})
	startReaderDrain(&wg, stderrLogger, stderr)

	wg.Wait()

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("error running command: %v", err)
	}

	return nil
}

func monitorJob(ctx context.Context, expression crontab.Expression, t0 time.Time, jobLogger *logrus.Entry, overlapping bool) {
	t := t0

	for {
		t = expression.Next(t)

		select {
		case <-time.After(time.Until(t)):
			m := "not starting"
			if overlapping {
				m = "overlapping jobs"
			}

			jobLogger.Warnf("%s: job is still running since %s (%s elapsed)", m, t0, t.Sub(t0))
		case <-ctx.Done():
			return
		}
	}
}

func startFunc(wg *sync.WaitGroup, exitCtx context.Context, logger *logrus.Entry, overlapping bool, expression crontab.Expression, fn func(time.Time, *logrus.Entry)) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var jobWg sync.WaitGroup
		defer jobWg.Wait()

		var cronIteration uint64
		nextRun := time.Now()

		// NOTE: if overlapping is disabled (default), this does not run multiple
		// instances of the job concurrently
		for {
			nextRun = expression.Next(nextRun)
			logger.Debugf("job will run next at %v", nextRun)

			delay := nextRun.Sub(time.Now())
			if delay < 0 {
				logger.Warningf("job took too long to run: it should have started %v ago", -delay)
				nextRun = time.Now()
				continue
			}

			select {
			case <-exitCtx.Done():
				logger.Debug("shutting down")
				return
			case <-time.After(delay):
				// Proceed normally
			}

			jobWg.Add(1)

			runThisJob := func(cronIteration uint64) {
				defer jobWg.Done()

				jobLogger := logger.WithFields(logrus.Fields{
					"iteration": cronIteration,
				})

				fn(nextRun, jobLogger)
			}

			if overlapping {
				go runThisJob(cronIteration)
			} else {
				runThisJob(cronIteration)
			}

			cronIteration++
		}
	}()
}

func StartJob(wg *sync.WaitGroup, cronCtx *crontab.Context, job *crontab.Job, exitCtx context.Context, cronLogger *logrus.Entry, overlapping bool) {
	runThisJob := func(t0 time.Time, jobLogger *logrus.Entry) {
		monitorCtx, cancelMonitor := context.WithCancel(context.Background())
		defer cancelMonitor()

		go monitorJob(monitorCtx, job.Expression, t0, jobLogger, overlapping)

		err := runJob(cronCtx, job.Command, jobLogger)

		if err == nil {
			jobLogger.Info("job succeeded")
		} else {
			jobLogger.Error(err)
		}
	}

	startFunc(wg, exitCtx, cronLogger, overlapping, job.Expression, runThisJob)
}
