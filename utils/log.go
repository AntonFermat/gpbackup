package utils

/*
 * This file contains structs and functions related to logging.
 */

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
)

var (
	logger *Logger

	defaultLogDir   = "gpAdminLogs"
	headerFormatStr = "%s:%s:%s:%06d-[%s]:-" // PROGRAMNAME:USERNAME:HOSTNAME:PID-[LOGLEVEL]:-, to match gpcrondump
)

/*
 * The following constants representing the current logging level, and are
 * cumulative (that is, setting the log level to Debug will print all Error-,
 * Info-, and Verbose-level messages in addition to Debug-level messages).
 */
const (
	LOGERROR = iota
	LOGINFO
	LOGVERBOSE
	LOGDEBUG
)

/*
 * Leveled logging output functions using the above log levels are implemented
 * below.  Info(), Verbose(), and Debug() print messages when the log level is
 * set at or above the log level matching their names.  Warn(), Error(), and
 * Fatal() always print their messages regardless of the current log level.
 *
 * The intended usage of these functions is as follows:
 * - Info: Messages that should always be written unless the user explicitly
 *         suppresses output, e.g. the timestamp that will be used in the backup.
 * - Verbose: More detailed messages that are mostly useful to the user, e.g.
 *            printing information about a function's substeps for progress tracking.
 * - Debug: More detailed messages that are mostly useful to developers, e.g.
 *          noting that a function has been called with certain arguments.
 * - Warn: Messages indicating unusual but not incorrect behavior that a user
 *         may want to know, e.g. that certain steps are skipped when using
 *         certain flags.  These messages are shown even if output is suppressed.
 * - Error: Messages indicating that an error has occurred, but that the program
 *          can continue, e.g. one table failed to back up but others succeeded.
 * - Fatal: Messages indicating that the program cannot proceed, e.g. the database
 *          cannot be reached.  This function will exit the program after printing
 *          the error message.
 */

type Logger struct {
	logStdout   *log.Logger
	logStderr   *log.Logger
	logFile     *log.Logger
	logFileName string
	verbosity   int
	header      string
}

/*
 * Logger initialization/helper functions
 */

// stdout and stderr are passed in to this function to enable output redirection in tests.
func NewLogger(stdout io.Writer, stderr io.Writer, logFile io.Writer, logFileName string, verbosity int, header string) *Logger {
	if verbosity < LOGERROR || verbosity > LOGDEBUG {
		Abort("Cannot create logger with an invalid logging level")
	}
	return &Logger{
		logStdout:   log.New(stdout, "", 0),
		logStderr:   log.New(stderr, "", 0),
		logFile:     log.New(logFile, "", 0),
		logFileName: logFileName,
		verbosity:   verbosity,
		header:      header,
	}
}

func GetLogger() *Logger {
	return logger
}

func SetLogger(log *Logger) {
	logger = log
}

/*
 * This function creates a logger, sets it to global so it's usable in utils/,
 * and then returns it so the same logger can be used in backup/ and restore/.
 */
func InitializeLogging(program string, logdir string) *Logger {
	user, homedir, host := GetUserAndHostInfo()
	pid := System.Getpid()
	header := fmt.Sprintf(headerFormatStr, program, user, host, pid, "%s")

	// Create a temporary logger to start in case there are fatal errors during initialization
	nullFile, _ := os.Open("/dev/null")
	tempLogger := NewLogger(os.Stdout, os.Stderr, nullFile, "/dev/null", LOGINFO, header)
	SetLogger(tempLogger)

	if logdir == "" {
		logdir = fmt.Sprintf("%s/%s", homedir, defaultLogDir)
	}

	CreateDirectoryOnMaster(logdir)

	logfile := fmt.Sprintf("%s/%s_%s.log", logdir, program, CurrentTimestamp()[0:8])
	logFileHandle := MustOpenFileForWriting(logfile)

	logger := NewLogger(os.Stdout, os.Stderr, logFileHandle, logfile, LOGINFO, header)
	SetLogger(logger)
	return logger
}

func (logger *Logger) GetLogPrefix(level string) string {
	logTimestamp := System.Now().Format("20060102:15:04:05")
	return fmt.Sprintf("%s %s", logTimestamp, fmt.Sprintf(logger.header, level))
}

func (logger *Logger) GetLogFilePath() string {
	return logger.logFileName
}

func (logger *Logger) GetVerbosity() int {
	return logger.verbosity
}

func (logger *Logger) SetVerbosity(verbosity int) {
	logger.verbosity = verbosity
}

/*
 * Log output functions, as described above
 */

func (logger *Logger) Info(s string, v ...interface{}) {
	message := logger.GetLogPrefix("INFO") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGINFO {
		logger.logStdout.Output(1, message)
	}
}

func (logger *Logger) Warn(s string, v ...interface{}) {
	message := logger.GetLogPrefix("WARNING") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	logger.logStdout.Output(1, message)
}

func (logger *Logger) Verbose(s string, v ...interface{}) {
	message := logger.GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGVERBOSE {
		logger.logStdout.Output(1, message)
	}
}

func (logger *Logger) Debug(s string, v ...interface{}) {
	message := logger.GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGDEBUG {
		logger.logStdout.Output(1, message)
	}
}

func (logger *Logger) Error(s string, v ...interface{}) {
	message := logger.GetLogPrefix("ERROR") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	logger.logStderr.Output(1, message)
}

func (logger *Logger) Fatal(err error, s string, v ...interface{}) {
	message := logger.GetLogPrefix("CRITICAL") + fmt.Sprintf(s, v...)
	stackTraceStr := ""
	if err != nil {
		if s != "" {
			message += ": "
		}
		message += fmt.Sprintf("%v", err)
		stackTraceStr = formatStackTrace(errors.WithStack(err))
	}
	logger.logFile.Output(1, message+stackTraceStr)
	if logger.verbosity >= LOGVERBOSE {
		Abort(message + stackTraceStr)
	} else {
		Abort(message)
	}
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func formatStackTrace(err error) string {
	st := err.(stackTracer).StackTrace()
	message := fmt.Sprintf("%+v", st[1:len(st)-2])
	return message
}
