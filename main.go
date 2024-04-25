package strikememongo

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/strikesecurity/strikememongo/monitor"
	"github.com/strikesecurity/strikememongo/strikememongolog"
)

// Server represents a running MongoDB server
type Server struct {
	cmd        *exec.Cmd
	watcherCmd *exec.Cmd
	dbDir      string
	logger     *strikememongolog.Logger
	port       int
}

// Start runs a MongoDB server at a given MongoDB version using default options
// and returns the Server.
func Start(version string) (*Server, error) {
	return StartWithOptions(&Options{
		MongoVersion: version,
	})
}

func StartContainer(version string) (*Server, error) {
	return StartContainerWithOptions(&Options{
		MongoVersion: version,
		DownloadURL:  "docker",
	})
}

func StartContainerWithOptions(opts *Options) (*Server, error) {
	err := opts.fillDefaults()
	if err != nil {
		return nil, err
	}

	logger := opts.getLogger()

	logger.Infof("Starting MongoDB container with options %#v", opts)

	if opts.ShouldUseReplica {
		panic("UseContainer and ShouldUseReplica cannot be used at the same time (yet)")
	}

	apiClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}
	defer apiClient.Close()

	mongoImageName := fmt.Sprintf("mongo:%s", opts.MongoVersion)

	reader, err := apiClient.ImagePull(context.Background(), mongoImageName, image.PullOptions{})
	if err != nil {
		panic(err)
	}

	defer reader.Close()
	// cli.ImagePull is asynchronous.
	// The reader needs to be read completely for the pull operation to complete.
	// If stdout is not required, consider using io.Discard instead of os.Stdout.
	io.Copy(os.Stdout, reader)

	ctx := context.Background()

	resp, err := apiClient.ContainerCreate(ctx, &container.Config{
		Image: mongoImageName,
		Cmd:   []string{"--port", strconv.Itoa(opts.Port)},
		Tty:   true,
	},
		&container.HostConfig{
			NetworkMode: "host",
		}, nil, nil, "")
	if err != nil {
		panic(err)
	}

	if err := apiClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		panic(err)
	}

	// stdoutHandler, startupErrCh, startupPortCh := stdoutHandler(logger)

	/*
		statusCh, errCh := apiClient.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
		select {
		case err := <-errCh:
			if err != nil {
				panic(err)
			}
		case <-statusCh:
		}
	*/

	// out, err := apiClient.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	// if err != nil {
	// 	panic(err)
	// }
	// go io.Copy(stdoutHandler, out)

	// stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	//

	// shove our info into cmd so the rest of the code works

	logger.Debugf("Started mongod; starting watcher")

	// Start a watcher: the watcher is a subprocess that ensure if this process
	// dies, the mongo server will be killed (and not reparented under init)
	// watcherCmd, err := monitor.RunMonitor(os.Getpid(), cmd.Process.Pid)
	// if err != nil {
	// 	return nil, err
	// }

	logger.Debugf("Started watcher; waiting for mongod to report port number")

	// Wait for the stdout handler to report the server's port number (or a
	// startup error)
	/*
		var port int
		select {
		case p := <-startupPortCh:
			port = p
		case err := <-startupErrCh:
			/*killErr := cmd.Process.Kill()
			if killErr != nil {
				logger.Warnf("error stopping mongo process: %s", killErr)
			}
			return nil, err
		case <-time.After(opts.StartupTimeout):
				killErr := cmd.Process.Kill()
				if killErr != nil {
					logger.Warnf("error stopping mongo process: %s", killErr)
				}

			return nil, errors.New("timed out waiting for mongod to start")
		}
	*/

	return &Server{
		//cmd        *exec.Cmd
		// watcherCmd: watcherCmd,
		//dbDir      string
		logger: logger,
		port:   opts.Port,
	}, nil
}

// StartWithOptions is like Start(), but accepts options.
func StartWithOptions(opts *Options) (*Server, error) {
	err := opts.fillDefaults()
	if err != nil {
		return nil, err
	}

	logger := opts.getLogger()

	logger.Infof("Starting MongoDB with options %#v", opts)

	binPath, err := opts.getOrDownloadBinPath()
	if err != nil {
		return nil, err
	}

	logger.Debugf("Using binary %s", binPath)

	// Create a db dir. Even the ephemeralForTest engine needs a dbpath.
	dbDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	// Construct the command and attach stdout/stderr handlers
	//  Safe to pass binPath and dbDir
	//nolint:gosec
	cmd := exec.Command(binPath, "--storageEngine", "ephemeralForTest", "--dbpath", dbDir, "--port", strconv.Itoa(opts.Port))
	if opts.ShouldUseReplica {
		//nolint:gosec
		cmd = exec.Command(binPath, "--storageEngine", "wiredTiger", "--dbpath", dbDir, "--port", strconv.Itoa(opts.Port), "--replSet", "rs0", "--bind_ip", "localhost")
	}

	stdoutHandler, startupErrCh, startupPortCh := stdoutHandler(logger)
	cmd.Stdout = stdoutHandler
	cmd.Stderr = stderrHandler(logger)

	logger.Debugf("Starting mongod")

	// Run the server
	err = cmd.Start()
	if err != nil {
		remErr := os.RemoveAll(dbDir)
		if remErr != nil {
			logger.Warnf("error removing data directory: %s", remErr)
		}

		return nil, err
	}

	logger.Debugf("Started mongod; starting watcher")

	// Start a watcher: the watcher is a subprocess that ensure if this process
	// dies, the mongo server will be killed (and not reparented under init)
	watcherCmd, err := monitor.RunMonitor(os.Getpid(), cmd.Process.Pid)
	if err != nil {
		killErr := cmd.Process.Kill()
		if killErr != nil {
			logger.Warnf("error stopping mongo process: %s", killErr)
		}

		remErr := os.RemoveAll(dbDir)
		if remErr != nil {
			logger.Warnf("error removing data directory: %s", remErr)
		}

		return nil, err
	}

	logger.Debugf("Started watcher; waiting for mongod to report port number")
	startupTime := time.Now()

	// Wait for the stdout handler to report the server's port number (or a
	// startup error)
	var port int
	select {
	case p := <-startupPortCh:
		port = p
	case err := <-startupErrCh:
		killErr := cmd.Process.Kill()
		if killErr != nil {
			logger.Warnf("error stopping mongo process: %s", killErr)
		}

		remErr := os.RemoveAll(dbDir)
		if remErr != nil {
			logger.Warnf("error removing data directory: %s", remErr)
		}

		return nil, err
	case <-time.After(opts.StartupTimeout):
		killErr := cmd.Process.Kill()
		if killErr != nil {
			logger.Warnf("error stopping mongo process: %s", killErr)
		}

		remErr := os.RemoveAll(dbDir)
		if remErr != nil {
			logger.Warnf("error removing data directory: %s", remErr)
		}

		return nil, errors.New("timed out waiting for mongod to start")
	}

	logger.Debugf("mongod started up and reported a port number after %s", time.Since(startupTime).String())

	// ---------- START OF REPLICA CODE ----------
	if opts.ShouldUseReplica {
		mongoCommand := fmt.Sprintf("mongo --port %d --retryWrites --eval \"rs.initiate()\"", opts.Port)
		//nolint:gosec
		cmd2 := exec.Command("bash", "-c", mongoCommand)
		cmd2.Stdout = stdoutHandler
		cmd2.Stderr = stderrHandler(logger)

		// Initiate Replica
		err2 := cmd2.Run()
		if err2 != nil {
			logger.Warnf("ERROR INITIAING REPLICA: %v", err2)

			return nil, err
		}

		logger.Debugf("Started mongo replica")
	}
	// ---------- END OF REPLICA CODE ----------

	// Return a Memongo server
	return &Server{
		cmd:        cmd,
		watcherCmd: watcherCmd,
		dbDir:      dbDir,
		logger:     logger,
		port:       port,
	}, nil
}

// Port returns the port the server is listening on.
func (s *Server) Port() int {
	return s.port
}

// URI returns a mongodb:// URI to connect to
func (s *Server) URI() string {
	return fmt.Sprintf("mongodb://localhost:%d", s.port)
}

// URIWithRandomDB returns a mongodb:// URI to connect to, with
// a random database name (e.g. mongodb://localhost:1234/somerandomname)
func (s *Server) URIWithRandomDB() string {
	return fmt.Sprintf("mongodb://localhost:%d/%s", s.port, RandomDatabase())
}

// Stop kills the mongo server
func (s *Server) Stop() {
	err := s.cmd.Process.Kill()
	if err != nil {
		s.logger.Warnf("error stopping mongod process: %s", err)
		return
	}

	err = s.watcherCmd.Process.Kill()
	if err != nil {
		s.logger.Warnf("error stopping watcher process: %s", err)
		return
	}

	err = os.RemoveAll(s.dbDir)
	if err != nil {
		s.logger.Warnf("error removing data directory: %s", err)
		return
	}
}

// Cribbed from https://github.com/nodkz/mongodb-memory-server/blob/master/packages/mongodb-memory-server-core/src/util/MongoInstance.ts#L206
var reReady = regexp.MustCompile(`waiting for connections.*port(\s|":)(\d+)`)
var reAlreadyInUse = regexp.MustCompile("addr already in use")
var reAlreadyRunning = regexp.MustCompile("mongod already running")
var rePermissionDenied = regexp.MustCompile("mongod permission denied")
var reDataDirectoryNotFound = regexp.MustCompile("data directory .*? not found")
var reShuttingDown = regexp.MustCompile("shutting down with code")

// The stdout handler relays lines from mongod's stout to our logger, and also
// watches during startup for error or success messages.
//
// It returns two channels: an error channel and a port channel. Only one
// message will be sent to one of these two channels. A port number will
// be sent to the port channel if the server start up correctly, and an
// error will be send to the error channel if the server does not start up
// correctly.
func stdoutHandler(log *strikememongolog.Logger) (io.Writer, <-chan error, <-chan int) {
	errChan := make(chan error)
	portChan := make(chan int)

	reader, writer := io.Pipe()

	go func() {
		scanner := bufio.NewScanner(reader)
		haveSentMessage := false

		for scanner.Scan() {
			line := scanner.Text()

			log.Debugf("[Mongod stdout] %s", line)

			if !haveSentMessage {
				downcaseLine := strings.ToLower(line)

				if match := reReady.FindStringSubmatch(downcaseLine); match != nil {
					port, err := strconv.Atoi(match[2])
					if err != nil {
						errChan <- errors.New("Could not parse port from mongod log line: " + downcaseLine)
					} else {
						portChan <- port
					}
					haveSentMessage = true
				} else if reAlreadyInUse.MatchString(downcaseLine) {
					errChan <- errors.New("Mongod startup failed, address in use")
					haveSentMessage = true
				} else if reAlreadyRunning.MatchString(downcaseLine) {
					errChan <- errors.New("Mongod startup failed, already running")
					haveSentMessage = true
				} else if rePermissionDenied.MatchString(downcaseLine) {
					errChan <- errors.New("mongod startup failed, permission denied")
					haveSentMessage = true
				} else if reDataDirectoryNotFound.MatchString(downcaseLine) {
					errChan <- errors.New("Mongod startup failed, data directory not found")
					haveSentMessage = true
				} else if reShuttingDown.MatchString(downcaseLine) {
					errChan <- errors.New("Mongod startup failed, server shut down")
					haveSentMessage = true
				}
			}
		}

		if err := scanner.Err(); err != nil {
			log.Warnf("reading mongod stdin failed: %s", err)
		}

		if !haveSentMessage {
			errChan <- errors.New("Mongod exited before startup completed")
		}
	}()

	return writer, errChan, portChan
}

// The stderr handler just relays messages from stderr to our logger
func stderrHandler(log *strikememongolog.Logger) io.Writer {
	reader, writer := io.Pipe()

	go func() {
		scanner := bufio.NewScanner(reader)

		for scanner.Scan() {
			log.Debugf("[Mongod stderr] %s", scanner.Text())
		}

		if err := scanner.Err(); err != nil {
			log.Warnf("reading mongod stdin failed: %s", err)
		}
	}()

	return writer
}
