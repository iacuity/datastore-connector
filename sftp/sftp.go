package sftp

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const (
	defaultNetwork = "tcp"
)

type SFTPConnector struct {
	cfg    *SFTPConfig
	client *sftp.Client
}

type SFTPConfig struct {
	Host     string
	Port     int
	UserName string
	Password string
}

func NewSFTPConnector(cfg SFTPConfig) (*SFTPConnector, error) {
	conn := &SFTPConnector{cfg: &cfg}
	err := conn.connect()
	return conn, err
}

func (conn *SFTPConnector) connect() error {
	config := &ssh.ClientConfig{
		User:            conn.cfg.UserName,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.Password(conn.cfg.Password),
		},
	}

	sshClient, err := ssh.Dial(defaultNetwork, fmt.Sprintf("%s:%d", conn.cfg.Host, conn.cfg.Port), config)
	if err != nil {
		return err
	}

	client, err := sftp.NewClient(sshClient)
	if err != nil {
		return err
	}

	conn.client = client
	return nil
}

func (conn *SFTPConnector) Reconnect() error {
	return conn.connect()
}

func (conn *SFTPConnector) GetWorkingDir() (string, error) {
	return conn.client.Getwd()
}

func (conn *SFTPConnector) downloadFile(remoteFilePath, localFilePath string) error {
	log.Printf("Downloading [%s] to [%s] ...", remoteFilePath, localFilePath)
	// Note: SFTP To Go doesn't support O_RDWR mode
	srcFile, err := conn.client.OpenFile(remoteFilePath, (os.O_RDONLY))
	if err != nil {
		return fmt.Errorf("unable to open remote file: %v", err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("unable to open local file: %v", err)
	}
	defer dstFile.Close()

	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("unable to download remote file: %v", err)
	}
	log.Printf("%d bytes copied to %v", bytes, localFilePath)

	return nil
}

// download the files from remote directory into local directory
// if regX is nil, downlaod all files... else download only regx matching files
// return the downloaded file list
func (conn *SFTPConnector) DownloadFileFromDir(remoteDir, localDir string, regX *regexp.Regexp, lastUpdateTime time.Time) ([]string, error) {
	// walk a directory
	walker := conn.client.Walk(remoteDir)
	downloadedFiles := make([]string, 0)
	for walker.Step() {
		if walker.Err() != nil {
			continue
		}

		if walker.Stat().IsDir() {
			continue
		}

		remoteFilePath := walker.Path()
		remoteFileName := path.Base(remoteFilePath)
		if nil != regX && !regX.MatchString(remoteFileName) {
			continue
		}

		if !lastUpdateTime.IsZero() && !walker.Stat().ModTime().After(lastUpdateTime) {
			continue
		}

		localFilePath := fmt.Sprintf("%s/%s", localDir, remoteFileName)

		if err := conn.downloadFile(remoteFilePath, localFilePath); nil != err {
			return downloadedFiles, err
		}

		downloadedFiles = append(downloadedFiles, localFilePath)
	}

	return downloadedFiles, nil
}

func (conn *SFTPConnector) Close() {
	if err := conn.client.Close(); nil != err {
		log.Println("Error while closing the SFTP connection: ", err.Error())
	}
}
