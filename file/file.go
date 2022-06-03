package file

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"strings"
)

type Line struct {
	Line string
	Err  error
}

// use this api for reading large size file
// read the file by delimiter
// write line to the the given channel
// end of file signal is written into channel by setting error to EOF error
func ReadFileByDlim(filePath string, delim byte, ch chan<- Line) error {
	f, err := os.Open(filePath)

	if err != nil {
		return err
	}

	defer f.Close()

	buf := bufio.NewReader(f)

	for {
		line, err := buf.ReadString(delim)
		if err != nil {
			if err == io.EOF {
				ch <- Line{Line: line, Err: err}
				return nil
			}
			ch <- Line{Line: line, Err: err}
			return err
		}

		ch <- Line{Line: line, Err: nil}
	}
}

//unzip the .gz file
func UnzipFile(fileName string) (string, error) {
	if "" == fileName {
		return "", nil
	}

	gzipfile, err := os.Open(fileName)
	if err != nil {
		return "", err
	}

	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		return "", err
	}

	defer reader.Close()
	newfilename := strings.TrimSuffix(fileName, ".gz")
	writer, err := os.Create(newfilename)
	if err != nil {
		return "", err
	}
	defer writer.Close()
	if _, err = io.Copy(writer, reader); err != nil {
		return "", err
	}

	return newfilename, nil
}

func RemoveFile(filePath string) error {
	return os.Remove(filePath)
}
