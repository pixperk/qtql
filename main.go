package main

import (
	"bufio"
	"os"
)

type file struct {
	name string
}

func newFile(name string) *file {

	return &file{
		name: name,
	}
}

// opens a file (without buffer), reads content, writes new content, and fsyncs it to disk
// returns the old read content, whether fsync was successful, and any error encountered
func (f *file) open_read_write_and_fsync_if_needed(content_to_write string, fsync_wanted bool) (string, bool, error) {
	fd, err := os.OpenFile(f.name, os.O_RDWR, 0644)
	if err != nil {
		return "", false, err
	}

	defer fd.Close()

	//get file size
	stat, err := fd.Stat()
	if err != nil {
		return "", false, err
	}

	size := int(stat.Size())

	// read existing content
	buf := make([]byte, size)
	n := 0
	for n < size {
		nn, err := fd.Read(buf[n:])
		if nn == 0 && err == nil {
			break
		}
		if err != nil {
			return "", false, err
		}
		n += nn
	}

	readContent := string(buf)

	//write/truncate new content
	_, err = fd.Seek(0, 0)
	if err != nil {
		return "", false, err
	}

	_, err = fd.WriteString(content_to_write)
	if err != nil {
		return "", false, err
	}

	//truncate file to new content length
	// this is important if new content is smaller than old content
	// otherwise old content will remain after new content
	err = fd.Truncate(int64(len(content_to_write)))
	if err != nil {
		return "", false, err
	}

	//fsync loads data from kernel buffer to disk
	// fsync if needed, not fsyncing will cause data loss on crash
	//although, in some cases fsync is not needed , eg : temp files, cache files etc
	// but for critical data files, fsync is recommended
	if fsync_wanted {
		err = fd.Sync()
		if err != nil {
			return readContent, false, err
		}

		return readContent, true, nil
	}

	return readContent, false, nil

}

func (f *file) append_to_existing_file_using_buffered_writer_then_read_the_new_content(content_to_append string, fsync_wanted bool) (string, error) {
	file, err := os.OpenFile(f.name, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}

	defer file.Close()

	//loads the data into an internal buffer (our app)
	//this writer will flush data to file when buffer is full or when Flush is called
	//why we are using it? to reduce number of write syscalls
	//we could have also used it in write+fsync function above
	// but for demonstration purpose, we are using it here
	//we are appending data, so using os.O_APPEND flag while opening file
	//if we were overwriting, we would have used os.O_WRONLY and we would have seeked to start
	writer := bufio.NewWriter(file)

	//n is number of bytes written
	//is it always equal to len(content_to_append)? not necessarily
	// because writer may not write all bytes in one go
	// it may write partial bytes and buffer the rest
	n := 0
	for n < len(content_to_append) {
		nn, err := writer.WriteString(content_to_append[n:])
		//why check for nn == 0? because it indicates no more data can be written
		// this can happen if writer buffer is full or some error occurs
		// in such cases, we should break the loop to avoid infinite loop
		if nn == 0 && err == nil {
			break
		}
		if err != nil {
			return "", err
		}
		n += nn
	}

	//flushing the writer is important because it writes the buffered data to the underlying file
	// if we don't flush, data may not be written to file
	// and may remain in the buffer
	// this is especially important before closing the file
	// because closing the file does not automatically flush the buffer
	err = writer.Flush()
	if err != nil {
		return "", err
	}

	//we have appended data using buffered writer
	//now read the new content from file
	//should we fsync after appending? it is recommended to fsync before reading to ensure data integrity
	//from kernel buffer to disk [total flow : app -> buffered writer -> kernel buffer -> disk]
	if fsync_wanted {
		err = file.Sync()
		if err != nil {
			return "", err
		}
	}

	//read the new content
	newContentBytes, err := os.ReadFile(f.name)
	if err != nil {
		return "", err
	}

	return string(newContentBytes), nil

}

func main() {

	f := newFile("notes.txt")

	content_to_write := "hello, overwriting some bse bs"
	fsync_wanted := true

	new_content, fsynced, err := f.open_read_write_and_fsync_if_needed(content_to_write, fsync_wanted)
	if err != nil {
		panic(err)
	}

	if fsynced {
		println("File content after write and fsync:", new_content)
	} else {
		println("File content after write (no fsync):", new_content)
	}

	content_to_append := "\nappending some more data to file"
	new_content_after_append, err := f.append_to_existing_file_using_buffered_writer_then_read_the_new_content(content_to_append, fsync_wanted)
	if err != nil {
		panic(err)
	}

	println("File content after append:", new_content_after_append)
}
