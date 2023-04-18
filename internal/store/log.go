package store

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const (
	logFileName      = "data.log"
	snapshotFileName = "data.snapshot"
)

func (s *Store) appendLogEntry(entry string) error {
	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	_, err = file.WriteString(entry + "\n")
	return err
}

func (s *Store) loadLog() error {
	// Load the snapshot file
	if err := s.loadSnapshot(); err != nil {
		return err
	}

	// Load the log file
	file, err := os.Open(logFileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // If the log file doesn't exist, just return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		entry := scanner.Text()
		parts := strings.Split(entry, " ")

		switch strings.ToUpper(parts[0]) {
		case "SET":
			s.Set(parts[1], parts[2])
		case "HSET":
			s.HSet(parts[1], parts[2], parts[3])
		case "DEL":
			s.Del(parts[1])
		}
	}
	return scanner.Err()
}

func (s *Store) compactLog() error {
	// Create a new snapshot file
	snapshotFile, err := os.Create(snapshotFileName)
	if err != nil {
		return err
	}
	defer snapshotFile.Close()

	// Write the current state of the data to the snapshot
	for key, value := range s.data {
		_, err := fmt.Fprintf(snapshotFile, "SET %s %v\n", key, value)
		if err != nil {
			return err
		}
	}

	// Write the current state of the hash data to the snapshot
	for key, m := range s.hashData {
		for field, value := range m {
			_, err := fmt.Fprintf(snapshotFile, "HSET %s %s %v\n", key, field, value)
			if err != nil {
				return err
			}
		}
	}

	// Create a new log file and remove the old one
	if err := os.Remove(logFileName); err != nil {
		return err
	}

	if _, err := os.Create(logFileName); err != nil {
		return err
	}
	return nil
}

func (s *Store) loadSnapshot() error {
	file, err := os.Open(snapshotFileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // If the snapshot file doesn't exist, just return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		entry := scanner.Text()
		parts := strings.Split(entry, " ")

		switch strings.ToUpper(parts[0]) {
		case "SET":
			s.Set(parts[1], parts[2])
		case "HSET":
			s.HSet(parts[1], parts[2], parts[3])
		}
	}

	return scanner.Err()
}