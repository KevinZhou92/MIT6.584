package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

func removeFilesByRegex(dir string, regex string) error {
	// Compile the regular expression
	re, err := regexp.Compile(regex)
	if err != nil {
		return fmt.Errorf("invalid regular expression: %v", err)
	}

	// Walk through the directory
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Match the file name with the regex
		if re.MatchString(info.Name()) {
			// Remove the file if it matches the pattern
			fmt.Printf("= Removing file: %s\n", path)
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("failed to remove file %s: %v", path, err)
			}
		}
		return nil
	})

	return err
}

func getTmpFile(dir string, pattern string) (*os.File, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		fmt.Println("= can't create tmp file")
		panic(err)
	}

	return file, nil
}
