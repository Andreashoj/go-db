package fr

import "fmt"

type FileReader interface {
	Yell()
}

type fileReader struct{}

func NewFileReader() FileReader {
	return &fileReader{}
}

func (f *fileReader) Yell() {
	fmt.Println("here")
}
