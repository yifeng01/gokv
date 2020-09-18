package util

import (
	"errors"
	"time"
)

// CheckKeyAndValue returns an error if k == "" or if v == nil
func CheckKeyAndValue(k string, v interface{}) error {
	if err := CheckKey(k); err != nil {
		return err
	}
	return CheckVal(v)
}

// CheckKey returns an error if k == ""
func CheckKey(k string) error {
	if k == "" {
		return errors.New("The passed key is an empty string, which is invalid")
	}
	return nil
}

// CheckVal returns an error if v == nil
func CheckVal(v interface{}) error {
	if v == nil {
		return errors.New("The passed value is nil, which is not allowed")
	}
	return nil
}

//deepcopy slice
func CopyData(data []byte) []byte {
	result := make([]byte, len(data))
	copy(result, data)

	return result
}

//get cur year + month + day, such as 20200918
const (
	//默认时间格式
	_defaultTimeFormat = "20060102"
)

func GetCurDay() string {
	tmNow := time.Now()
	return tmNow.Format(_defaultTimeFormat)
}
