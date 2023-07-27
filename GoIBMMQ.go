package main

//#cgo !windows CFLAGS:  -I/opt/mqm/inc -D_REENTRANT
//#cgo  windows CFLAGS:  -I"C:/Program Files/IBM/MQ/Tools/c/include"
//#cgo !windows LDFLAGS: -L/opt/mqm/lib64 -lmqm_r -Wl,-rpath,/opt/mqm/lib64 -Wl,-rpath,/usr/lib64
//#cgo  windows LDFLAGS: -L "C:/Program Files/IBM/MQ/bin64" -lmqic
//#include "GoIBMMQ.c"
import "C"
import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

func main() {
	if !(len(os.Args) == 2) {
		panic("error! port number missing!")
	}

	portNumber := os.Args[1]

	http.HandleFunc("/", handler4Message)

	//err := http.ListenAndServeTLS("localhost:" + portNumber, "mypubcer.pem", "my.pem", nil)
	err := http.ListenAndServe("localhost:"+portNumber, nil)

	if err != nil {
		log.Fatal("ListenAndServe error: ", err)
	}
}

func handler4Message(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if x := recover(); x != nil {
			log.Printf("panic: %v", x)
		}
	}()

	portNumberH := os.Args[1]

	if r.Method == "POST" {
		r.ParseForm()

		operation := r.Form.Get("Operation")
		showMsg := r.Form.Get("ShowMsg")

		if operation == "Put" {
			cconnectionName := C.CString(r.Form.Get("ConnectionName"))
			defer C.free(unsafe.Pointer(cconnectionName))

			cchannelName := C.CString(r.Form.Get("ChannelName"))
			defer C.free(unsafe.Pointer(cchannelName))

			csslCipherSpec := C.CString(r.Form.Get("SSLCipherSpec"))
			defer C.free(unsafe.Pointer(csslCipherSpec))

			ckeyRepository := C.CString(r.Form.Get("KeyRepository"))
			defer C.free(unsafe.Pointer(ckeyRepository))

			cuserLogin := C.CString(r.Form.Get("UserLogin"))
			defer C.free(unsafe.Pointer(cuserLogin))

			cuserPassword := C.CString(r.Form.Get("UserPassword"))
			defer C.free(unsafe.Pointer(cuserPassword))

			cqueueManagerName := C.CString(r.Form.Get("QueueManagerName"))
			defer C.free(unsafe.Pointer(cqueueManagerName))

			cqueueName := C.CString(r.Form.Get("QueueName"))
			defer C.free(unsafe.Pointer(cqueueName))

			jmsData := r.Form.Get("JMSData")
			cjmsData := C.CString(jmsData)
			defer C.free(unsafe.Pointer(cjmsData))

			usrData := r.Form.Get("UsrData")
			cusrData := C.CString(usrData)
			defer C.free(unsafe.Pointer(cusrData))

			msgData := r.Form.Get("MsgData")
			cmsgData := C.CString(msgData)
			defer C.free(unsafe.Pointer(cmsgData))

			cerrorMessage := C.CString(getStringBuffer(333))
			defer C.free(unsafe.Pointer(cerrorMessage))

			println("=== " + operation + " " + portNumberH + " start ======================================================================================")
			println("jmsData - ", jmsData)
			println("usrData - ", usrData)
			if showMsg == "yes" {
				println("msgData - ", msgData)
			}
			println(time.Now().String())

			C.IBMMQPut(cconnectionName, cchannelName, csslCipherSpec, ckeyRepository, cqueueManagerName, cqueueName, cjmsData, cusrData, cmsgData, cuserLogin, cuserPassword, cerrorMessage)

			println(time.Now().String())

			errorMessage := ""
			errorMessage = C.GoString(cerrorMessage)
			if errorMessage != "ok" {
				println("Error - ", errorMessage)
				fmt.Fprint(w, "Error - "+errorMessage)
			} else {
				fmt.Fprint(w, "ok")
			}

			println("=== " + operation + " " + portNumberH + " finish =====================================================================================")
		}

		if operation == "Get" {
			cconnectionName := C.CString(r.Form.Get("ConnectionName"))
			defer C.free(unsafe.Pointer(cconnectionName))

			cchannelName := C.CString(r.Form.Get("ChannelName"))
			defer C.free(unsafe.Pointer(cchannelName))

			csslCipherSpec := C.CString(r.Form.Get("SSLCipherSpec"))
			defer C.free(unsafe.Pointer(csslCipherSpec))

			ckeyRepository := C.CString(r.Form.Get("KeyRepository"))
			defer C.free(unsafe.Pointer(ckeyRepository))

			cuserLogin := C.CString(r.Form.Get("UserLogin"))
			defer C.free(unsafe.Pointer(cuserLogin))

			cuserPassword := C.CString(r.Form.Get("UserPassword"))
			defer C.free(unsafe.Pointer(cuserPassword))

			cqueueManagerName := C.CString(r.Form.Get("QueueManagerName"))
			defer C.free(unsafe.Pointer(cqueueManagerName))

			cqueueName := C.CString(r.Form.Get("QueueName"))
			defer C.free(unsafe.Pointer(cqueueName))

			msgCount, _ := strconv.Atoi(r.Form.Get("MsgCount"))
			cmsgCount := C.MQLONG(msgCount)

			cmsgData := C.CString(getStringBuffer(1024 * 1024))
			defer C.free(unsafe.Pointer(cmsgData))

			cerrorMessage := C.CString(getStringBuffer(333))
			defer C.free(unsafe.Pointer(cerrorMessage))

			println("=== " + operation + " " + portNumberH + " start ========================================================================================")
			println(time.Now().String())

			C.IBMMQGet(cconnectionName, cchannelName, csslCipherSpec, ckeyRepository, cqueueManagerName, cqueueName, cmsgCount, cmsgData, cuserLogin, cuserPassword, cerrorMessage)

			println(time.Now().String())

			errorMessage := ""
			errorMessage = C.GoString(cerrorMessage)

			if errorMessage != "ok" {
				println("Error - ", errorMessage)
				fmt.Fprint(w, "Error - "+errorMessage)
			} else {
				msgData := ""
				msgData = C.GoString(cmsgData)
				msgData = strings.TrimSpace(msgData)

				//if showMsg == "yes" {
				//	println("msgData - ", msgData)
				//}

				if strings.Contains(msgData, "<Cid>") == false {
					msgData = "<Cid>empty</Cid>" + msgData
				}

				cid := ""
				msgJSON := ""

				for strings.Contains(msgData, "<Cid>") == true {
					cid = ""
					cid = msgData[(strings.Index(msgData, "<Cid>") + 5):strings.Index(msgData, "</Cid>")]
					println("cid: " + cid)

					if strings.Contains(msgData, "{\"") == true {
						msgJSON = ""
						msgJSON = strings.TrimSpace(msgData[strings.Index(msgData, "{\""):strings.Index(msgData, "{#}")])

						if showMsg == "yes" {
							println("msgJSON: " + msgJSON)
						}
					} else {
						msgJSON = "empty"
					}

					fmt.Fprint(w, "{###}"+cid+"{#}"+msgJSON)

					msgData = strings.TrimSpace(msgData[(strings.Index(msgData, "{#}") + 3):])
				}
			}

			println("=== " + operation + " " + portNumberH + " finish ========================================================================================")
		}
	}
}

func getStringBuffer(bufSize int) string {
	var buf bytes.Buffer
	for i := 1; i <= bufSize; i++ {
		buf.WriteString(" ")
	}
	return buf.String()
}
