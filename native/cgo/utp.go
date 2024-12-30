package main

// #cgo LDFLAGS: -L${SRCDIR}/lib -lutp_lib
// #include "utp_lib.h"
import "C"
import (
	"bytes"
	"fmt"
	"sync"
	"time"
	"unsafe"
)

func main() {
	config := C.connection_config_create()
	defer C.connection_config_destroy(config)

	serverAddr := C.CString("0.0.0.0:3500")
	defer C.free(unsafe.Pointer(serverAddr))
	clientAddr := C.CString("0.0.0.0:3501")
	defer C.free(unsafe.Pointer(clientAddr))

	//server := C.udp_socket_create(serverAddr)

	client := C.udp_socket_create(clientAddr)
	var wg sync.WaitGroup
	wg.Add(1)
	//readBuf := make([]byte, 1024*1024*50)
	hugeData := bytes.Repeat([]byte{0xda}, 1024*1024*50)
	start := time.Now()
	//go func() {
	//	defer wg.Done()
	//	fmt.Println("start to accept")
	//	serverStreamHandle := C.udp_socket_accept(server, config)
	//	// struct UdpStreamHandle *stream, uint8_t *buf, uintptr_t len
	//	cBuf := (*C.uint8_t)(unsafe.Pointer(&readBuf[0]))
	//	_ = C.udp_stream_read(serverStreamHandle, cBuf, C.uintptr_t(len(readBuf)))
	//
	//	if !bytes.Equal(readBuf, hugeData) {
	//		fmt.Printf("read data is not correct")
	//	}
	//	C.udp_stream_close(serverStreamHandle)
	//}()

	go func() {
		defer wg.Done()
		fmt.Println("start to connect")
		clientStreamHandle := C.udp_socket_connect(client, serverAddr, config)
		fmt.Println("start to connected")
		cBuf := (*C.uint8_t)(unsafe.Pointer(&hugeData[0]))
		n := C.udp_stream_write(clientStreamHandle, cBuf, C.uintptr_t(len(hugeData)))

		if int(n) != len(hugeData) {
			fmt.Printf("write data is not correct")
		}
		C.udp_stream_close(clientStreamHandle)
	}()
	wg.Wait()
	elapsed := time.Since(start)
	megabytesSent := float64(len(hugeData)) / 1_000_000.0
	megabitsSent := megabytesSent * 8.0
	transferRate := megabitsSent / elapsed.Seconds()

	fmt.Printf(
		"finished single large transfer test with %.0f MB, in %v, at a rate of %.1f Mbps",
		megabytesSent,
		elapsed,
		transferRate,
	)
}
