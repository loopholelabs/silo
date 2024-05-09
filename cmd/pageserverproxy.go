package main

import (
	"fmt"
	"io"
	"net"

	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
	"github.com/spf13/cobra"
)

var pages_serve_addr string
var pages_connect_addr string

var (
	cmdPagesProxy = &cobra.Command{
		Use:   "pagesproxy",
		Short: "Run page server proxy",
		Long:  `Run page server proxy`,
		Run:   runPagesProxy,
	}
)

func init() {
	rootCmd.AddCommand(cmdPagesProxy)
	cmdPagesProxy.Flags().StringVarP(&pages_serve_addr, "addr", "a", "localhost:5170", "Address to serve from")
	cmdPagesProxy.Flags().StringVarP(&pages_connect_addr, "connect", "c", "192.168.1.120:9044", "Address to connect to")
}

func runPagesProxy(ccmd *cobra.Command, args []string) {
	fmt.Printf("Running page server proxy listening on %s will connect to %s\n", pages_serve_addr, pages_connect_addr)

	listener, err := net.Listen("tcp", pages_serve_addr)
	if err != nil {
		panic(err)
	}

	for {
		con, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		go pages_serve_proxy(con)
	}
}

func pages_serve_proxy(con net.Conn) {
	fmt.Printf("Handling connection from %s\n", con.RemoteAddr())
	defer func() {
		con.Close()
	}()

	// Proxy this connection to the actual page server

	con2, err := net.Dial("tcp", pages_connect_addr)
	if err != nil {
		fmt.Printf("Cannot dial to %s %v\n", pages_connect_addr, err)
		return
	}

	defer func() {
		con2.Close()
	}()

	psProxy(con, con2)

	//go simpleProxy(con2, con2.RemoteAddr().String(), con, con.RemoteAddr().String())

	//simpleProxy(con, con.RemoteAddr().String(), con2, con2.RemoteAddr().String())
}

func simpleProxy(from io.Reader, fromAddr string, to io.Writer, toAddr string) {

	for {
		buff := make([]byte, 65536)

		n, err := from.Read(buff)
		if err != nil {
			fmt.Printf("Error reading from server %v\n", err)
			return
		}

		n, err = to.Write(buff[:n])
		if err != nil {
			fmt.Printf("Error writing to connect %v\n", err)
			return
		}

		showData(fromAddr, toAddr, buff[:n])
	}
}

func psProxy(client net.Conn, server net.Conn) {
	for {
		buffer := make([]byte, 24)
		_, err := io.ReadFull(client, buffer)
		if err != nil {
			fmt.Printf("Error reading from connect %v\n", err)
			return
		}

		i := criu.Decode(buffer)
		fmt.Printf("-> %s\n", i.String())

		_, err = server.Write(buffer)
		if err != nil {
			fmt.Printf("Error writing to serve %v\n", err)
			return
		}

		if i.Command() == criu.PS_IOV_GET {
			// Read the page data and discard
			databuffer := make([]byte, i.Page_count*PAGE_SIZE)
			_, err := io.ReadFull(client, databuffer)
			if err != nil {
				fmt.Printf("Error reading from connect %v\n", err)
				return
			}
			fmt.Printf("-> Page DATA %d bytes\n", i.Page_count*PAGE_SIZE)

			_, err = server.Write(databuffer)
			if err != nil {
				fmt.Printf("Error writing to serve %v\n", err)
				return
			}
		} else if i.Command() == criu.PS_IOV_ADD_F {
			// Read the page data and discard
			databuffer := make([]byte, i.Page_count*PAGE_SIZE)
			_, err := io.ReadFull(client, databuffer)
			if err != nil {
				fmt.Printf("Error reading from connect %v\n", err)
				return
			}
			fmt.Printf("-> Page DATA %d bytes\n", i.Page_count*PAGE_SIZE)

			_, err = server.Write(databuffer)
			if err != nil {
				fmt.Printf("Error writing to serve %v\n", err)
				return
			}
		} else if i.Command() == criu.PS_IOV_OPEN2 {
			// byte signifies if we have parent
			b := make([]byte, 1)
			_, err := io.ReadFull(server, b)
			if err != nil {
				fmt.Printf("Error reading from connect %v\n", err)
				return
			}
			fmt.Printf("-> Open2 has_parent %x\n", b[0])

			_, err = client.Write(b)
			if err != nil {
				fmt.Printf("Error writing to serve %v\n", err)
				return
			}
		} else if i.Command() == criu.PS_IOV_PARENT {
			b := make([]byte, 4)
			_, err := io.ReadFull(server, b)
			if err != nil {
				fmt.Printf("Error reading from connect %v\n", err)
				return
			}
			fmt.Printf("-> status %x\n", b)

			_, err = client.Write(b)
			if err != nil {
				fmt.Printf("Error writing to serve %v\n", err)
				return
			}
		} else if i.Command() == criu.PS_IOV_CLOSE {
			// status 4 bytes
			b := make([]byte, 4)
			_, err := io.ReadFull(server, b)
			if err != nil {
				fmt.Printf("Error reading from connect %v\n", err)
				return
			}
			fmt.Printf("-> Status %x\n", b)

			_, err = client.Write(b)
			if err != nil {
				fmt.Printf("Error writing to serve %v\n", err)
				return
			}
		}
	}

}

func showData(from string, to string, data []byte) {
	fmt.Printf("DATA %s -> %s : %d\n", from, to, len(data))
}
