package commandUtil

import (
    "github.com/valyala/fasthttp"
    "sync"
    "fmt"
)

type Client fasthttp.Client
type Server fasthttp.Server
type Args fasthttp.Args

var client Client
var server Server

const StatusOK = fasthttp.StatusOK
const StatusNotFound = fasthttp.StatusNotFound

// type RequestCtx fasthttp.RequestCtx

func AcquireArgs() *Args  {
  args := fasthttp.AcquireArgs()
  a := Args(*args)
  return &a
}

func (a *Args) SetBytesV(key string, value []byte)  {
   arg := (fasthttp.Args)(*a)
   arg.SetBytesV(key, value)
}

func (c *Client) Post(dst []byte, url string, postArgs *Args) (statusCode int, body []byte, err error)  {
  client := (fasthttp.Client)(*c)
  args := (fasthttp.Args)(*postArgs)
  statusCode, body, err = client.Post(dst, url, &args)
  return statusCode, body, err
}

func (s *Server) ListenAndServe(addr string) error  {
  server := (fasthttp.Server)(*s)
  err := server.ListenAndServe(addr)
  if err != nil {
    return fmt.Errorf("error: %v", err)
  } else {
    return nil
  }

}

func GetClient() *Client  {
  var once sync.Once
  once.Do(func () {
    c := fasthttp.Client{}
    // if c == nil {
    //   panic("no client return")
    // }
    client = Client(c)
  })
  return &client
}

func GetServer() *Server  {
  var once sync.Once
  once.Do(func () {
    s := fasthttp.Server{}
    // if server == nil {
    //   panic("no client return")
    // }
    server = Server(s)
  })
  return &server
}

func ReleaseArgs(args *Args)  {
  a := fasthttp.Args(*args)
  fasthttp.ReleaseArgs(&a)
}
