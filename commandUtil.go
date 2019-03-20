package commandUtil

import (
    "github.com/valyala/fasthttp"
)

var Client *fasthttp.Client
var Server *fasthttp.Server
var Args *fasthttp.Args
var StatusOK = fasthttp.StatusOK
var StatusNotFound = fasthttp.StatusNotFound

type RequestCtx fasthttp.RequestCtx

func GetClient() Client  {
    sync.Once.Do(func ()  {
        client = &fasthttp.Client{}
        if !client {
            panic("no client return")
        }
    })
    return client
}

func GetServer() Client  {
    sync.Once.Do(func ()  {
        server = &fasthttp.Server{}
        if server == nil {
            panic("no client return")
        }
    })
    return server
}

func ReleaseArgs(args Args)  {
    fasthttp.ReleaseArgs(args)
}
