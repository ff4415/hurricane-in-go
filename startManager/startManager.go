package main

import (
    "github.com/ff4415/hurricane-in-go/manager"
    // "github.com/ff4415/hurricane-in-go/configure"
    // log "github.com/Sirupsen/logrus"
)

func main()  {
    configure := "/db_configuration"
    manager := manager.GetManager(configure)
    manager.JoinPresident()
}
